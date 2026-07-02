# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import random
import signal
from collections.abc import AsyncGenerator, Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TypeVarTuple, Unpack

from hydrastream.actors.aggregator import DiskAggregator
from hydrastream.actors.analyzer import TelemetryAnalyzer
from hydrastream.actors.autosaver import FileAutosaver
from hydrastream.actors.controller import TrafficController
from hydrastream.actors.dispatcher import DiskFileDispatcher, StreamFileDispatcher
from hydrastream.actors.feeder import LinkFeederDaemon
from hydrastream.actors.memory_throttler import MemoryThrottler
from hydrastream.actors.resolver import (
    BaseResolverKwargs,
    DiskMetadataResolver,
    StreamMetadataResolver,
)
from hydrastream.actors.stater import StateKeeperActor
from hydrastream.actors.streamer import file_streamer
from hydrastream.actors.throttler import ThrottleController
from hydrastream.actors.worker import (
    BaseWorkerKwargs,
    DiskDownloadWorker,
    StreamDownloadWorker,
    WakeUpPill,
)
from hydrastream.actors.writer import DiskWriter
from hydrastream.domain.base_actor import BaseActorKwargs
from hydrastream.domain.context import HydraContext
from hydrastream.domain.entities import Checksum, File, TypeHash
from hydrastream.exceptions import (
    LogStatus,
)
from hydrastream.messages.base import StandardPill, TerminalPill, ask
from hydrastream.messages.state import GetSnapshotCmd

Ts = TypeVarTuple("Ts")


async def delayed_task(
    ctx: HydraContext,
    task: Callable[[HydraContext, Unpack[Ts]], Awaitable[None]],
    *args: *Ts,
    delay: tuple[float, float] = (0, 0.3),
) -> None:
    await asyncio.sleep(random.uniform(*delay))
    await task(ctx, *args)


async def teardown_engine(ctx: HydraContext, loop: asyncio.AbstractEventLoop) -> None:
    if not ctx.session_killer.is_set():
        return

    ctx.session_killer.set()

    await stop(ctx, complete=True)

    ctx.fs.force_close_all()

    await ctx.ui.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)

    await loop.shutdown_default_executor()


async def stop(ctx: HydraContext, complete: bool = False) -> None:
    if ctx.session_killer.is_set():
        return

    ctx.session_killer.clear()

    if not complete:
        await ctx.ui.log(
            "Interrupt signal received. Initiating graceful shutdown...",
            status=LogStatus.INTERRUPT,
        )

        if ctx.is_stream:
            ctx.stream_chunks_q.send_poison_pills_nowait()
            ctx.file_discovery_q.send_poison_pills_nowait()


async def prepare_runtime(ctx: HydraContext, loop: asyncio.AbstractEventLoop) -> None:
    optimal_threads = max(20, ctx.config.threads + 10)
    max_safe_threads = min(optimal_threads, 64)
    custom_pool = ThreadPoolExecutor(
        max_workers=max_safe_threads, thread_name_prefix="HydraIO"
    )
    loop.set_default_executor(custom_pool)

    main_task = asyncio.current_task()

    def handle_signal() -> None:
        if main_task and not main_task.done():
            main_task.cancel()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)


async def _bootstrap_engine(  # noqa
    ctx: HydraContext,
    tg: asyncio.TaskGroup,
) -> None:
    base_actor_kwargs: BaseActorKwargs = {
        "ui": ctx.ui,
        "is_debug": ctx.config.debug,
    }
    feeder = LinkFeederDaemon(
        **base_actor_kwargs,
        inbox=ctx.raw_links_q,
        links_outbox=ctx.links_q,
    )

    base_resolver_kwargs: BaseResolverKwargs = {
        **base_actor_kwargs,
        "threads": ctx.config.threads,
        "MIN_CHUNK": ctx.config.MIN_CHUNK,
        "inbox": ctx.links_q,
        "files_outbox": ctx.files_q,
        "state_outbox": ctx.state_q,
        "is_dry_run": ctx.config.dry_run,
        "is_verify": ctx.config.verify,
        "net": ctx.net,
        "provider": ctx.provider,
    }
    resolvers: list[DiskMetadataResolver | StreamMetadataResolver] | None = []

    for _ in range(ctx.resolvers):
        resolvers.append(
            DiskMetadataResolver(
                **base_resolver_kwargs,
                fs=ctx.fs,
            )
            if not ctx.is_stream
            else StreamMetadataResolver(
                **base_resolver_kwargs, STREAM_CHUNK_SIZE=ctx.config.STREAM_CHUNK_SIZE
            )
        )

    stater = StateKeeperActor(
        inbox=ctx.state_q,
        throttler_output=ctx.throttler_q,
        bytes_to_check=ctx.config.MIN_CHUNK,
        ui=ctx.ui,
        fs=ctx.fs,
        is_stream=ctx.is_stream,
        is_debug=ctx.config.debug,
    )

    dispatcher = None
    workers = None
    memory_throttler = None
    aggregator = None
    analyzer = None
    autosaver = None
    controller = None
    throttler = None
    writer = None

    if not ctx.config.dry_run:
        dispatcher = (
            StreamFileDispatcher(
                **base_actor_kwargs,
                limit=ctx.config.threads,
                inbox=ctx.files_q,
                chunks_outbox=ctx.chunks_q,
                file_limit_inbox=ctx.file_limit_q,
                num_workers=1,
                file_discovery=ctx.file_discovery_q,
            )
            if ctx.is_stream
            else DiskFileDispatcher(
                **base_actor_kwargs,
                limit=ctx.config.threads,
                inbox=ctx.files_q,
                chunks_outbox=ctx.chunks_q,
                file_limit_inbox=ctx.file_limit_q,
                num_workers=ctx.workers,
                fs=ctx.fs,
            )
        )

        if ctx.is_stream:
            memory_throttler = MemoryThrottler(
                **base_actor_kwargs,
                inbox=ctx.chunks_q,
                chunk_outbox=ctx.ready_chunks_q,
                credit_inbox=ctx.credit_q,
                budget=ctx.config.BUFFER_SIZE,
                num_workers=ctx.workers,
            )

        base_worker_kwargs: BaseWorkerKwargs = {
            **base_actor_kwargs,
            "throttler_outbox": ctx.throttler_q,
            "controller_outbox": ctx.controller_q,
            "state_outbox": ctx.state_q,
            "sleep_signals_indox": ctx.sleep_signals,
            "wait_in_sleep_inbox": ctx.wait_in_sleep,
            "net": ctx.net,
        }
        workers: list[DiskDownloadWorker | StreamDownloadWorker] | None = []

        for _ in range(ctx.workers):
            workers.append(
                StreamDownloadWorker(
                    **base_worker_kwargs,
                    inbox=ctx.ready_chunks_q,
                    stream_chunks_outbox=ctx.stream_chunks_q,
                    file_discovery_outbox=ctx.file_discovery_q,
                )
                if ctx.is_stream
                else DiskDownloadWorker(
                    **base_worker_kwargs,
                    inbox=ctx.chunks_q,
                    disk_outbox=ctx.disk_q,
                    file_limit_outbox=ctx.file_limit_q,
                    fs=ctx.fs,
                )
            )

        writer = DiskWriter(
            **base_actor_kwargs,
            inbox=ctx.writer_q,
            ack_outbox=ctx.ack_q,
            fs=ctx.fs,
        )

        aggregator = DiskAggregator(
            **base_actor_kwargs,
            inbox=ctx.disk_q,
            throttler_outbox=ctx.throttler_q,
            ack_inbox=ctx.ack_q,
            writer_outbox=ctx.writer_q,
            MAX_BUFFER=int(ctx.config.BUFFER_SIZE / 3),
            fs=ctx.fs,
        )

        analyzer = TelemetryAnalyzer(
            **base_actor_kwargs,
            inbox=ctx.analyzer_q,
            threads=ctx.config.threads,
            current_limit=ctx.start_works,
            bytes_to_check=ctx.config.MIN_CHUNK,
            state_outbox=ctx.state_q,
            controller_outbox=ctx.controller_q,
        )

        autosaver = FileAutosaver(
            **base_actor_kwargs,
            inbox=ctx.autosaver_q,
            interval=60,
            disk_q=ctx.disk_q,
            reg_events_q=ctx.state_q,
            fs=ctx.fs,
        )

        controller = TrafficController(
            **base_actor_kwargs,
            inbox=ctx.controller_q,
            sleep_signals_outdox=ctx.sleep_signals,
            wait_in_sleep_outbox=ctx.wait_in_sleep,
            dynamic_limit=ctx.start_works,
            prev_dynamic_limit=ctx.start_works,
            workers=ctx.workers,
        )

        throttler = ThrottleController(
            **base_actor_kwargs,
            inbox=ctx.throttler_q,
            speed_limit=ctx.config.speed_limit,
            bytes_to_check=ctx.config.MIN_CHUNK,
        )

    async def session_killer() -> None:
        try:
            await ctx.session_killer.wait()
            ctx.session_killer.clear()
        except asyncio.CancelledError:
            await ctx.net.close()
            raise

    tg.create_task(session_killer(), name="stage:killer")

    async def stage_0_stating() -> None:
        try:
            await stater.run()
        finally:
            ctx.session_killer.set()

    tg.create_task(stage_0_stating(), name="stage:stater")

    async def stage_1_feeding() -> None:
        try:
            await feeder.run()
        finally:
            ctx.links_q.send_poison_pills_nowait(count=ctx.resolvers)

    tg.create_task(stage_1_feeding(), name="stage:feeder")

    async def stage_2_resolving() -> None:
        try:
            async with asyncio.TaskGroup() as stage_tg:
                for i, resolver in enumerate(resolvers):
                    stage_tg.create_task(resolver.run(), name=f"resolver_{i}")
        finally:
            if not ctx.config.dry_run:
                ctx.files_q.send_poison_pills_nowait(count=1)
            else:
                ctx.state_q.send_poison_pills_nowait(count=1)

    tg.create_task(stage_2_resolving(), name="stage:resolvers")

    if (
        not ctx.config.dry_run
        and dispatcher is not None
        and workers is not None
        and aggregator is not None
        and analyzer is not None
        and autosaver is not None
        and controller is not None
        and throttler is not None
    ):

        async def stage_3_dispatching() -> None:
            try:
                await dispatcher.run()
            finally:
                if not ctx.is_stream:
                    ctx.chunks_q.send_poison_pills_nowait(count=ctx.workers)
                    for _ in range(ctx.workers):
                        await ctx.wait_in_sleep.send_data(WakeUpPill())
                else:
                    ctx.chunks_q.send_poison_pills_nowait(count=1)

        tg.create_task(stage_3_dispatching(), name="stage:dispatcher")

        if ctx.is_stream and memory_throttler is not None:

            async def stage_4_memory_throttling() -> None:
                try:
                    await memory_throttler.run()
                finally:
                    ctx.ready_chunks_q.send_poison_pills_nowait(count=ctx.workers)
                    for _ in range(ctx.workers):
                        await ctx.wait_in_sleep.send_data(WakeUpPill())

            tg.create_task(stage_4_memory_throttling(), name="stage:feeder")

        async def stage_5_working() -> None:
            try:
                async with asyncio.TaskGroup() as stage_tg:
                    for i, worker in enumerate(workers):
                        stage_tg.create_task(worker.run(), name=f"worker_{i}")
            finally:
                ctx.disk_q.send_poison_pills_nowait(count=1)
                ctx.writer_q.send_poison_pills_nowait(count=1)
                ctx.analyzer_q.send_poison_pills_nowait(count=1)
                ctx.controller_q.send_poison_pills_nowait(count=1)
                ctx.file_discovery_q.send_poison_pills_nowait(count=1)
                ctx.autosaver_q.send_poison_pills_nowait(count=1)

        tg.create_task(stage_5_working(), name="stage:feeder")

        async def stage_6_servicing() -> None:
            try:
                async with asyncio.TaskGroup() as stage_tg:
                    stage_tg.create_task(aggregator.run(), name="aggregator")
                    stage_tg.create_task(analyzer.run(), name="analyzer")
                    stage_tg.create_task(autosaver.run(), name="autosaver")
                    stage_tg.create_task(controller.run(), name="traffic_tontroller")
                    stage_tg.create_task(throttler.run(), name="throttle_controller")
            finally:
                if not ctx.is_stream:
                    ctx.writer_q.send_poison_pills_nowait(count=1)
                else:
                    ctx.state_q.send_poison_pills_nowait(count=1)

        tg.create_task(stage_6_servicing(), name="stage:service")

        if not ctx.is_stream and writer is not None:

            async def stage_7_writing() -> None:
                try:
                    await writer.run()
                finally:
                    ctx.state_q.send_poison_pills_nowait(count=1)

            tg.create_task(stage_7_writing(), name="stage:writer")


async def stream_all(
    ctx: HydraContext,
    links: list[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
    loop = asyncio.get_running_loop()
    await prepare_runtime(ctx, loop)

    try:
        try:
            async with asyncio.TaskGroup() as tg:
                await _bootstrap_engine(ctx, tg)

                if not ctx.config.dry_run:
                    file_gen = None
                    while True:
                        msg = await ctx.file_discovery_q.get()
                        match msg:
                            case File():
                                filename = msg.actual_filename
                                file_gen = file_streamer(
                                    ui=ctx.ui,
                                    is_debug=ctx.config.debug,
                                    file_obj=msg,
                                    stream_chunk_inbox=ctx.stream_chunks_q,
                                    credit_outbox=ctx.credit_q,
                                    reg_events_q=ctx.state_q,
                                    file_limit_q=ctx.file_limit_q,
                                )
                                yield filename, file_gen

                            case StandardPill() | TerminalPill():
                                break

                            case _:
                                if ctx.config.debug:
                                    raise RuntimeError(
                                        "Unknown message type in "
                                        f"file_discovery_q: {type(msg)}"
                                    )
                                await ctx.ui.log(
                                    f"Received unknown message: {msg}",
                                    status=LogStatus.ERROR,
                                )

        except* Exception as eg:
            for e in eg.exceptions:
                await ctx.ui.log(
                    f"Critical System Failure: {e!r}", status=LogStatus.CRITICAL
                )
            raise
    except (asyncio.CancelledError, GeneratorExit):
        if ctx.config.debug:
            raise
        # Логируем через твой статус и останавливаем контекст
        await ctx.ui.log("Operation cancelled by user.", status=LogStatus.INTERRUPT)
        await stop(ctx)

    finally:
        await teardown_engine(ctx, loop)


async def run_downloads(
    ctx: HydraContext,
    links: list[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> None:

    loop = asyncio.get_running_loop()
    await prepare_runtime(ctx, loop)

    try:
        try:
            async with asyncio.TaskGroup() as tg:
                await _bootstrap_engine(ctx, tg)

            if ctx.config.dry_run:
                files = await ask(
                    inbox=ctx.state_q,
                    msg_factory=GetSnapshotCmd.create_request,
                    timeout=5.0,
                    sort_key=(-1,),
                )
                await ctx.ui.dry_run(files, ctx.config.output_dir)

        except* Exception as eg:
            await ctx.ui.log(
                f"Critical failure in TaskGroup: {eg.exceptions}",
                status=LogStatus.CRITICAL,
            )
            raise

    except asyncio.CancelledError:
        await stop(ctx)
        raise

    finally:
        await teardown_engine(ctx, loop)
