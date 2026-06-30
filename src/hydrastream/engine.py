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
from hydrastream.actors.feeder import LinkFeeder
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
)
from hydrastream.actors.writer import DiskWriter
from hydrastream.domain.context import HydraContext
from hydrastream.domain.entities import Checksum, File, TypeHash
from hydrastream.exceptions import (
    LogStatus,
)
from hydrastream.messages.base import StandardPill, TerminalPill
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
    if not ctx.is_running:
        return

    ctx.is_running = False

    await stop(ctx, complete=True)

    ctx.fs.force_close_all()

    await ctx.ui.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)

    await loop.shutdown_default_executor()


async def stop(ctx: HydraContext, complete: bool = False) -> None:
    if ctx.is_stopping:
        return
    ctx.is_stopping = True

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


async def _bootstrap_engine(
    ctx: HydraContext,
    tg: asyncio.TaskGroup,
    links: list[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> None:

    base_resolver_kwargs: BaseResolverKwargs = {
        "threads": ctx.config.threads,
        "MIN_CHUNK": ctx.config.MIN_CHUNK,
        "links_inbox": ctx.links_q,
        "files_outbox": ctx.files_q,
        "state_outbox": ctx.state_q,
        "all_complete": ctx.all_complete,
        "is_dry_run": ctx.config.dry_run,
        "is_verify": ctx.config.verify,
        "is_debug": ctx.config.debug,
        "ui": ctx.ui,
        "net": ctx.net,
        "provider": ctx.provider,
    }

    for i in range(ctx.resolvers):
        resolver = (
            DiskMetadataResolver(
                **base_resolver_kwargs,
                fs=ctx.fs,
            )
            if not ctx.is_stream
            else StreamMetadataResolver(
                **base_resolver_kwargs, STREAM_CHUNK_SIZE=ctx.config.STREAM_CHUNK_SIZE
            )
        )
        tg.create_task(resolver.run(), name=f"MetadataResolver: {i}")

    stater = StateKeeperActor(
        stater_inbox=ctx.state_q,
        throttler_output=ctx.throttler_q,
        bytes_to_check=ctx.config.MIN_CHUNK,
        analyzer_checkpoint_event=ctx.analyzer_checkpoint_event,
        ui=ctx.ui,
        fs=ctx.fs,
        is_stream=ctx.is_stream,
        is_debug=ctx.config.debug,
    )
    tg.create_task(stater.run(), name="StateKeeper")

    if not ctx.config.dry_run:
        dispatcher = (
            StreamFileDispatcher(
                limit=ctx.config.threads,
                files_inbox=ctx.files_q,
                chunks_outbox=ctx.chunks_q,
                file_limit_inbox=ctx.file_limit_q,
                num_workers=1,
                file_discovery=ctx.file_discovery_q,
                ui=ctx.ui,
                is_debug=ctx.config.debug,
            )
            if ctx.is_stream
            else DiskFileDispatcher(
                limit=ctx.config.threads,
                files_inbox=ctx.files_q,
                chunks_outbox=ctx.chunks_q,
                file_limit_inbox=ctx.file_limit_q,
                num_workers=ctx.workers,
                ui=ctx.ui,
                is_debug=ctx.config.debug,
                fs=ctx.fs,
            )
        )
        tg.create_task(dispatcher.run(), name="FileDispatcher")

        if ctx.is_stream:
            memory_throttler = MemoryThrottler(
                chunk_inbox=ctx.chunks_q,
                chunk_outbox=ctx.ready_chunks_q,
                credit_inbox=ctx.credit_q,
                budget=ctx.config.BUFFER_SIZE,
                num_workers=ctx.workers,
                ui=ctx.ui,
                is_debug=ctx.config.debug,
            )
            tg.create_task(memory_throttler.run(), name="MemoryThrottler")

        base_worker_kwargs: BaseWorkerKwargs = {
            "throttler_outbox": ctx.throttler_q,
            "controller_outbox": ctx.controller_q,
            "state_outbox": ctx.state_q,
            "all_complete": ctx.all_complete,
            "ui": ctx.ui,
            "net": ctx.net,
            "is_debug": ctx.config.debug,
        }

        for i in range(ctx.workers):
            if ctx.is_stream:
                worker = StreamDownloadWorker(
                    **base_worker_kwargs,
                    chunks_inbox=ctx.ready_chunks_q,
                    wakeup_event=ctx.worker_events[i],
                    stream_chunks_outbox=ctx.stream_chunks_q,
                    file_discovery_outbox=ctx.file_discovery_q,
                )
            else:
                worker = DiskDownloadWorker(
                    **base_worker_kwargs,
                    chunks_inbox=ctx.chunks_q,
                    wakeup_event=ctx.worker_events[i],
                    disk_outbox=ctx.disk_q,
                    file_limit_outbox=ctx.file_limit_q,
                    fs=ctx.fs,
                )
            tg.create_task(worker.run(), name=f"DownloadWorker-{i}")
        writer = DiskWriter(
            writer_inbox=ctx.writer_q,
            ack_outbox=ctx.ack_q,
            fs=ctx.fs,
            ui=ctx.ui,
            is_debug=ctx.config.debug,
        )
        tg.create_task(writer.run(), name="DiskWriter")

        aggregator = DiskAggregator(
            disk_inbox=ctx.disk_q,
            throttler_outbox=ctx.throttler_q,
            ack_inbox=ctx.ack_q,
            writer_outbox=ctx.writer_q,
            MAX_BUFFER=int(ctx.config.BUFFER_SIZE / 3),
            ui=ctx.ui,
            fs=ctx.fs,
            is_debug=ctx.config.debug,
        )
        tg.create_task(aggregator.run(), name="DiskAggregator")

        analyzer = TelemetryAnalyzer(
            threads=ctx.config.threads,
            _current_limit=ctx.start_works,
            bytes_to_check=ctx.config.MIN_CHUNK,
            state_outbox=ctx.state_q,
            analyzer_checkpoint_event=ctx.analyzer_checkpoint_event,
            controller_outbox=ctx.controller_q,
            ui=ctx.ui,
            stop_analyzer=ctx.stop_analyzer,
            is_debug=ctx.config.debug,
        )
        tg.create_task(analyzer.run(), name="TelemetryAnalyzer")

        autosaver = FileAutosaver(
            interval=60,
            all_complete=ctx.all_complete,
            disk_q=ctx.disk_q,
            reg_events_q=ctx.state_q,
            fs=ctx.fs,
            ui=ctx.ui,
            is_debug=ctx.config.debug,
        )
        tg.create_task(autosaver.run(), name="FileAutosaver")

        controller = TrafficController(
            reg_events_q=ctx.controller_q,
            worker_events=ctx.worker_events,
            stop_analyzer=ctx.stop_analyzer,
            analyzer_checkpoint_event=ctx.analyzer_checkpoint_event,
            dynamic_limit=ctx.start_works,
            prev_dynamic_limit=ctx.start_works,
        )
        tg.create_task(controller.run(), name="TrafficController")

        throttler = ThrottleController(
            throttler_input=ctx.throttler_q,
            speed_limit=ctx.config.speed_limit,
            bytes_to_check=ctx.config.MIN_CHUNK,
            is_debug=ctx.config.debug,
            ui=ctx.ui,
            all_complete=ctx.all_complete,
        )
        tg.create_task(throttler.run(), name="ThrottleController")

    feeder = LinkFeeder(
        links=links,
        expected_checksums=expected_checksums,
        links_outbox=ctx.links_q,
        num_resolvers=ctx.resolvers,
    )
    tg.create_task(feeder.run(), name="LinkFeeder")


async def session_killer(ctx: HydraContext) -> None:
    try:
        await ctx.all_complete.wait()
    except asyncio.CancelledError:
        await ctx.net.close()
        raise


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
                tg.create_task(session_killer(ctx), name="SessionKiller")
                await _bootstrap_engine(ctx, tg, links, expected_checksums)

                if not ctx.config.dry_run:
                    file_gen = None
                    while True:
                        msg = await ctx.file_discovery_q.get()
                        match msg:
                            case File():
                                filename = msg.actual_filename
                                file_gen = file_streamer(
                                    file_obj=msg,
                                    stream_chunk_inbox=ctx.stream_chunks_q,
                                    credit_outbox=ctx.credit_q,
                                    reg_events_q=ctx.state_q,
                                    file_limit_q=ctx.file_limit_q,
                                    ui=ctx.ui,
                                    is_debug=ctx.config.debug,
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
                tg.create_task(session_killer(ctx), name="SessionKiller")
                await _bootstrap_engine(ctx, tg, links, expected_checksums)

            if ctx.config.dry_run:
                _get_shapshot: asyncio.Queue[dict[int, File]] = asyncio.Queue()
                await ctx.state_q.send_data(GetSnapshotCmd(reply_to=_get_shapshot))
                files = await _get_shapshot.get()
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
