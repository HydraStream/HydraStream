# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
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
from hydrastream.actors.resolver import DiskMetadataResolver, StreamMetadataResolver
from hydrastream.actors.stater import StateKeeperActor
from hydrastream.actors.throttler import ThrottleController
from hydrastream.actors.worker import DiskDownloadWorker, StreamDownloadWorker
from hydrastream.actors.writer import DiskWriter
from hydrastream.domain.context import HydraContext
from hydrastream.domain.entities import Checksum, TypeHash
from hydrastream.exceptions import (
    LogStatus,
)

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

    if not ctx.stream:
        save_all_states(ctx, ctx.files)
        for file_obj in ctx.files.values():
            if file_obj.fd:
                ctx.fs.close_file(file_obj.fd)
    await ui_stop(ctx.ui)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)

    await loop.shutdown_default_executor()


async def stop(ctx: HydraContext, complete: bool = False) -> None:
    if ctx.is_stopping:
        return
    ctx.is_stopping = True

    if not complete:
        ctx.ui.cancelled = True
        await ctx.ui.log(
            "Interrupt signal received. Initiating graceful shutdown...",
            status=LogStatus.INTERRUPT,
        )

        if ctx.stream:
            with contextlib.suppress(asyncio.QueueFull):
                ctx.queues.stream.put_nowait(
                    Envelope(sort_key=(-1,), is_poison_pill=True)
                )
                ctx.queues.file_discovery.put_nowait(-1)


async def prepare_runtime(ctx: HydraContext, loop: asyncio.AbstractEventLoop) -> None:
    optimal_threads = max(20, ctx.config.threads + 10)
    max_safe_threads = min(optimal_threads, 64)
    custom_pool = ThreadPoolExecutor(
        max_workers=max_safe_threads, thread_name_prefix="HydraIO"
    )
    loop.set_default_executor(custom_pool)

    await ui_start(ctx.ui)
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
    for i in range(ctx.resolvers):
        resolver = (
            DiskMetadataResolver(
                threads=ctx.config.threads,
                MIN_CHUNK=ctx.config.MIN_CHUNK,
                links_inbox=ctx.links_q,
                files_outbox=ctx.files_q,
                state_outbox=ctx.state_q,
                barrier=ctx.resolver_barrier,
                all_complete=ctx.all_complete,
                is_dry_run=ctx.config.dry_run,
                is_verify=ctx.config.verify,
                is_debug=ctx.config.debug,
                ui=ctx.ui,
                net=ctx.net,
                fs=ctx.fs,
            )
            if not ctx.is_stream
            else StreamMetadataResolver(
                threads=ctx.config.threads,
                MIN_CHUNK=ctx.config.MIN_CHUNK,
                STREAM_CHUNK_SIZE=ctx.config.STREAM_CHUNK_SIZE,
                links_inbox=ctx.links_q,
                files_outbox=ctx.files_q,
                state_outbox=ctx.state_q,
                barrier=ctx.resolver_barrier,
                all_complete=ctx.all_complete,
                is_dry_run=ctx.config.dry_run,
                is_verify=ctx.config.verify,
                is_debug=ctx.config.debug,
                ui=ctx.ui,
                net=ctx.net,
            )
        )
        tg.create_task(resolver.run(), name=f"MetadataResolver: {i}")

    dispatcher = (
        DiskFileDispatcher(
            limit=ctx.config.threads,
            files_inbox=ctx.files_q,
            chunks_outbox=ctx.chunks_q,
            file_limit_inbox=ctx.file_limit_q,
            ui=ctx.ui,
            is_debug=ctx.config.debug,
            fs=ctx.fs,
        )
        if not ctx.is_stream
        else StreamFileDispatcher(
            limit=ctx.config.threads,
            files_inbox=ctx.files_q,
            chunks_outbox=ctx.chunks_q,
            file_limit_inbox=ctx.file_limit_q,
            file_discovery=ctx.file_discovery_q,
            ui=ctx.ui,
            is_debug=ctx.config.debug,
        )
    )
    tg.create_task(dispatcher.run(), name="FileDispatcher")

    memory_throttler = MemoryThrottler(
        chunk_inbox=ctx.chunks_q,
        chunk_outbox=ctx.chunks_q,
        credit_inbox=ctx.credit_q,
        budget=ctx.config.BUFFER_SIZE,
    )
    tg.create_task(memory_throttler.run(), name="MemoryThrottler")

    for i in range(ctx.workers):
        worker = (
            DiskDownloadWorker(
                chunks_inbox=ctx.chunks_q,
                throttler_outbox=ctx.throttler_q,
                controller_outbox=ctx.controller_q,
                state_outbox=ctx.state_q,
                wakeup_event=ctx.worker_events[i],
                all_complete=ctx.all_complete,
                barrier=ctx.worker_barrier,
                ui=ctx.ui,
                net=ctx.net,
                is_debug=ctx.config.debug,
                disk_outbox=ctx.disk_q,
                file_limit_outbox=ctx.file_limit_q,
                fs=ctx.fs,
            )
            if not ctx.is_stream
            else StreamDownloadWorker(
                chunks_inbox=ctx.chunks_q,
                throttler_outbox=ctx.throttler_q,
                controller_outbox=ctx.controller_q,
                state_outbox=ctx.state_q,
                wakeup_event=ctx.worker_events[i],
                all_complete=ctx.all_complete,
                barrier=ctx.worker_barrier,
                ui=ctx.ui,
                net=ctx.net,
                is_debug=ctx.config.debug,
                stream_chunks_outbox=ctx.stream_chunks_q,
                file_discovery_outbox=ctx.file_discovery_q,
            )
        )
        tg.create_task(worker.run(), name=f"DownloadWorker: {i}")

    writer = DiskWriter(
        writer_inbox=ctx.writer_q,
        ack_outbox=ctx.ack_q,
        fs=ctx.fs,
        ui=ctx.ui,
        is_debug=ctx.config.debug,
    )
    tg.create_task(writer.run(), name="DiskWriter")

    stater = StateKeeperActor(
        stater_inbox=ctx.state_q,
        bytes_to_check=ctx.config.MIN_CHUNK,
        analyzer_checkpoint_event=ctx.analyzer_checkpoint_event,
        throttler_checkpoint_event=ctx.throttler_checkpoint_event,
        ui=ctx.ui,
        is_debug=ctx.config.debug,
    )
    tg.create_task(stater.run(), name="StateKeeper")

    aggregator = DiskAggregator(
        disk_inbox=ctx.disk_q,
        throttler_outbox=ctx.throttler_q,
        ack_inbox=ctx.ack_q,
        writer_outbox=ctx.writer_q,
        flush_event=ctx.flush_event,
        MAX_BUFFER=int(ctx.config.BUFFER_SIZE / 3),
        ui=ctx.ui,
        is_debug=ctx.config.debug,
    )
    tg.create_task(aggregator.run(), name="DiskAggregator")

    analyzer = TelemetryAnalyzer(
        threads=ctx.config.threads,
        _current_limit=ctx.start_works,
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
        flush_event=ctx.flush_event,
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
        is_debug=ctx.config.debug,
        ui=ctx.ui,
        all_complete=ctx.all_complete,
        throttler_checkpoint_event=ctx.throttler_checkpoint_event,
    )
    tg.create_task(throttler.run(), name="ThrottleController")

    feeder = LinkFeeder(
        links=links, expected_checksums=expected_checksums, links_outbox=ctx.links_q
    )
    tg.create_task(feeder.run(), name="LinkFeeder")


async def session_killer(ctx: HydraContext) -> None:
    try:
        await ctx.sync.all_complete.wait()
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
                        file_id = await ctx.file_discovery_q.get()

                        if file_id == -1:
                            break
                        filename = ctx.files[file_id].meta.filename

                        file_gen = streamer(ctx, file_id)

                        yield filename, file_gen

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
    stream = False

    loop = asyncio.get_running_loop()
    await prepare_runtime(ctx, loop)

    try:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(session_killer(ctx), name="SessionKiller")
                await _bootstrap_engine(ctx, tg, links, expected_checksums)
            if ctx.config.dry_run:
                await ctx.ui.dry_run(ctx.files, ctx.config.output_dir)
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
