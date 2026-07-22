# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

from hydrastream.actors.aggregator import DiskAggregator
from hydrastream.actors.analyzer import TelemetryAnalyzer
from hydrastream.actors.autosaver import FileAutosaver
from hydrastream.actors.controller import TrafficController
from hydrastream.actors.dispatcher import (
    BaseDispatcherKwargs,
    DiskFileDispatcher,
    StreamFileDispatcher,
)
from hydrastream.actors.memory_throttler import MemoryThrottler
from hydrastream.actors.resolver import (
    BaseResolverKwargs,
    DiskMetadataResolver,
    StreamMetadataResolver,
)
from hydrastream.actors.stater import StateKeeperActor
from hydrastream.actors.throttler import ThrottleController
from hydrastream.actors.worker import (
    BaseWorkerKwargs,
    DiskDownloadWorker,
    StreamDownloadWorker,
)
from hydrastream.actors.writer import DiskWriter
from hydrastream.domain.base_actor import BaseActorKwargs
from hydrastream.domain.context import HydraContext
from hydrastream.exceptions import ExitCode, LogStatus


async def teardown_engine(ctx: HydraContext) -> None:

    ctx.fs.force_close_all()

    await ctx.ui.stop()

    loop = asyncio.get_running_loop()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
    else:
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    await loop.shutdown_default_executor()


def prepare_runtime(ctx: HydraContext) -> None:
    optimal_threads = max(20, ctx.config.threads + 10)
    max_safe_threads = min(optimal_threads, 64)
    custom_pool = ThreadPoolExecutor(
        max_workers=max_safe_threads, thread_name_prefix="HydraIO"
    )
    loop = asyncio.get_running_loop()
    loop.set_default_executor(custom_pool)

    graceful_started = False

    def handle_signal() -> None:
        nonlocal graceful_started
        if graceful_started:
            ctx.ui.log(
                "\n[Сигнал] Повторная команда! Экстренное принудительное завершение...",
                status=LogStatus.CRITICAL,  # Assuming your logger supports severity
            )
            sys.exit(ExitCode.INTERRUPTED)

        ctx.ui.log(
            "\n[Сигнал] Получена команда на остановку. Отменяем главную задачу...",
        )
        # Mark that the first stage has been initiated
        graceful_started = True

        ctx.session_killer.set()
        try:
            ctx.links_q.send_poison_pills_nowait(count=ctx.resolvers)
        except Exception as e:
            ctx.ui.log(f"Не удалось отправить PoisonPill: {e}", status=LogStatus.ERROR)

    # 1. ЗАЩИТА: Отключаем дефолтный KeyboardInterrupt в Python.
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_signal)
    else:
        # Костыль для Windows, так как там add_signal_handler не поддерживается
        signal.signal(signal.SIGINT, lambda sig, frame: handle_signal())


def bootstrap_engine(  # noqa
    ctx: HydraContext,
    tg: asyncio.TaskGroup,
) -> None:
    base_actor_kwargs: BaseActorKwargs = {
        "ui": ctx.ui,
        "is_debug": ctx.config.debug,
    }

    base_resolver_kwargs: BaseResolverKwargs = {
        **base_actor_kwargs,
        "threads": ctx.config.threads,
        "MIN_CHUNK": ctx.config.MIN_CHUNK,
        "inbox": ctx.links_q,
        "files_outbox": ctx.files_q,
        "state_outbox": ctx.state_q,
        "is_dry_run": ctx.config.dry_run,
        "is_verify": ctx.config.is_verify,
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
            if not ctx.config.is_stream
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
        is_stream=ctx.config.is_stream,
        is_dru_run=ctx.config.dry_run,
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

    base_dispatcher_kwargs: BaseDispatcherKwargs = {
        **base_actor_kwargs,
        "inbox": ctx.files_q,
        "limit": ctx.config.threads,
        "chunks_outbox": ctx.chunks_q,
        "file_limit_inbox": ctx.file_limit_q,
        "state_outbox": ctx.state_q,
    }

    if not ctx.config.dry_run:
        dispatcher = (
            StreamFileDispatcher(
                **base_dispatcher_kwargs,
            )
            if ctx.config.is_stream
            else DiskFileDispatcher(
                **base_dispatcher_kwargs,
                fs=ctx.fs,
            )
        )

        if ctx.config.is_stream:
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
            "sleep_signals_indox": ctx.sleep_signals_q,
            "wait_in_sleep_inbox": ctx.wait_in_sleep_q,
            "net": ctx.net,
        }
        workers: list[DiskDownloadWorker | StreamDownloadWorker] | None = []

        for _ in range(ctx.workers):
            workers.append(
                StreamDownloadWorker(
                    **base_worker_kwargs,
                    inbox=ctx.ready_chunks_q,
                )
                if ctx.config.is_stream
                else DiskDownloadWorker(
                    **base_worker_kwargs,
                    inbox=ctx.chunks_q,
                    aggregator_outbox=ctx.aggregator_q,
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
            inbox=ctx.aggregator_q,
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
            aggregator_outbox=ctx.aggregator_q,
            reg_events_q=ctx.state_q,
            fs=ctx.fs,
        )

        controller = TrafficController(
            **base_actor_kwargs,
            inbox=ctx.controller_q,
            sleep_signals_outdox=ctx.sleep_signals_q,
            wait_in_sleep_outbox=ctx.wait_in_sleep_q,
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
        finally:
            await ctx.net.close()

    tg.create_task(session_killer(), name="stage:killer")

    async def stage_0_stating() -> None:

        async with asyncio.TaskGroup() as stage_tg:
            stage_tg.create_task(stater.run(), name="stater")

    tg.create_task(stage_0_stating(), name="stage:stater")

    async def stage_1_resolving() -> None:
        try:
            async with asyncio.TaskGroup() as stage_tg:
                for i, resolver in enumerate(resolvers):
                    stage_tg.create_task(resolver.run(), name=f"resolver_{i}")
            print("Your debug message here 1", file=sys.__stderr__, flush=True)
            if ctx.config.dry_run:
                await ctx.ui.refresh_ui(ctx.state_q)
        finally:
            if not ctx.config.dry_run:
                ctx.files_q.send_poison_pills_nowait(count=1)
                ctx.file_limit_q.send_poison_pills_nowait(count=1)
            else:
                ctx.state_q.send_poison_pills_nowait(count=1)

    tg.create_task(stage_1_resolving(), name="stage:resolvers")

    if (
        not ctx.config.dry_run  # noqa: PLR0916
        and dispatcher is not None
        and workers is not None
        and aggregator is not None
        and analyzer is not None
        and autosaver is not None
        and controller is not None
        and throttler is not None
    ):

        async def stage_2_dispatching() -> None:
            try:
                async with asyncio.TaskGroup() as stage_tg:
                    stage_tg.create_task(dispatcher.run(), name="dispatcher")
            finally:
                if not ctx.config.is_stream:
                    ctx.chunks_q.send_poison_pills_nowait(count=ctx.workers)
                    ctx.sleep_signals_q.send_poison_pills_nowait(count=ctx.workers)
                    ctx.wait_in_sleep_q.send_poison_pills_nowait(count=ctx.workers)
                else:
                    ctx.chunks_q.send_poison_pills_nowait(count=1)
                    ctx.credit_q.send_poison_pills_nowait(count=1)

        tg.create_task(stage_2_dispatching(), name="stage:dispatcher")

        if ctx.config.is_stream and memory_throttler is not None:

            async def stage_3_memory_throttling() -> None:
                try:
                    async with asyncio.TaskGroup() as stage_tg:
                        stage_tg.create_task(
                            memory_throttler.run(), name="memory_throttler"
                        )
                finally:
                    ctx.ready_chunks_q.send_poison_pills_nowait(count=ctx.workers)
                    ctx.sleep_signals_q.send_poison_pills_nowait(count=ctx.workers)
                    ctx.wait_in_sleep_q.send_poison_pills_nowait(count=ctx.workers)

            tg.create_task(stage_3_memory_throttling(), name="stage:feeder")

        async def stage_4_working() -> None:
            try:
                async with asyncio.TaskGroup() as stage_tg:
                    for i, worker in enumerate(workers):
                        stage_tg.create_task(worker.run(), name=f"worker_{i}")
            finally:
                if not ctx.config.is_stream:
                    ctx.aggregator_q.send_poison_pills_nowait(count=1)

                ctx.analyzer_q.send_poison_pills_nowait(count=1)
                ctx.controller_q.send_poison_pills_nowait(count=1)
                ctx.autosaver_q.send_poison_pills_nowait(count=1)
                ctx.throttler_q.send_poison_pills_nowait(count=1)

        tg.create_task(stage_4_working(), name="stage:feeder")

        async def stage_5_servicing() -> None:
            try:
                async with asyncio.TaskGroup() as stage_tg:
                    if not ctx.config.is_stream:
                        stage_tg.create_task(aggregator.run(), name="aggregator")
                    stage_tg.create_task(analyzer.run(), name="analyzer")
                    stage_tg.create_task(autosaver.run(), name="autosaver")
                    stage_tg.create_task(controller.run(), name="traffic_tontroller")
                    stage_tg.create_task(throttler.run(), name="throttle_controller")
                await ctx.ui.refresh_ui(ctx.state_q)

            finally:
                if not ctx.config.is_stream:
                    ctx.writer_q.send_poison_pills_nowait(count=1)
                else:
                    ctx.state_q.send_poison_pills_nowait(count=1)

        tg.create_task(stage_5_servicing(), name="stage:service")

        if not ctx.config.is_stream and writer is not None:

            async def stage_6_writing() -> None:
                try:
                    async with asyncio.TaskGroup() as stage_tg:
                        stage_tg.create_task(writer.run(), name="writer")
                    await ctx.ui.refresh_ui(ctx.state_q)
                finally:
                    ctx.state_q.send_poison_pills_nowait(count=1)

            tg.create_task(stage_6_writing(), name="stage:writer")
