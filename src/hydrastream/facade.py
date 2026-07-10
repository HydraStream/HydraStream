import asyncio
import contextlib
from collections.abc import AsyncGenerator, Iterator
from dataclasses import InitVar, field
from itertools import count
from types import TracebackType
from urllib.parse import urlparse

from hydrastream.actors.streamer import file_streamer
from hydrastream.domain.config import HydraConfig, UIConfig
from hydrastream.domain.context import HydraContext, build_context, create_monitor
from hydrastream.domain.entities import Checksum, TypeHash
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.engine import bootstrap_engine, prepare_runtime, teardown_engine
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import MonitorBackend
from hydrastream.messages.base import ask
from hydrastream.messages.io import LinkData
from hydrastream.messages.state import (
    AwaitFileCmd,
    GetReadyFileCmd,
    GetSnapshotCmd,
    GetStatusCmd,
    LinkAddedCmd,
    TaskStatus,
)


@hydra_dataclass
class HydraDaemon:
    config: HydraConfig
    ui_config: UIConfig = field(default_factory=UIConfig)
    initial_ui: InitVar[MonitorBackend | None] = None

    _ui: MonitorBackend = field(init=False)
    _ctx: HydraContext = field(init=False)
    _counter: Iterator[int] = field(init=False, default_factory=count)
    _engine_task: asyncio.Task[None] | None = None
    _is_stopping: bool = False

    def __post_init__(self, initial_ui: MonitorBackend | None) -> None:
        if self.config.custom_monitor is not None:
            self._ui = self.config.custom_monitor
        if initial_ui is None:
            self._ui = create_monitor(config=self.ui_config)
        else:
            self._ui = initial_ui

        self._ctx = build_context(self.config, ui=self._ui)

    async def __aenter__(self) -> "HydraDaemon":
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:

        await self.stop(timeout=5.0)

    def start(self) -> None:
        """Включает завод. Он работает в фоне и ждет задач."""
        if self._engine_task is not None:
            self._ui.log("Daemon is already running.", status=LogStatus.WARNING)
            return

        self._is_stopping = False
        # Запускаем движок стандартным способом в фоне
        self._engine_task = asyncio.create_task(
            self._run_engine_in_background(), name="hydra:engine_main"
        )
        self._ui.log(
            "HydraEngine successfully started in background.", status=LogStatus.INFO
        )

    async def get_stream(self, id: int) -> AsyncGenerator[bytes, None] | None:
        """
        Блокируется, пока Диспетчер не подготовит следующий файл для стриминга.
        Возвращает Асинхронный Генератор с байтами!
        """
        loop = asyncio.get_running_loop()
        reply_future = loop.create_future()

        await self._ctx.state_q.send_data(
            GetReadyFileCmd(file_id=id, reply_to=reply_future)
        )

        try:
            msg = await reply_future

        except asyncio.CancelledError:
            self._ui.log(
                "Engine background task was explicitly cancelled.",
                status=LogStatus.INFO,
            )
            if self.config.debug:
                raise
            return None

        return file_streamer(
            ui=self._ctx.ui,
            is_debug=self._ctx.config.debug,
            file_obj=msg,
            credit_outbox=self._ctx.credit_q,
            reg_events_outbox=self._ctx.state_q,
            file_limit_outbox=self._ctx.file_limit_q,
        )

    async def get_status(self, task_id: int) -> TaskStatus | None:
        """
        Мгновенно возвращает текущий статус задачи.
        Идеально для поллинга (polling) из внешних систем (Web API, Bots).
        """
        try:
            return await ask(
                task_id,
                inbox=self._ctx.state_q,
                msg_factory=GetStatusCmd.create_request,
                timeout=2.0,
            )
        except Exception:
            return None

    async def wait_for_file(self, id: int) -> TaskStatus | None:
        """
        Блокируется, пока файл с указанным ID не будет
        полностью скачан на диск (или не упадет).
        """
        loop = asyncio.get_running_loop()
        reply_future = loop.create_future()

        await self._ctx.state_q.send_data(
            AwaitFileCmd(file_id=id, reply_to=reply_future)
        )

        try:
            return await reply_future
        except asyncio.CancelledError:
            if self.config.debug:
                raise
            return None

        except Exception as e:
            self._ui.log(
                f"Failed to get result for task {id}: {e}", status=LogStatus.ERROR
            )
            if self.config.debug:
                raise
            return None

    async def _run_engine_in_background(self) -> None:
        try:
            prepare_runtime(self._ctx)

            try:
                async with asyncio.TaskGroup() as tg:
                    bootstrap_engine(self._ctx, tg)

                if self._ctx.config.dry_run:
                    files = await ask(
                        inbox=self._ctx.state_q,
                        msg_factory=GetSnapshotCmd.create_request,
                        timeout=5.0,
                        sort_key=(-1,),
                    )
                    self._ctx.ui.dry_run(files, self._ctx.config.output_dir)

            except* Exception as eg:
                self._ctx.ui.log(
                    f"Critical failure in TaskGroup: {eg.exceptions}",
                    status=LogStatus.CRITICAL,
                )  # type: ignore
                if self.config.debug:
                    raise

        except asyncio.CancelledError:
            self._ui.log(
                "Engine background was explicitly cancelled.",
                status=LogStatus.INFO,
            )
            if self.config.debug:
                raise

        except GeneratorExit:
            if self.config.debug:
                raise

        except Exception as e:
            self._ui.log(f"Fatal crash in HydraEngine: {e}", status=LogStatus.CRITICAL)
            if self.config.debug:
                raise
        finally:
            await teardown_engine(self._ctx)

    async def stop(self, timeout: float = 10.0) -> None:
        """Останавливает завод изящно с ограничением по времени."""
        if self._engine_task is None or self._is_stopping:
            return

        self._is_stopping = True
        self._ui.log("Initiating graceful shutdown...", status=LogStatus.INFO)

        try:
            async with asyncio.timeout(timeout):
                # 1. Запускаем каскад смерти через пилюлю фидеру
                self._ctx.links_q.send_poison_pills_nowait(count=self._ctx.resolvers)

                # 2. Ждем штатного закрытия, но не вечно (защита от зависания)
                await asyncio.shield(self._engine_task)
            self._ui.log("Daemon stopped gracefully.", status=LogStatus.INFO)

        except TimeoutError:
            self._ui.log(
                f"Graceful shutdown timed out after {timeout}s! Forcing cancel...",
                status=LogStatus.ERROR,
            )
            # Если завод застрял — жестко рубим корневой TaskGroup движка
            self._engine_task.cancel()

            with contextlib.suppress(asyncio.CancelledError):
                await self._engine_task

            if self.config.debug:
                raise
        finally:
            self._engine_task = None
            self._is_stopping = False

    # ==========================================
    # ПУБЛИЧНОЕ API С ЗАЩИТОЙ
    # ==========================================
    async def add_download(  # noqa
        self,
        url: str,
        priority: int = 0,
        type_hash: TypeHash | None = None,
        expected_checksums: str | None = None,
    ) -> int | None:

        if self._engine_task is None or self._is_stopping:
            self._ui.log(
                f"Rejected download for {url}: Daemon is stopped or stopping.",
                status=LogStatus.WARNING,
            )
            return None
        try:
            result = urlparse(url)
            if not (result.scheme in {"http", "https"} and result.netloc):
                raise ValueError()

        except ValueError:
            self._ui.log(
                f"Rejected: Invalid HTTP/HTTPS URL -> {url}", status=LogStatus.WARNING
            )
            if self.config.debug:
                raise
            return None

        checksum = None
        if type_hash and expected_checksums:
            try:
                checksum = Checksum(algorithm=type_hash, value=expected_checksums)
            except Exception as e:
                self._ui.log(
                    f"Failed to push hash to queue: {e}", status=LogStatus.ERROR
                )
                if self.config.debug:
                    raise
                return None

        elif type_hash:
            self._ui.log(
                f"Skipped checksums for {url}",
                status=LogStatus.WARNING,
            )
            return None

        elif expected_checksums:
            self._ui.log(
                f"Skipped type hash for {url}",
                status=LogStatus.WARNING,
            )
            return None

        id = next(self._counter)
        try:
            link_data = LinkData(id=id, url=url, checksum=checksum)
            await self._ctx.links_q.send_data(link_data, sort_key=(priority, id))
            await self._ctx.state_q.send_data(LinkAddedCmd(link_data=link_data))

            return id

        except Exception as e:
            self._ui.log(f"Failed to push link to queue: {e}", status=LogStatus.ERROR)
            if self.config.debug:
                raise
            return None
