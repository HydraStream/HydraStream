# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import shutil
import sys
import time
import typing
from abc import ABC
from dataclasses import asdict, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict, final, override

import orjson
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    FileSizeColumn,
    Progress,
    ProgressColumn,
    SpinnerColumn,
    Task,
    TaskID,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)
from rich.progress_bar import ProgressBar
from rich.rule import Rule
from rich.table import Column, Table
from rich.text import Text

from hydrastream.actors.stater import GetUIDeltasCmd, StateKeeperMsg
from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import HydraError, LogFileError, LogStatus
from hydrastream.interfaces import MonitorBackend
from hydrastream.messages.base import ActorFifoQueue, PoisonPill, ask
from hydrastream.utils import format_size


class BaseMonitorKwargs(TypedDict):
    is_verify: bool
    is_dry_run: bool
    is_debug: bool

    log_file: Path


@hydra_dataclass
class BaseMonitor(MonitorBackend, ABC):
    _is_running: bool = False
    _is_cancelled: bool = False
    is_stream: bool = False
    is_verify: bool = True
    is_dry_run: bool = False
    _console: Console = field(default_factory=lambda: Console(stderr=True))

    """Всё, что касается записи логов на жесткий диск."""

    log_file: Path
    _log_throttle: dict[str, float] = field(default_factory=dict[str, float])
    _log_fd: typing.TextIO | None = field(default=None, init=False, repr=False)
    _log_queue: asyncio.Queue[str | None] = field(
        default_factory=asyncio.Queue[str | None], init=False
    )
    _log_task: asyncio.Task[None] | None = field(default=None, init=False)

    """Чисто статистика и счетчики загрузки."""
    _has_hash: int = 0
    _has_ranges: int = 0
    _start_time: float = 0.0
    _total_bytes: int = 0
    _download_bytes: int = 0
    _total_files: int = 0
    _files_completed: int = 0
    _files_size: dict[int, int] = field(default_factory=dict[int, int])

    is_debug: bool = False

    def __post_init__(self) -> None:
        if self.is_debug:
            self._console = Console(stderr=True, file=sys.__stderr__)

        self.log_file = Path(self.log_file).expanduser().resolve()
        self.log_file /= "hydra.log"

    @override
    def log(
        self,
        message: str | Rule | Table,
        *,
        status: LogStatus | str = "",
        progress: bool = False,
        throttle_key: str | None = None,
        throttle_sec: float = 10.0,
        **kwargs: object,
    ) -> None:
        if status == LogStatus.INTERRUPT:
            self._is_cancelled = True

        # 1. Троттлинг логов
        if throttle_key:
            now = time.monotonic()
            last_time = self._log_throttle.get(throttle_key, 0.0)
            if now - last_time < throttle_sec:
                return
            self._log_throttle[throttle_key] = now

        # 2. Логирование в файл (Делаем capture ТОЛЬКО здесь, если файл включен)
        if self.log_file:
            if isinstance(message, Table | Rule):
                with self._console.capture() as capture:
                    self._console.print(message)
                rendered_str = capture.get()
                clean_message_body = Text.from_ansi(rendered_str).plain
            else:
                clean_message_body = message

            clean_message_body = clean_message_body.strip("\n")

            if isinstance(message, Table):
                clean_message_body = "\n" + clean_message_body

            timestamp = datetime.now(UTC).strftime("[%H:%M:%S UTC]")
            final_msg = f"{timestamp} {clean_message_body}"
            self._log_queue.put_nowait(final_msg)

        # 3. Вывод на экран (Передаем ОРИГИНАЛЬНЫЙ message для сохранения цветов Rich)
        self._display_log(message, status, progress=progress, **kwargs)

    @final
    @override
    def report(self, error: HydraError, **log_extra: Any) -> None:
        """
        Передаем данные ошибки в логгер.
        asdict() превратит поля датакласса в ключи для JSON-лога.
        """
        # Убираем системные поля, которые не нужны в JSON-атрибутах
        data = asdict(error)
        for key in ["exit_code", "log_status", "message_tpl", "formatted_msg"]:
            data.pop(key, None)

        self.log(
            f"[{error.error_id}] {error.formatted_msg}",
            status=error.log_status,
            **data,  # Все поля (filename, required и т.д.) попадут в JSON!
            **log_extra,  # Дополнительные флаги типа throttle_key
        )

    def _date_print(self) -> None:
        current_date = datetime.now(UTC).strftime("%Y-%m-%d UTC")
        self.log(f"--- {current_date} ---")

    @final
    async def _log_worker(self) -> None:
        """
        Фоновый воркер. Живет всё время работы программы.
        Берет строки из очереди и пишет в ОТКРЫТЫЙ файл.
        """
        if not self._log_fd:
            return

        while True:
            msg = await self._log_queue.get()

            # Ядовитая пилюля для остановки логгера
            if msg is None:
                break

            try:
                self._log_fd.write(f"{msg}\n")
                self._log_fd.flush()  # Гарантируем, что строка сразу упала на диск
            except OSError as e:
                if self.is_debug:
                    raise
                err = LogFileError(path=str(self.log_file), original_err=str(e))
                self.log(f"{err.formatted_msg}", status=LogStatus.WARNING)

                self._log_fd.close()
                self._log_fd = None
                break

    @override
    def add_file(
        self, file_id: int, filename: str, total_size: int | None = None
    ) -> None:
        if total_size is not None:
            self._total_bytes += total_size
            self._total_files += 1
            self._files_size[file_id] = total_size

    @override
    def update_progress(self, file_id: int, advance_bytes: int) -> None:
        pass

    @override
    def update_filename(self, file_id: int, new_filename: str) -> None:
        pass

    @override
    def done(self, file_id: int, filename: str) -> None:
        if self._files_size.get(file_id, 0):
            self._files_completed += 1
            self.log(f"Done: {filename}", status=LogStatus.SUCCESS, progress=True)

    @final
    def _safe_init(self) -> None:
        """Пытается создать папку для лога. Если не выходит - падает на дефолт."""
        try:
            self.log_file.parent.mkdir(parents=True, exist_ok=True)
            # Пробуем открыть файл на дозапись (тест прав доступа)
            with self.log_file.open("a", encoding="utf-8"):
                pass
        except OSError:
            # Если юзер передал /root/secret/dir/ и у нас нет прав,
            # откатываемся в текущую директорию!
            fallback = Path.cwd() / "hydra.log"
            self.log_file = fallback

    @final
    @override
    def start(self) -> None:
        """Запускает фонового воркера (вызывать внутри async_main)"""
        if self._is_running:
            return

        self._is_running = True

        try:
            self._safe_init()
            self._log_fd = self.log_file.open("a", encoding="utf-8")
            self._log_task = asyncio.create_task(self._log_worker())
            self._start_time = time.monotonic()
            self.log("--- Session Started ---")
            self._date_print()
            self._ui_start()
        except OSError as e:
            if self.is_debug:
                raise
            self._log_fd = None

            err = LogFileError(path=str(self.log_file), original_err=str(e))
            self.log(
                f"[bold yellow]LOGGING DISABLED:[/] {err.formatted_msg}",
                status=LogStatus.WARNING,
            )

    @final
    def _handle_exit(self) -> None:
        self._is_running = False
        size_str = (
            f"{format_size(self._download_bytes)} / {format_size(self._total_bytes)}"
        )

        if not self.is_dry_run:
            elapsed = time.monotonic() - self._start_time
            avg_speed = (
                f"{format_size(self._download_bytes / elapsed)}/s"
                if elapsed > 0
                else "0 B/s"
            )

            mins, secs = divmod(int(elapsed), 60)
            hours, mins = divmod(mins, 60)
            time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

            # Определяем статус и цвет заголовка
            status_word = "CANCELLED" if self._is_cancelled else "SUCCESS"
            title_color = "bold red" if self._is_cancelled else "bold green"

            # Создаем таблицу без лишних рамок по бокам для компактности
            table = Table(
                title=f"[{title_color}]--- Final Report ({status_word}) ---[/]",
                padding=(0, 1),
            )

            # Добавляем две колонки: для названия метрики и для значения
            table.add_column("Metric", style="cyan", justify="left")
            table.add_column("Value", style="magenta", justify="center")

            # Наполняем таблицу данными
            table.add_row(
                "Total files:", f"{self._files_completed}/{self._total_files}"
            )
            table.add_row("Total Data:", size_str)
            table.add_row("Average Speed:", avg_speed)
            table.add_row("Total Time:", time_str)
            report_dict = {
                "total_files": self._files_completed,
                "total_bytes": self._download_bytes,
                "average_speed": avg_speed,
                "time_elapsed_sec": elapsed,
            }

            self.log(
                table,
                status=LogStatus.INFO,
                progress=False,
                throttle_key=None,
                throttle_sec=10.0,
                **report_dict,
            )

        else:
            size_str = f"{format_size(self._total_bytes)}"
            table = Table()
            table.add_column("Metric", style="cyan", justify="left")
            table.add_column("Value", style="magenta", justify="center")

            table.add_row("[white]Total files:", f"[green3]{self._total_files}[/]")
            table.add_row("[white]Total Data:", f"[bold cyan]{size_str}[/]")
            if self.is_verify:
                table.add_row(
                    "[white]Hash Found:",
                    f"[bold yellow]{self._has_hash}/{self._total_files}[/]",
                )
            table.add_row(
                "[white]Ranges:",
                f"[bold magenta]{self._has_ranges}/{self._total_files}[/]",
            )

            self.log(
                table,
            )

    @final
    @override
    async def stop(self) -> None:
        if not self._is_running:
            return
        self._handle_exit()
        self._ui_stop()
        self.log("--- Session Finished ---")

        if self._log_task:
            self._log_queue.put_nowait(None)
            await self._log_task

        if self._log_fd:
            self._log_fd.close()
            self._log_fd = None

    @override
    def dry_run(self, files: dict[int, File], output_dir: str | Path) -> None:
        """Выводит отчет о том, что БЫЛО БЫ сделано, без фактического скачивания."""
        table = Table(
            title="[bold yellow] DRY RUN REPORT (No data will be downloaded)[/]"
        )
        table.add_column("Filename", style="cyan", justify="left", no_wrap=True)
        table.add_column("Size", justify="center")
        table.add_column("Chunks", justify="center")
        if self.is_verify:
            table.add_column("Hash Found", justify="center")
        table.add_column("Ranges", justify="center")

        for f in files.values():
            f.create_chunks()
            str_size = format_size(f.meta.content_length)
            if self.is_verify and f.meta.expected_checksum:
                self._has_hash += 1

            if f.meta.supports_ranges:
                ranges = "✅"
                self._has_ranges += 1
            else:
                ranges = "❌ (Fallback to 1 thread)"

            if self.is_verify:
                table.add_row(
                    f.meta.original_filename,
                    str_size,
                    str(len(f.chunks)),
                    "✅" if f.meta.expected_checksum else "❌",
                    ranges,
                )
            else:
                table.add_row(
                    f.meta.original_filename, str_size, str(len(f.chunks)), ranges
                )

        self.log(table, progress=True)
        if not self.is_stream:
            self._check_storage_capacity(output_path=output_dir)

    @final
    def _check_storage_capacity(self, output_path: str | Path) -> None:
        """Проверяет наличие свободного места на диске перед началом загрузки."""

        output_dir = Path(output_path).expanduser().resolve()
        check_dir = output_dir if output_dir.exists() else output_dir.parent

        try:
            free_space = shutil.disk_usage(check_dir).free
            required = self._total_bytes

            if free_space < required:
                self.log(
                    "\n[bold red] DANGER: Insufficient disk space![/]", progress=True
                )
                self.log(
                    f"[red]Required: {format_size(required)} | "
                    f"Available: {format_size(free_space)}[/]",
                    progress=True,
                )
            else:
                self.log(
                    f"\nDisk space check passed ({format_size(free_space)} free).\n",
                    status=LogStatus.SUCCESS,
                    progress=True,
                )

        except OSError as e:
            if self.is_debug:
                raise
            self.log(
                f"Warning: Could not check disk space: {e}",
                status=LogStatus.WARNING,
            )

    def _display_log(
        self,
        message: str | Rule | Table,
        status: LogStatus | str,
        *,
        progress: bool = False,
        **kwargs: object,
    ) -> None:
        pass

    @override
    def bind_to_state_keeper(
        self, state_q: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    ) -> None:
        pass

    @override
    async def refresh_ui(
        self, state_q: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    ) -> None:
        deltas = await ask(
            inbox=state_q,
            msg_factory=GetUIDeltasCmd.create_request,
            timeout=5.0,
            sort_key=(-1,),
        )
        for bytes_to_advance in deltas.values():
            self._download_bytes += bytes_to_advance

    def _ui_start(self) -> None:
        pass

    def _ui_stop(self) -> None:
        pass


@hydra_dataclass
class QuietMonitor(BaseMonitor): ...


@hydra_dataclass
class JsonMonitor(BaseMonitor):
    @override
    def _display_log(
        self,
        message: str | Rule | Table,
        status: LogStatus | str,
        progress: bool = False,
        **kwargs: object,
    ) -> None:
        if isinstance(message, Table | Rule):
            with self._console.capture() as capture:
                self._console.print(message)
            rendered_str = capture.get()
            clean_message_body = Text.from_ansi(rendered_str).plain
        else:
            clean_message_body = message

        clean_message_body = clean_message_body.strip("\n")

        if isinstance(message, Table):
            clean_message_body = "\n" + clean_message_body

        log_record = {
            "timestamp": datetime.now(UTC),
            "level": status.upper(),
            "message": clean_message_body,
            **kwargs,
        }
        # Сериализуем в байты, потом в строку
        sys.stderr.buffer.write(orjson.dumps(log_record) + b"\n")

    @override
    def dry_run(self, files: dict[int, File], output_dir: str | Path) -> None:
        report_data = {
            "total_files": self._total_files,
            "total_bytes": self._total_bytes,
            "files": [
                {
                    "filename": f.actual_filename,
                    "size_bytes": f.meta.content_length,
                    "chunks": len(f.chunks),
                    "supports_ranges": f.meta.supports_ranges,
                    "algorithm": f.meta.expected_checksum.algorithm
                    if f.meta.expected_checksum
                    else None,
                    "expected_hash": f.meta.expected_checksum.value
                    if f.meta.expected_checksum
                    else None,
                }
                for f in files.values()
            ],
        }

        self.log(
            "DRY_RUN_REPORT",
            status=LogStatus.INFO,
            progress=False,
            throttle_key=None,
            throttle_sec=10,
            **report_data,
        )
        if self.is_stream:
            self._check_storage_capacity(output_dir)


@hydra_dataclass
class PlainMonitor(BaseMonitor):
    @override
    def _display_log(
        self,
        message: str | Rule | Table,
        status: LogStatus | str,
        progress: bool = False,
        **kwargs: object,
    ) -> None:
        timestamp = datetime.now(UTC).strftime("[%H:%M:%S UTC]")

        # Если пришла обычная строка, убираем у нее скрытые \n на конце
        if isinstance(message, str):
            message = message.rstrip("\n")
            final_msg = f"{timestamp} {message}"
            self._console.print(final_msg)
        else:
            # Если пришла таблица или линия, печатаем таймстамп, а под ним — объект Rich
            self._console.print(f"{timestamp}")
            self._console.print(message)


def get_gradient_color(percentage: float) -> str:
    p = max(0, min(100, percentage or 0))
    if p < 50:
        r, g, b = 255, int((p / 50) * 255), 0
    else:
        r, g, b = int(255 - ((p - 50) / 50) * 255), 255, 0
    return f"#{r:02x}{g:02x}{b:02x}"


class GradientBar(BarColumn):
    def render(self, task: Task) -> ProgressBar:
        if task.total is None:
            self.complete_style = "cyan"
        elif task.finished:
            self.complete_style = "bold bright_green blink"
        else:
            self.complete_style = get_gradient_color(task.percentage)
        return super().render(task)


class GradientPercent(ProgressColumn):
    @override
    def render(self, task: Task) -> Text:
        if task.total is None:
            return Text(" CALC ", style="yellow")
        p = task.percentage
        color = get_gradient_color(p)
        return Text(f"{p:>5.1f}%", style=f"bold {color}")


@hydra_dataclass
class RichMonitor(BaseMonitor):
    dynamic_title: str = ""

    refresh_per_second: int = 10
    renewal_rate: float = field(init=False)

    tasks: dict[int, TaskID] = field(default_factory=dict[int, TaskID])
    active_files: set[int] = field(default_factory=set[int])

    refresh: asyncio.Task[None] = field(init=False)
    rich: Progress = field(init=False)
    live: Live = field(init=False)

    @override
    def __post_init__(self) -> None:
        BaseMonitor.__post_init__(self)

        self.renewal_rate = 1 / self.refresh_per_second

        self.rich = Progress(
            SpinnerColumn("aesthetic"),
            TextColumn("[bold yellow]{task.description}"),
            TextColumn(
                "[bold blue]{task.fields[filename]}",
                justify="left",
                table_column=Column(overflow="ellipsis", no_wrap=True, width=30),
            ),
            GradientBar(bar_width=None, finished_style="green"),
            GradientPercent(),
            "•",
            FileSizeColumn(),
            "/",
            TotalFileSizeColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            console=self._console,
            transient=False,
            expand=True,
        )

        self.live = Live(
            get_renderable=self._make_panel,
            console=self._console,
            auto_refresh=True,
            refresh_per_second=10,
            transient=False,
        )

    @override
    def log(
        self,
        message: str | Rule | Table,
        *,
        status: LogStatus | str = "",
        progress: bool = False,
        throttle_key: str | None = None,
        throttle_sec: float = 10.0,
        **kwargs: object,
    ) -> None:
        if status == LogStatus.INTERRUPT:
            self._is_cancelled = True
        if throttle_key:
            now = time.monotonic()
            last_time = self._log_throttle.get(throttle_key, 0.0)
            if now - last_time < throttle_sec:
                return
            self._log_throttle[throttle_key] = now

        if self.log_file:
            timestamp = datetime.now(UTC).strftime("[%H:%M:%S UTC]")
            final_msg = f"{timestamp} {message}"
            clean_msg = Text.from_markup(str(final_msg)).plain
            self._log_queue.put_nowait(clean_msg)

        self._display_log(message, status, progress, **kwargs)

    @override
    def _display_log(
        self,
        message: str | Rule | Table,
        status: LogStatus | str,
        progress: bool = False,
        **kwargs: object,
    ) -> None:
        renderable = self._formatting_log(message, status)
        if progress or status in {
            LogStatus.WARNING,
            LogStatus.ERROR,
            LogStatus.CRITICAL,
            LogStatus.INTERRUPT,
        }:
            self.rich.console.print(renderable)

    @staticmethod
    def _truncate_filename(name: str, w: int = 30) -> str:
        return (
            f"{name[: w // 2 - 1]}...{name[-w // 2 + 2 :]}" if len(name) > w else name
        )

    def _date_print(self) -> None:
        current_date = datetime.now(UTC).strftime("%Y-%m-%d UTC")
        date_header = f"[bold cyan] Date: {current_date}[/]"

        self.rich.console.print(Rule(date_header))
        self.log(f"--- {current_date} ---")

    @staticmethod
    def _formatting_log(
        message: str | Rule | Table,
        status: str | LogStatus,
    ) -> Panel | str | Rule | Table:
        timestamp = datetime.now(UTC).strftime("[%H:%M:%S UTC]")
        formatted_msg = f"{timestamp} {message}"

        match status.upper():
            case "CRITICAL" | "INTERRUPT":
                renderable = Panel(
                    f"[bold red]{message}[/]\n[dim white]"
                    f"Partial data may have been saved.",
                    title="[bold red]Interrupted",
                    border_style="red",
                    expand=False,
                )
            case "ERROR":
                renderable = Panel(
                    f"[bold red]{message}[/]",
                    title="Error",
                    border_style="red",
                    padding=(0, 1),
                )
            case "WARNING":
                renderable = f"[yellow]{formatted_msg}[/]"
            case "INFO":
                renderable = f"[white]{formatted_msg}[/]"
            case "SUCCESS":
                renderable = f"[green]{formatted_msg}[/]"
            case _:
                renderable = message
        return renderable

    @override
    def add_file(
        self, file_id: int, filename: str, total_size: int | None = None
    ) -> None:
        if total_size is not None:
            self._total_bytes += total_size
            self._total_files += 1
            self._files_size[file_id] = total_size

        t_filename = self._truncate_filename(filename)
        if total_size is None:
            task_id = self.rich.add_task(
                "Download Hash for", filename=t_filename, total=total_size
            )
        else:
            task_id = self.rich.add_task(
                "Download file",
                filename=t_filename,
                total=total_size,
                visible=False,
            )
        self.tasks[file_id] = task_id
        self._update_panel_title()

    @override
    def update_filename(self, file_id: int, new_filename: str) -> None:
        self.rich.update(self.tasks[file_id], description=new_filename)

    async def _ui_refresh_actor(
        self, state_keeper_q: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    ) -> None:
        while self._is_running:
            try:
                # 1. Засыпаем до следующего "кадра" (например, на 0.1 сек)
                await asyncio.sleep(self.renewal_rate)

                # 2. Запрашиваем дельты у базы данных (StateKeeper)
                deltas = await ask(
                    inbox=state_keeper_q,
                    msg_factory=GetUIDeltasCmd.create_request,
                    timeout=5.0,
                    sort_key=(-1,),
                )

                # 3. Отрисовываем!
                for file_id, bytes_to_advance in deltas.items():
                    if file_id in self.tasks:
                        self.rich.update(
                            self.tasks[file_id],
                            advance=bytes_to_advance,
                            visible=True,
                        )
                    self._download_bytes += bytes_to_advance

                    if file_id not in self.active_files:
                        self.active_files.add(file_id)
                        self._update_panel_title()

            except Exception as e:
                if self.is_debug:
                    raise
                self.log(f"UI Refresh Error: {e!r}", status=LogStatus.ERROR)

    def _update_panel_title(self) -> None:
        active = len(self.active_files)

        self.dynamic_title = (
            f"[bold white][green]{self._files_completed}[/]/"
            f"[blue]{self._total_files}[/] Files | [yellow]{active} Active[/]"
        )

    @override
    def dry_run(self, files: dict[int, File], output_dir: str | Path) -> None:
        """Выводит отчет о том, что БЫЛО БЫ сделано, без фактического скачивания."""

        table = Table(
            title="[bold yellow] DRY RUN REPORT (No data will be downloaded)[/]"
        )
        table.add_column("Filename", style="cyan", no_wrap=True)
        table.add_column("Size", justify="right")
        table.add_column("Chunks", justify="right")
        if self.is_verify:
            table.add_column("Hash Found", justify="center")
        table.add_column("Ranges", justify="center")

        for f in files.values():
            f.create_chunks()
            str_size = format_size(f.meta.content_length)
            if self.is_verify and f.meta.expected_checksum:
                self._has_hash += 1

            if f.meta.supports_ranges:
                ranges = "✅"
                self._has_ranges += 1
            else:
                ranges = "❌ (Fallback to 1 thread)"

            if self.is_verify:
                table.add_row(
                    f.meta.original_filename,
                    str_size,
                    str(len(f.chunks)),
                    "✅" if f.meta.expected_checksum else "❌",
                    ranges,
                )
            else:
                table.add_row(
                    f.meta.original_filename, str_size, str(len(f.chunks)), ranges
                )

        self.log(table, progress=True)
        if not self.is_stream:
            self._check_storage_capacity(output_path=output_dir)

    @override
    def done(self, file_id: int, filename: str) -> None:
        task_id = self.tasks[file_id]
        self.rich.update(
            task_id,
            completed=self.rich.tasks[task_id].total,
            visible=False,
        )
        del self.tasks[file_id]
        self.active_files.discard(file_id)

        if self.rich.tasks[task_id].total is not None:
            self._files_completed += 1
            self._update_panel_title()
            self.log(f"Done: {filename}", status=LogStatus.SUCCESS, progress=True)

        elif self._files_size.get(file_id, 0):
            self._files_completed += 1
            self.log(f"Done: {filename}", status=LogStatus.SUCCESS, progress=True)

    def _make_panel(self) -> Panel | str:  # noqa: PLR0914
        if not self.rich.tasks and self._is_running:
            return ""

        elapsed = time.monotonic() - self._start_time
        avg_speed = self._download_bytes / elapsed if elapsed > 0 else 0
        speed_str = f"{format_size(avg_speed)}/s"

        mins, secs = divmod(int(elapsed), 60)
        hours = 0
        if mins >= 60:
            hours, mins = divmod(mins, 60)
        time_str = f"{hours:02d}:{mins:02d}:{secs:02d}"

        remain_time = (
            (self._total_bytes - self._download_bytes) / avg_speed
            if self._total_bytes and avg_speed
            else 0
        )

        r_mins, r_secs = divmod(int(remain_time), 60)
        r_hours = 0
        if r_mins >= 60:
            r_hours, r_mins = divmod(r_mins, 60)
        remain_time_str = f"{r_hours:02d}:{r_mins:02d}:{r_secs:02d}"

        size_str = (
            f"{format_size(self._download_bytes)}/{format_size(self._total_bytes)}"
        )
        if (not self._is_running or self._is_cancelled) and not self.is_dry_run:
            grid = Table.grid(expand=True)
            grid.add_column()
            grid.add_column(justify="center")
            content = Group("[green]All downloads completed successfully!\n", grid)
            grid.add_row(
                "[white]Total files:",
                f"[green3]{self._files_completed}/{self._total_files}[/]",
            )
            grid.add_row("[white]Total Data:", f"[bold cyan]{size_str}[/]")
            grid.add_row("[white]Average Speed:", f"[bold yellow]{speed_str}[/]")
            grid.add_row("[white]Total Time:", f"[bold magenta]{time_str}[/]")
            return Panel(
                grid if self._is_cancelled else content,
                title="[#2e8b57]Final Report",
                border_style="#2e8b57",
                expand=False,
            )

        if self.is_dry_run:
            size_str = f"{format_size(self._total_bytes)}"
            grid = Table.grid(expand=True)
            grid.add_column()
            grid.add_column(justify="center")

            grid.add_row("[white]Total files:", f"[green3]{self._total_files}[/]")
            grid.add_row("[white]Total Data:", f"[bold cyan]{size_str}[/]")
            if self.is_verify:
                grid.add_row(
                    "[white]Hash Found:",
                    f"[bold yellow]{self._has_hash}/{self._total_files}[/]",
                )
            grid.add_row(
                "[white]Ranges:",
                f"[bold magenta]{self._has_ranges}/{self._total_files}[/]",
            )
            return Panel(
                grid,
                title="[#2e8b57]Final Report",
                border_style="#2e8b57",
                expand=False,
            )

        dynamic_title_full = (
            f"\nAvg: [yellow]{speed_str}[/] | "
            f"Remaining Time: [green3]{remain_time_str}[/] | "
            f"Time: [magenta]{time_str}[/] | Download: [bold cyan]{size_str}[/]"
        )

        return Panel(
            self.rich,
            title=self.dynamic_title + dynamic_title_full,
            border_style="blue",
            padding=(1, 2),
        )

    @override
    def _ui_start(self) -> None:
        self.live.start()

    @override
    def _ui_stop(self) -> None:
        self.live.refresh()
        self.live.stop()
        if hasattr(self, "refresh") and not self.refresh.done():
            self.refresh.cancel()

    @override
    def bind_to_state_keeper(
        self, state_q: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    ) -> None:
        self.refresh = asyncio.create_task(self._ui_refresh_actor(state_q))
