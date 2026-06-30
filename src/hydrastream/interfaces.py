from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from typing_extensions import Buffer, runtime_checkable

from hydrastream.exceptions import HydraError, LogStatus
from hydrastream.messages.base import ActorFifoQueue
from hydrastream.messages.state import StateKeeperMsg

if TYPE_CHECKING:
    from hydrastream.domain.entities import Checksum, File, TypeHash


@runtime_checkable
class StorageBackend(Protocol):
    def allocate_space(self, filename: str, size: int) -> str | None: ...

    def open_file(self, filename: str) -> Any: ...

    def write_chunk_data(
        self, fd_or_conn: Any, data_bytes: list[bytes], len_data: int, offset: int
    ) -> None: ...

    def close_file(self, fd_or_conn: Any) -> None: ...

    def force_close_all(self) -> None: ...

    def delete_file(self, filename: str) -> None: ...

    def save_state(self, file_obj: File) -> None: ...

    def load_state(self, filename: str) -> tuple[File | None, int]: ...

    def delete_state(self, filename: str) -> None: ...

    def verify_size(self, filename: str, expected_size: int) -> bool: ...

    async def verify_file_hash(
        self, filename: str, expected_checksum: str, algorithm: TypeHash
    ) -> None: ...

    def get_unique_path(self, file_path: Any) -> Any: ...

    def get_state_path(self, filename: str) -> Any: ...


@runtime_checkable
class MonitorBackend(Protocol):
    async def log(
        self,
        message: str,
        *,
        status: LogStatus | str,
        progress: bool = False,
        throttle_key: str | None = None,
        throttle_sec: float = 10.0,
        **kwargs: object,
    ) -> None:
        """Записать сообщение в лог/на экран"""
        ...

    def add_file(
        self, file_id: int, filename: str, total_size: int | None = None
    ) -> None:
        """Зарегистрировать новый файл в UI"""
        ...

    def update_progress(self, file_id: int, advance_bytes: int) -> None:
        """Сдвинуть прогресс-бар"""
        ...

    def update_filename(self, file_id: int, new_filename: str) -> None: ...

    async def dry_run(self, files: dict[int, File], output_dir: str | Path) -> None: ...

    async def done(self, file_id: int, filename: str) -> None:
        """Отметить файл как завершенный"""
        ...

    def get_dup_fd(self) -> int | None: ...

    async def start(self) -> None: ...

    def bind_to_state_keeper(self, state_q: ActorFifoQueue[StateKeeperMsg]) -> None:
        """Позволяет привязать очередь состояния ПОСЛЕ создания UI"""
        ...

    async def stop(self) -> None:
        """Остановить отрисовку и закрыть ресурсы"""
        ...

    async def report(self, error: HydraError, **log_extra: Any) -> None: ...


@runtime_checkable
class NetworkStream(Protocol):
    """Абстракция над потоком (response от curl_cffi или httpx)"""

    @property
    def response(self) -> Any: ...

    @property
    def headers(self) -> dict[str, str]: ...

    def aiter_bytes(self, chunk_size: int) -> AsyncGenerator[bytes, None]: ...

    def set_speed_limit(self, limit: int) -> None: ...


@runtime_checkable
class NetworkBackend(Protocol):
    async def request(self, method: str, url: str, **kwargs: Any) -> Any:
        """Выполнить разовый запрос (например, HEAD)"""
        ...

    def stream(
        self, url: str, headers: dict[str, str] | None = None
    ) -> AbstractAsyncContextManager[NetworkStream]:
        """Открыть стрим для скачивания чанка"""
        ...

    def get_error_response(self, e: Any) -> Any | None:
        return e.response  # type: ignore

    async def close(self) -> None:
        """Закрыть все соединения"""
        ...


@runtime_checkable
class HashProvider(Protocol):
    async def resolve(
        self, net: NetworkBackend, ui: MonitorBackend, url: str, filename: str
    ) -> Checksum | None: ...


@runtime_checkable
class Hasher(Protocol):
    def update(self, data: Buffer, /) -> None: ...
    def hexdigest(self) -> str: ...
