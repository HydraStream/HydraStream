from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any, Protocol

from typing_extensions import Buffer, runtime_checkable

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
    async def log(self, message: str, status: str, **kwargs: Any) -> None:
        """Записать сообщение в лог/на экран"""
        ...

    def add_file(self, filename: str, total_size: int | None = None) -> None:
        """Зарегистрировать новый файл в UI"""
        ...

    def update_progress(self, file_id: int, advance_bytes: int) -> None:
        """Сдвинуть прогресс-бар"""
        ...

    async def done(self, filename: str) -> None:
        """Отметить файл как завершенный"""
        ...

    async def stop(self) -> None:
        """Остановить отрисовку и закрыть ресурсы"""
        ...


@runtime_checkable
class NetworkStream(Protocol):
    """Абстракция над потоком (response от curl_cffi или httpx)"""

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

    async def close(self) -> None:
        """Закрыть все соединения"""
        ...


@runtime_checkable
class HashProvider(Protocol):
    async def resolve(
        self, ctx: NetworkBackend, url: str, filename: str
    ) -> Checksum | None: ...


@runtime_checkable
class Hasher(Protocol):
    def update(self, data: Buffer, /) -> None: ...
    def hexdigest(self) -> str: ...
