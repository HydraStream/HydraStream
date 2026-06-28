# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import random
from abc import ABC, abstractmethod
from typing import TypedDict

from curl_cffi import Headers, Response
from curl_cffi.requests import RequestsError

from hydrastream.domain.entities import Checksum, File, FileMeta, TypeHash
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import (
    MonitorBackend,
    NetworkBackend,
    StorageBackend,
)
from hydrastream.messages.base import (
    ActorFifoQueue,
    ActorPriorityQueue,
    StandardPill,
    TerminalPill,
)
from hydrastream.messages.io import LinkData
from hydrastream.messages.state import ProgressDeltaCmd, RegisterFileCmd, StateKeeperMsg
from hydrastream.providers import ProviderRouter
from hydrastream.utils import extract_filename, redact_url


class BaseResolverKwargs(TypedDict):
    threads: int
    MIN_CHUNK: int

    links_inbox: ActorPriorityQueue[LinkData | TerminalPill]

    files_outbox: ActorPriorityQueue[File | TerminalPill]
    state_outbox: ActorFifoQueue[StateKeeperMsg]

    all_complete: asyncio.Event

    is_dry_run: bool
    is_verify: bool
    is_debug: bool

    ui: MonitorBackend
    net: NetworkBackend
    provider: ProviderRouter


@hydra_dataclass
class BaseMetadataResolver(ABC):
    threads: int
    MIN_CHUNK: int

    links_inbox: ActorPriorityQueue[LinkData | TerminalPill]
    files_outbox: ActorPriorityQueue[File | TerminalPill]
    state_outbox: ActorFifoQueue[StateKeeperMsg]

    all_complete: asyncio.Event

    is_dry_run: bool
    is_verify: bool
    is_debug: bool

    ui: MonitorBackend
    net: NetworkBackend
    provider: ProviderRouter

    async def run(self) -> None:
        """Это ШАБЛОННЫЙ МЕТОД. Наследники не переопределяют его!"""
        checksum = None
        while True:
            msg = await self.links_inbox.get()

            match msg:
                case LinkData() as data:
                    try:
                        meta = await self._fetch_metadata(data.url)
                        filename, total_size, supports_ranges = meta

                        if self.is_verify and not data.checksum:
                            checksum = await self._resolve_hash(
                                data.id, data.url, filename, data.checksum
                            )

                        file_obj = await self._prepare_file_object(
                            data=data,
                            filename=filename,
                            total_size=total_size,
                            supports_ranges=supports_ranges,
                            checksum=checksum,
                        )

                        await self._register_file(file_obj)

                    except Exception as e:
                        await self._handle_error(e, msg)

                case StandardPill():
                    break

                case TerminalPill():
                    await self.files_outbox.send_poison_pills()
                    if self.is_dry_run:
                        self.all_complete.set()
                    break

                case _:
                    if self.is_debug:
                        raise RuntimeError(
                            f"Unknown message type in links_inbox: {type(msg)}"
                        )
                    await self.ui.log(
                        f"Received unknown message: {msg}",
                        status=LogStatus.ERROR,
                    )

    async def _register_file(self, file_obj: File) -> None:
        """Общая логика регистрации, внутри которой есть ХУК для наследников."""
        filename = file_obj.meta.original_filename

        await self.state_outbox.send_data(
            data=RegisterFileCmd(file_id=file_obj.meta.id, file_obj=file_obj)
        )
        self.ui.add_file(file_obj.meta.id, filename, file_obj.meta.content_length)

        # ВЫЗЫВАЕМ ХУК (Стрим проигнорирует, Диск - обновит UI)
        await self._on_file_registered(file_obj)

        await self.files_outbox.send_data(sort_key=(file_obj.meta.id,), data=file_obj)

    @abstractmethod
    async def _prepare_file_object(
        self,
        data: LinkData,
        filename: str,
        total_size: int,
        supports_ranges: bool,
        checksum: Checksum | None,
    ) -> File:
        pass

    @abstractmethod
    async def _on_file_registered(self, file_obj: File) -> None:
        pass

    async def _handle_error(
        self,
        e: Exception,
        data: LinkData,
    ) -> None:
        """Возвращает True, если нужно пропустить итерацию (continue)."""

        if isinstance(e, RequestsError):
            response = self.net.get_error_response

            if isinstance(response, Response):
                status = response.status_code
                # Постоянные ошибки: логируем и забываем
                if status in {400, 401, 403, 404, 410, 416}:
                    await self.ui.log(
                        f"Link {redact_url(data.url)} failed permanently "
                        f"(HTTP {status}).",
                        status=LogStatus.ERROR,
                    )

                # Временные ошибки сервера (5xx, 429) — в очередь
                await self._requeue_chunk(data, delay_range=(0.5, 2.0))
            else:
                # Сетевая ошибка без ответа
                await self._requeue_chunk(data)

        if isinstance(e, TimeoutError):
            await self._requeue_chunk(data)

        # Если мы здесь, значит ошибка критическая (Exception)
        await self.ui.log(
            f"Critical Task Creator crash: {e!r}", status=LogStatus.CRITICAL
        )
        raise e

    async def _requeue_chunk(
        self,
        data: LinkData,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        await self.links_inbox.send_data(data)
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def _fetch_metadata(self, url: str) -> tuple[str, int, bool]:
        # 1. Пробуем HEAD
        response = await self.net.request("HEAD", url=url)
        # 2. Если HEAD не дал инфы, используем GET, но ОБЯЗАТЕЛЬНО через stream
        if response is None or int(response.headers.get("content-length", 0)) == 0:
            # Контекстный менеджер 'async with' сам закроет соединение в конце
            async with self.net.stream(url) as connect:
                if response := connect.response:
                    headers = connect.response.headers
                    return self._parse_headers(url, headers)

        return self._parse_headers(url, response.headers)

    def _parse_headers(self, url: str, headers: Headers) -> tuple[str, int, bool]:
        total_size = int(headers.get("content-length", 0))

        accept_ranges = headers.get("accept-ranges", "").lower()
        supports_ranges = (accept_ranges == "bytes") and (total_size > 0)
        filename = extract_filename(url, headers)
        return filename, total_size, supports_ranges

    async def _resolve_hash(
        self,
        id: int,
        url: str,
        filename: str,
        checksum_tuple: tuple[TypeHash, str] | None,
    ) -> Checksum | None:
        if checksum_tuple:
            return Checksum(algorithm=checksum_tuple[0], value=checksum_tuple[1])

        self.ui.add_file(id, filename)

        checksum = await self.provider.resolve_hash(self.net, url, filename)
        await self.ui.done(id, filename)

        if checksum is None:
            await self.ui.log(
                f"Missing MD5 hash for file: {filename}",
                status=LogStatus.WARNING,
            )

        return checksum


@hydra_dataclass
class StreamMetadataResolver(BaseMetadataResolver):
    # Специфичная зависимость только для стрима!
    STREAM_CHUNK_SIZE: int

    async def _prepare_file_object(
        self,
        data: LinkData,
        filename: str,
        total_size: int,
        supports_ranges: bool,
        checksum: Checksum | None,
    ) -> File:
        chunk_size = (
            max(total_size // self.threads, self.MIN_CHUNK) if total_size > 0 else 0
        )
        chunk_size = min(chunk_size, self.STREAM_CHUNK_SIZE)

        return File(
            meta=FileMeta(
                id=data.id,
                original_filename=filename,
                url=data.url,
                content_length=total_size,
                supports_ranges=supports_ranges,
                expected_checksum=checksum,
            ),
            chunk_size=chunk_size,
        )

    async def _on_file_registered(self, file_obj: File) -> None:
        # В режиме стрима нам не нужно пересчитывать скачанные байты для UI!
        pass


@hydra_dataclass
class DiskMetadataResolver(BaseMetadataResolver):
    fs: StorageBackend

    async def _prepare_file_object(
        self,
        data: LinkData,
        filename: str,
        total_size: int,
        supports_ranges: bool,
        checksum: Checksum | None,
    ) -> File:
        chunk_size = (
            max(total_size // self.threads, self.MIN_CHUNK) if total_size > 0 else 0
        )

        file_obj = None
        if supports_ranges:
            file_obj, num_states = self.fs.load_state(filename=filename)
            if num_states > 1:
                await self.ui.log(
                    f"Multiple state files found for {filename}!",
                    status=LogStatus.WARNING,
                )

        if file_obj:
            return file_obj

        return File(
            meta=FileMeta(
                id=data.id,
                original_filename=filename,
                url=data.url,
                content_length=total_size,
                supports_ranges=supports_ranges,
                expected_checksum=checksum,
            ),
            chunk_size=chunk_size,
        )

    async def _on_file_registered(self, file_obj: File) -> None:
        filename = file_obj.meta.original_filename
        await self.state_outbox.send_data(
            RegisterFileCmd(file_id=file_obj.meta.id, file_obj=file_obj)
        )
        chunks = file_obj.chunks or []

        self.ui.add_file(file_obj.meta.id, filename, file_obj.meta.content_length)
        downloaded = sum(c.uploaded for c in chunks)
        if downloaded - len(chunks) > 0:
            await self.state_outbox.send_data(
                ProgressDeltaCmd(file_id=file_obj.meta.id, delta_bytes=downloaded)
            )
        await self.files_outbox.send_data(sort_key=(file_obj.meta.id,), data=file_obj)
