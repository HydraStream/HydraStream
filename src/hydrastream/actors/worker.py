# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import os
import random
import traceback
from abc import ABC, abstractmethod
from typing import TypedDict

from curl_cffi import Response
from curl_cffi.requests import RequestsError

from hydrastream.actors.controller import (
    MaxLimitSignal,
    TerminalPill,
    TrafficSignal,
)
from hydrastream.actors.dispatcher import FileCompleted
from hydrastream.domain.entities import Chunk, File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import (
    DownloadFailedError,
    LogStatus,
    StreamError,
    WorkerScaleDown,
)
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
from hydrastream.messages.io import StreamChunk, WriteChunk
from hydrastream.messages.state import ProgressDeltaCmd, RemoveFileCmd, StateKeeperMsg
from hydrastream.messages.traffic import (
    FlushCmd,
    RegisterStreamCmd,
    RemoveStreamCmd,
    ThrottlerMsg,
)


class BaseWorkerKwargs(TypedDict):
    throttler_outbox: ActorFifoQueue[ThrottlerMsg]
    controller_outbox: ActorFifoQueue[TrafficSignal]
    state_outbox: ActorFifoQueue[StateKeeperMsg]

    all_complete: asyncio.Event

    ui: MonitorBackend
    net: NetworkBackend

    is_debug: bool


@hydra_dataclass
class BaseDownloadWorker(ABC):
    chunks_inbox: ActorPriorityQueue[Chunk | TerminalPill]
    throttler_outbox: ActorFifoQueue[ThrottlerMsg] | None = None
    controller_outbox: ActorFifoQueue[TrafficSignal]
    state_outbox: ActorFifoQueue[StateKeeperMsg]

    all_complete: asyncio.Event

    ui: MonitorBackend
    net: NetworkBackend

    is_debug: bool

    wakeup_event: asyncio.Event

    async def run(self) -> None:

        while True:
            await self.wakeup_event.wait()
            chunk = await self.get_chunk()

            if isinstance(chunk, Chunk):
                pass
            elif chunk:
                continue
            else:
                break

            try:
                if chunk.current_pos > chunk.end:
                    await self.process_chunk(chunk)

                if not chunk.is_finished:
                    await self.ui.log(
                        f"Truncated read for {chunk.file.actual_filename}. "
                        f"Requeuing remaining {chunk.remaining} bytes.",
                        status=LogStatus.WARNING,
                        throttle_key="truncated_read",
                        throttle_sec=2.0,
                    )
                    await self.requeue_chunk(chunk, delay_range=(0.1, 1.0))
                    continue
                await self.file_done(chunk)
            except Exception as e:
                await self.handle_worker_error(chunk, e)

    async def get_chunk(self) -> Chunk | bool:
        msg = await self.chunks_inbox.get()

        match msg:
            case Chunk() as chunk:
                file_obj = chunk.file
                if not file_obj or file_obj.is_failed:
                    return True

                return msg

            case StandardPill():
                await self.controller_outbox.send_data(MaxLimitSignal())
                return False

            case TerminalPill():
                await self._finally()
                return False

            case _:
                if self.is_debug:
                    raise RuntimeError(
                        f"Unknown message type in links_inbox: {type(msg)}"
                    )
                await self.ui.log(
                    f"Received unknown message: {msg}",
                    status=LogStatus.ERROR,
                )
                return True

    @abstractmethod
    async def _finally(self) -> None:
        pass

    async def handle_worker_error(self, chunk: Chunk, e: Exception) -> None:
        if isinstance(e, WorkerScaleDown):
            await self.chunks_inbox.send_data(
                sort_key=self._get_sort_key(chunk.file.meta.id, chunk.current_pos),
                data=chunk,
            )

        if isinstance(e, RequestsError):
            await self._handle_requests_error(chunk, e)
            self.dynamic_limit = max(self.dynamic_limit - 1, 1)
            await self.controller_outbox.send_poison_pills()

        if isinstance(e, TimeoutError):
            await self.requeue_chunk(chunk)
            return

        tb_str = traceback.format_exc()

        if self.is_debug:
            await self.ui.log(f"CRITICAL CRASH:\n{tb_str}", status=LogStatus.CRITICAL)

        else:
            await self.ui.log(
                f"Worker internal crash: {e!r}",
                status=LogStatus.CRITICAL,
                traceback=tb_str,
            )
        raise e

    async def _handle_requests_error(self, chunk: Chunk, e: RequestsError) -> None:
        """Разбирает сетевые ошибки и решает: убить файл или переповторить чанк."""
        response = self.net.get_error_response(e)
        if not isinstance(response, Response):
            await self.requeue_chunk(chunk)
            return

        status = response.status_code

        if status in {400, 401, 403, 404, 410, 416}:
            await self.ui.log(
                f"Chunk for {chunk.file.actual_filename} "
                f"failed permanently (HTTP {status}).",
                status=LogStatus.ERROR,
            )
            await self._handle_critical_requests_error(chunk, response)
        else:
            await self.requeue_chunk(chunk, delay_range=(0.5, 2.0))

    @abstractmethod
    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        pass

    @abstractmethod
    async def requeue_chunk(
        self,
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        pass

    @abstractmethod
    async def process_chunk(self, chunk: Chunk) -> None:
        pass

    @abstractmethod
    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        """Специфичный ключ сортировки для очередей"""
        pass

    @abstractmethod
    async def file_done(
        self,
        chunk: Chunk,
    ) -> None:
        pass


@hydra_dataclass
class StreamDownloadWorker(BaseDownloadWorker):
    stream_chunks_outbox: ActorPriorityQueue[StreamChunk | TerminalPill]
    file_discovery_outbox: ActorFifoQueue[File | TerminalPill]

    async def _finally(self) -> None:
        await self.file_discovery_outbox.send_poison_pills()

        self.all_complete.set()

    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        raise DownloadFailedError(
            url=chunk.file.meta.url,
            status_code=response.status_code,
            reason=response.reason,
        )

    async def requeue_chunk(
        self,
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        file_obj = chunk.file
        supports_ranges = file_obj.meta.supports_ranges

        if not supports_ranges:
            raise StreamError(
                url=chunk.file.meta.url, filename=chunk.file.actual_filename
            )
        await self.chunks_inbox.send_data(
            sort_key=self._get_sort_key(chunk.file.meta.id, chunk.current_pos),
            data=chunk,
        )
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def process_chunk(self, chunk: Chunk) -> None:
        if chunk.file.meta.supports_ranges:
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
        else:
            headers = None
        buffer_list: list[bytes] = []
        current_buffer_size = 0

        async with self.net.stream(
            chunk.file.meta.url,
            headers=headers,
        ) as r:
            try:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.send_data(RegisterStreamCmd(stream=r))
                bytes_to_read = chunk.end - chunk.current_pos + 1

                async for data in r.aiter_bytes(chunk_size=131072):
                    if len(data) > bytes_to_read:
                        data = data[:bytes_to_read]  # noqa: PLW2901

                    buffer_list.append(data)
                    current_buffer_size += len(data)

                    bytes_to_read -= len(data)
                    await self.state_outbox.send_data(
                        ProgressDeltaCmd(
                            file_id=chunk.file.meta.id, delta_bytes=len(data)
                        )
                    )

                    if not self.wakeup_event.is_set():
                        raise WorkerScaleDown

                    if bytes_to_read <= 0:
                        break

            finally:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.send_data(RemoveStreamCmd(stream=r))

                if buffer_list:
                    await self.stream_chunks_outbox.send_data(
                        sort_key=(chunk.current_pos,),
                        data=StreamChunk(start=chunk.current_pos, data=buffer_list),
                    )
                    chunk.current_pos = chunk.current_pos + current_buffer_size

    async def file_done(
        self,
        chunk: Chunk,
    ) -> None:
        pass

    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # СТРИМ: Сначала ID файла, потом позиция (Качаем файлы по очереди!)
        return (file_id, current_pos)


@hydra_dataclass
class DiskDownloadWorker(BaseDownloadWorker):
    disk_outbox: ActorFifoQueue[WriteChunk | FlushCmd | TerminalPill]
    file_limit_outbox: ActorFifoQueue[FileCompleted]

    fs: StorageBackend

    async def _finally(self) -> None:
        self.all_complete.set()
        await self.disk_outbox.send_poison_pills()

    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        chunk.file.is_failed = True
        self.fs.delete_file(chunk.file.actual_filename)

    async def requeue_chunk(
        self,
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        file_obj = chunk.file
        supports_ranges = file_obj.meta.supports_ranges

        if not supports_ranges:
            await self.ui.log(
                f"Connection dropped for {chunk.file.actual_filename}. "
                f"Server does not support resume. Restarting download from 0 bytes.",
                status=LogStatus.WARNING,
            )

            downloaded_so_far = chunk.current_pos - chunk.start
            if downloaded_so_far > 0:
                await self.state_outbox.send_data(
                    ProgressDeltaCmd(
                        file_id=chunk.file.meta.id, delta_bytes=-downloaded_so_far
                    )
                )

            chunk.current_pos = chunk.start

            fd = file_obj.fd
            if fd is not None:
                loop = asyncio.get_running_loop()
                # truncate(0) обрезает файл до 0 байт
                await loop.run_in_executor(None, os.ftruncate, fd, 0)

                # Если изначально размер был известен, снова выделяем место
                if file_obj.meta.content_length > 0:
                    await loop.run_in_executor(
                        None, os.ftruncate, fd, file_obj.meta.content_length
                    )
        await self.chunks_inbox.send_data(
            sort_key=self._get_sort_key(chunk.file.meta.id, chunk.current_pos),
            data=chunk,
        )
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    async def process_chunk(self, chunk: Chunk) -> None:  # noqa
        if chunk.file.meta.supports_ranges:
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
        else:
            headers = None

        buffer_list: list[bytes] = []
        current_buffer_size = 0

        fd = chunk.file.fd

        if fd is None:
            fd = self.fs.open_file(chunk.file.actual_filename)
        buffer_size = 1_048_576
        async with self.net.stream(
            chunk.file.meta.url,
            headers=headers,
        ) as r:
            try:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.send_data(RegisterStreamCmd(stream=r))
                bytes_to_read = chunk.end - chunk.current_pos + 1

                async for data in r.aiter_bytes(chunk_size=131072):
                    if len(data) > bytes_to_read:
                        data = data[:bytes_to_read]  # noqa: PLW2901

                    buffer_list.append(data)
                    current_buffer_size += len(data)

                    bytes_to_read -= len(data)
                    await self.state_outbox.send_data(
                        ProgressDeltaCmd(
                            file_id=chunk.file.meta.id, delta_bytes=len(data)
                        )
                    )

                    if current_buffer_size >= buffer_size:
                        await self.disk_outbox.send_data(
                            WriteChunk(
                                fd=fd,
                                offset=chunk.current_pos,
                                length=current_buffer_size,
                                data=buffer_list,
                            )
                        )
                        chunk.current_pos += current_buffer_size

                        buffer_list.clear()
                        current_buffer_size = 0

                    if bytes_to_read <= 0:
                        break

                    if not self.wakeup_event.is_set():
                        raise WorkerScaleDown

            finally:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.send_data(RemoveStreamCmd(stream=r))
                if buffer_list:
                    await self.disk_outbox.send_data(
                        WriteChunk(
                            fd=fd,
                            offset=chunk.current_pos,
                            length=current_buffer_size,
                            data=buffer_list,
                        )
                    )
                    chunk.current_pos += current_buffer_size

    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # ДИСК: Сначала позиция, потом ID файла (Round-Robin параллельность!)
        return (current_pos, file_id)

    async def file_done(
        self,
        chunk: Chunk,
    ) -> None:

        filename = chunk.file.actual_filename
        file_obj = chunk.file
        if chunk.file.meta.content_length:
            if chunk.file.verified or not chunk.file.is_complete:
                return
            chunk.file.verified = True
            if not self.fs.verify_size(filename, file_obj.meta.content_length):
                return
        if file_obj.meta.expected_checksum:
            await self.ui.log(
                f"Verifying Hash checksum for {chunk.file.actual_filename}...",
                status=LogStatus.INFO,
            )
            await self.fs.verify_file_hash(
                file_obj.actual_filename,
                file_obj.meta.expected_checksum.value,
                file_obj.meta.expected_checksum.algorithm,
            )
            await self.ui.log(
                f"Integrity confirmed: {chunk.file.actual_filename}",
                status=LogStatus.SUCCESS,
            )
        self.fs.close_file(fd_or_conn=file_obj.fd)
        self.fs.delete_state(filename)
        await self.ui.done(file_obj.meta.id, filename)
        await self.state_outbox.send_data(RemoveFileCmd(file_id=chunk.file.meta.id))
        await self.file_limit_outbox.send_data(FileCompleted())
