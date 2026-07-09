# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import os
import random
import traceback
from abc import ABC, abstractmethod
from typing import assert_never, final, override

from curl_cffi import Response
from curl_cffi.requests import RequestsError

from hydrastream.actors.controller import (
    TrafficSignal,
)
from hydrastream.actors.dispatcher import FileCompleted
from hydrastream.domain.base_actor import BaseActor, BaseActorKwargs, ErrorVerdict
from hydrastream.domain.entities import Chunk
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import (
    DownloadFailedError,
    LogStatus,
    StreamError,
)
from hydrastream.interfaces import (
    NetworkBackend,
    StorageBackend,
)
from hydrastream.messages.base import (
    ActorFifoQueue,
    PoisonPill,
    ask,
)
from hydrastream.messages.io import StreamChunk, WriteChunk
from hydrastream.messages.state import ProgressDeltaCmd, RemoveFileCmd, StateKeeperMsg
from hydrastream.messages.traffic import (
    FlushCmd,
    GoToSleepPill,
    NetworkCongestionSignal,
    RegisterStreamCmd,
    RemoveStreamCmd,
    ThrottlerMsg,
    WakeUpPill,
)
from hydrastream.network import stream_chunk


class BaseWorkerKwargs(BaseActorKwargs):
    throttler_outbox: ActorFifoQueue[ThrottlerMsg | PoisonPill]
    controller_outbox: ActorFifoQueue[TrafficSignal | PoisonPill]
    state_outbox: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    sleep_signals_indox: ActorFifoQueue[GoToSleepPill]
    wait_in_sleep_inbox: ActorFifoQueue[WakeUpPill]

    net: NetworkBackend


@hydra_dataclass
class BaseDownloadWorker(BaseActor[Chunk], ABC):
    throttler_outbox: ActorFifoQueue[ThrottlerMsg | PoisonPill] | None = None
    controller_outbox: ActorFifoQueue[TrafficSignal | PoisonPill]
    state_outbox: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    sleep_signals_indox: ActorFifoQueue[GoToSleepPill]
    wait_in_sleep_inbox: ActorFifoQueue[WakeUpPill]

    net: NetworkBackend

    @final
    @override
    async def _handle_msg(self, msg: Chunk) -> None:

        match msg:
            case Chunk() as chunk:
                try:
                    _ = self.sleep_signals_indox.get_nowait()

                    await self.inbox.send_data(
                        sort_key=self._get_sort_key(msg.file.meta.id, msg.current_pos),
                        data=msg,
                    )

                    await self.wait_in_sleep_inbox.get()

                    return
                except asyncio.QueueEmpty:
                    pass
                file_obj = chunk.file
                if not file_obj or file_obj.is_failed:
                    return

                await self._process_chunk(chunk)

                if not chunk.is_finished:
                    await self.ui.log(
                        f"Truncated read for {chunk.file.actual_filename}. "
                        f"Requeuing remaining {chunk.remaining} bytes.",
                        status=LogStatus.WARNING,
                        throttle_key="truncated_read",
                        throttle_sec=2.0,
                    )
                    await self._requeue_chunk(chunk, delay_range=(0.1, 1.0))
                    return

                await self._file_done(chunk)

            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    @final
    @override
    async def _on_terminal_pill(self) -> None:
        await self._finally()

    @abstractmethod
    async def _finally(self) -> None:
        pass

    @final
    @override
    async def _on_error(
        self, e: Exception, msg: Chunk | PoisonPill | None = None
    ) -> ErrorVerdict:
        if not isinstance(msg, Chunk):
            return ErrorVerdict.RESUME

        if isinstance(e, RequestsError):
            await self._handle_requests_error(msg, e)
            await self.controller_outbox.send_data(NetworkCongestionSignal())
            return ErrorVerdict.RESUME

        if isinstance(e, TimeoutError):
            await self._requeue_chunk(msg)
            return ErrorVerdict.RESUME

        tb_str = traceback.format_exc()

        if self.is_debug:
            await self.ui.log(f"CRITICAL CRASH:\n{tb_str}", status=LogStatus.CRITICAL)
            return ErrorVerdict.ESCALATE

        await self.ui.log(
            f"Worker internal crash: {e!r}",
            status=LogStatus.CRITICAL,
            traceback=tb_str,
        )
        return ErrorVerdict.STOP

    @final
    async def _handle_requests_error(self, chunk: Chunk, e: RequestsError) -> None:
        """Разбирает сетевые ошибки и решает: убить файл или переповторить чанк."""
        response = self.net.get_error_response(e)
        if not isinstance(response, Response):
            await self._requeue_chunk(chunk)
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
            await self._requeue_chunk(chunk, delay_range=(0.5, 2.0))

    @abstractmethod
    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        pass

    @abstractmethod
    async def _requeue_chunk(
        self,
        chunk: Chunk,
        delay_range: tuple[float, float] = (1.0, 3.0),
    ) -> None:
        pass

    @abstractmethod
    async def _process_chunk(self, chunk: Chunk) -> None:
        pass

    @abstractmethod
    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        """Специфичный ключ сортировки для очередей"""
        pass

    @abstractmethod
    async def _file_done(
        self,
        chunk: Chunk,
    ) -> None:
        pass


@hydra_dataclass
class StreamDownloadWorker(BaseDownloadWorker):
    @override
    async def _finally(self) -> None:
        pass

    @override
    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        raise DownloadFailedError(
            url=chunk.file.meta.url,
            status_code=response.status_code,
            reason=response.reason,
        )

    @override
    async def _requeue_chunk(
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
        await self.inbox.send_data(
            sort_key=self._get_sort_key(chunk.file.meta.id, chunk.current_pos),
            data=chunk,
        )
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    @override
    async def _process_chunk(self, chunk: Chunk) -> None:
        if chunk.file.meta.supports_ranges:
            headers = {"Range": f"bytes={chunk.current_pos}-{chunk.end}"}
        else:
            headers = None
        buffer_list: list[bytes] = []
        current_buffer_size = 0

        async with stream_chunk(
            net=self.net,
            ui=self.ui,
            url=chunk.file.meta.url,
            headers=headers,
            max_retries=1,
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

                    if bytes_to_read <= 0:
                        break

            finally:
                if self.throttler_outbox is not None:
                    await self.throttler_outbox.send_data(RemoveStreamCmd(stream=r))

                if buffer_list:
                    await chunk.file.stream_q.send_data(
                        sort_key=(chunk.current_pos,),
                        data=StreamChunk(start=chunk.current_pos, data=buffer_list),
                    )
                    chunk.current_pos = chunk.current_pos + current_buffer_size

    @override
    async def _file_done(
        self,
        chunk: Chunk,
    ) -> None:
        pass

    @override
    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # СТРИМ: Сначала ID файла, потом позиция (Качаем файлы по очереди!)
        return (file_id, current_pos)


@hydra_dataclass
class DiskDownloadWorker(BaseDownloadWorker):
    disk_outbox: ActorFifoQueue[WriteChunk | FlushCmd | PoisonPill]
    file_limit_outbox: ActorFifoQueue[FileCompleted]

    fs: StorageBackend

    @override
    async def _finally(self) -> None:
        pass

    @override
    async def _handle_critical_requests_error(
        self, chunk: Chunk, response: Response
    ) -> None:
        chunk.file.is_failed = True
        self.fs.delete_file(chunk.file.actual_filename)

    @override
    async def _requeue_chunk(
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
        await self.inbox.send_data(
            sort_key=self._get_sort_key(chunk.file.meta.id, chunk.current_pos),
            data=chunk,
        )
        delay = random.uniform(*delay_range)
        await asyncio.sleep(delay)

    @override
    async def _process_chunk(self, chunk: Chunk) -> None:
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
        async with stream_chunk(
            net=self.net,
            ui=self.ui,
            url=chunk.file.meta.url,
            headers=headers,
            max_retries=1,
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

                        buffer_list = []
                        current_buffer_size = 0

                    if bytes_to_read <= 0:
                        break

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

    @override
    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # ДИСК: Сначала позиция, потом ID файла (Round-Robin параллельность!)
        return (current_pos, file_id)

    @override
    async def _file_done(
        self,
        chunk: Chunk,
    ) -> None:

        filename = chunk.file.actual_filename
        file_obj = chunk.file
        if chunk.file.meta.content_length:
            if chunk.file.verified or not chunk.file.is_complete:
                return
            chunk.file.verified = True

        await ask(
            inbox=self.disk_outbox,
            msg_factory=FlushCmd.create_request,
            timeout=60.0,
            sort_key=(-1,),
        )

        self.fs.verify_size(filename, file_obj.meta.content_length)
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
