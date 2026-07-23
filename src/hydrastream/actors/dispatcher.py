# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from abc import ABC, abstractmethod
from typing import assert_never, final, override

from hydrastream.domain.base_actor import BaseActor, BaseActorKwargs
from hydrastream.domain.entities import Chunk, File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import GracefulShutdownError, LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    ActorPriorityQueue,
    PoisonPill,
    StandardPill,
    TerminalPill,
)
from hydrastream.messages.state import StateKeeperMsg, UpdateStatusDownloading
from hydrastream.messages.traffic import FileCompleted


class BaseDispatcherKwargs(BaseActorKwargs):
    limit: int

    inbox: ActorPriorityQueue[File | PoisonPill]

    chunks_outbox: ActorPriorityQueue[Chunk | PoisonPill]
    file_limit_inbox: ActorFifoQueue[FileCompleted | PoisonPill]
    state_outbox: ActorFifoQueue[StateKeeperMsg | PoisonPill]


@hydra_dataclass
class BaseFileDispatcher(BaseActor[File], ABC):
    limit: int

    chunks_outbox: ActorPriorityQueue[Chunk | PoisonPill]
    file_limit_inbox: ActorFifoQueue[FileCompleted | PoisonPill]
    state_outbox: ActorFifoQueue[StateKeeperMsg | PoisonPill]

    _current_files: int = 0

    @final
    @override
    async def _handle_msg(self, msg: File) -> None:
        match msg:
            case File() as file_obj:
                if self._current_files >= self.limit:
                    msg_ = await self.file_limit_inbox.get()
                    if isinstance(msg_, StandardPill | TerminalPill):
                        raise GracefulShutdownError
                    self._current_files -= 1

                self._current_files += 1

                await self._prepare_file(file_obj)

                file_obj.create_chunks()
                await self.state_outbox.send_data(
                    UpdateStatusDownloading(file_id=file_obj.meta.id)
                )

                for c in file_obj.chunks:
                    if c.current_pos <= c.end:
                        await self.chunks_outbox.send_data(
                            sort_key=self._get_sort_key(
                                file_obj.meta.id, c.current_pos
                            ),
                            data=c,
                        )
            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    @abstractmethod
    async def _prepare_file(self, file_obj: File) -> None:
        """Специфичная логика подготовки файла"""
        pass

    @abstractmethod
    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        """Специфичный ключ сортировки для очередей"""
        pass


@hydra_dataclass
class StreamFileDispatcher(BaseFileDispatcher):
    @override
    async def _prepare_file(self, file_obj: File) -> None:
        pass

    @override
    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # СТРИМ: Сначала ID файла, потом позиция (Качаем файлы по очереди!)
        return (file_id, current_pos)


@hydra_dataclass
class DiskFileDispatcher(BaseFileDispatcher):
    fs: StorageBackend

    @override
    async def _prepare_file(self, file_obj: File) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._prepare_file_on_disk, file_obj)

    def _prepare_file_on_disk(self, file_obj: File) -> None:
        if not file_obj.actual_filename:
            fd, new_filename = self.fs.allocate_space(
                filename=file_obj.meta.original_filename,
                size=file_obj.meta.content_length,
            )
            file_obj.fd = fd

            if new_filename is not None:
                file_obj.actual_filename = new_filename
                self.ui.update_filename(file_obj.meta.id, new_filename)
                self.ui.log(
                    f"{file_obj.meta.original_filename} already exists. "
                    f"Saving as {file_obj.actual_filename}.",
                    status=LogStatus.WARNING,
                )
            else:
                file_obj.actual_filename = file_obj.meta.original_filename
        else:
            file_obj.fd = self.fs.open_file(filename=file_obj.actual_filename)

    @override
    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # ДИСК: Сначала позиция, потом ID файла (Round-Robin параллельность!)
        return (current_pos, file_id)
