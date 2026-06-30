# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from abc import ABC, abstractmethod
from typing import assert_never

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.entities import Chunk, File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    ActorPriorityQueue,
    PoisonPill,
)
from hydrastream.messages.traffic import FileCompleted


@hydra_dataclass
class BaseFileDispatcher(BaseActor[File], ABC):
    limit: int
    current_files: int = 0
    num_workers: int

    chunks_outbox: ActorPriorityQueue[Chunk | PoisonPill]
    file_limit_inbox: ActorFifoQueue[FileCompleted]

    async def _handle_msg(self, msg: File) -> None:
        match msg:
            case File() as file_obj:
                if self.current_files >= self.limit:
                    await self.file_limit_inbox.get()
                    self.current_files -= 1

                self.current_files += 1

                await self._prepare_file(file_obj)

                if file_obj.meta.original_filename != file_obj.actual_filename:
                    await self.ui.log(
                        f"{file_obj.meta.original_filename} already exists. "
                        f"Saving as {file_obj.actual_filename}.",
                        status=LogStatus.WARNING,
                    )

                file_obj.create_chunks()

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

    async def _on_terminal_pill(self) -> None:
        await self.chunks_outbox.send_poison_pills(self.num_workers)

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
    file_discovery: ActorFifoQueue[File | PoisonPill]

    async def _prepare_file(self, file_obj: File) -> None:
        # Для стрима просто закидываем файл в трубу (имя не меняется)
        await self.file_discovery.send_data(file_obj)

    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # СТРИМ: Сначала ID файла, потом позиция (Качаем файлы по очереди!)
        return (file_id, current_pos)


@hydra_dataclass
class DiskFileDispatcher(BaseFileDispatcher):
    fs: StorageBackend

    async def _prepare_file(self, file_obj: File) -> None:

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._prepare_file_on_disk, file_obj)

    def _prepare_file_on_disk(self, file_obj: File) -> None:
        new_filename = self.fs.allocate_space(
            filename=file_obj.meta.original_filename, size=file_obj.meta.content_length
        )
        if new_filename:
            file_obj.actual_filename = new_filename
            self.ui.update_filename(file_obj.meta.id, new_filename)
        else:
            file_obj.actual_filename = file_obj.meta.original_filename

        file_obj.fd = self.fs.open_file(filename=file_obj.actual_filename)

    def _get_sort_key(self, file_id: int, current_pos: int) -> tuple[int, ...]:
        # ДИСК: Сначала позиция, потом ID файла (Round-Robin параллельность!)
        return (current_pos, file_id)
