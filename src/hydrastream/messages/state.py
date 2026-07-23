# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import time
from dataclasses import field
from enum import StrEnum

from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.messages.io import LinkData


class TaskState(StrEnum):
    QUEUED = "QUEUED"  # Только что добавили ссылку
    RESOLVING = "RESOLVING"  # Резолвер пошел за метаданными
    DOWNLOADING = "DOWNLOADING"  # Воркеры качают чанки
    FLUSHING = "FLUSHING"  # Ждем записи последних байт на диск
    COMPLETED = "COMPLETED"  # Всё готово!
    FAILED = "FAILED"  # Ошибка (404, 403 и т.д.)


@hydra_dataclass(frozen=True)
class TaskStatus:
    id: int
    state: TaskState
    url: str
    filename: str | None = None
    total_bytes: int | None = None
    downloaded_bytes: int = 0
    timeline: list[tuple[TaskState, float]]
    error: str | None = None

    @property
    def progress(self) -> float:
        if not self.total_bytes:
            return 0.0
        return (self.downloaded_bytes / self.total_bytes) * 100

    def get_lag(self, from_state: TaskState, to_state: TaskState) -> float | None:
        t1, t2 = None, None
        for state, t in self.timeline:
            if state == from_state:
                t1 = t
            if state == to_state:
                t2 = t

        if t1 and t2:
            return t2 - t1
        return None


@hydra_dataclass
class JobTrace:
    file_obj: File | LinkData
    downloaded_bytes: int = 0
    timeline: list[tuple[TaskState, float]] = field(
        default_factory=list[tuple[TaskState, float]]
    )
    error_msg: str | None = None

    def transition_to(self, new_state: TaskState) -> None:
        """Записываем переход в новое состояние с текущим временем"""
        self.timeline.append((new_state, time.monotonic()))

    @property
    def create_task_status(self) -> TaskStatus:
        return TaskStatus(
            id=self.file_obj.meta.id
            if isinstance(self.file_obj, File)
            else self.file_obj.id,
            url=self.file_obj.meta.url
            if isinstance(self.file_obj, File)
            else self.file_obj.url,
            state=self.timeline[-1][0],
            filename=self.file_obj.actual_filename
            if isinstance(self.file_obj, File)
            else None,
            total_bytes=self.file_obj.meta.content_length
            if isinstance(self.file_obj, File)
            else None,
            downloaded_bytes=self.downloaded_bytes,
            timeline=list(self.timeline),
            error=self.error_msg,
        )


@hydra_dataclass(frozen=True)
class LinkAddedCmd:
    link_data: LinkData


@hydra_dataclass(frozen=True)
class RegisterFileCmd:
    file_obj: File


@hydra_dataclass(frozen=True)
class GetReadyFileCmd:
    file_id: int
    reply_to: asyncio.Future[File]


@hydra_dataclass(frozen=True)
class FileFinishedCmd:
    file_id: int
    error: str | None = None


@hydra_dataclass(frozen=True)
class GetSnapshotCmd:
    reply_to: asyncio.Future[dict[int, File]]

    @classmethod
    def create_request(
        cls, future: asyncio.Future[dict[int, File]]
    ) -> "GetSnapshotCmd":
        return cls(reply_to=future)


@hydra_dataclass(frozen=True)
class ProgressDeltaCmd:
    file_id: int
    delta_bytes: int


@hydra_dataclass(frozen=True)
class UpdateBytesToCheckCmd:
    bytes_to_check: int


@hydra_dataclass(frozen=True)
class GetUIDeltasCmd:
    reply_to: asyncio.Future[dict[int, int]]

    @classmethod
    def create_request(cls, future: asyncio.Future[dict[int, int]]) -> "GetUIDeltasCmd":
        return cls(reply_to=future)


@hydra_dataclass(frozen=True)
class GetStatusCmd:
    file_id: int
    reply_to: asyncio.Future[TaskStatus]

    @classmethod
    def create_request(
        cls, file_id: int, future: asyncio.Future[TaskStatus]
    ) -> "GetStatusCmd":
        return cls(file_id=file_id, reply_to=future)


@hydra_dataclass(frozen=True)
class UpdateStatusDownloading:
    file_id: int


@hydra_dataclass(frozen=True)
class AwaitFileCmd:
    """Запрос от юзера: 'Дай знать, когда этот файл скачается на диск'."""

    file_id: int
    reply_to: asyncio.Future[TaskStatus]


type StateKeeperMsg = (
    LinkAddedCmd
    | RegisterFileCmd
    | GetStatusCmd
    | UpdateStatusDownloading
    | GetReadyFileCmd
    | FileFinishedCmd
    | FileFinishedCmd
    | GetSnapshotCmd
    | ProgressDeltaCmd
    | UpdateBytesToCheckCmd
    | GetUIDeltasCmd
    | AwaitFileCmd
)
