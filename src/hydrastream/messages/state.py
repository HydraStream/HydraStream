import asyncio
from typing import TypeAlias

from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass

# 1. Базовый маркерный класс для конкретного актора


@hydra_dataclass(frozen=True)
class RegisterFileCmd:
    file_id: int
    file_obj: File


@hydra_dataclass(frozen=True)
class GetReadyFileCmd:
    file_id: int
    reply_to: asyncio.Future[File]


@hydra_dataclass(frozen=True)
class RemoveFileCmd:
    file_id: int


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


StateKeeperMsg: TypeAlias = (
    RegisterFileCmd
    | GetReadyFileCmd
    | RemoveFileCmd
    | GetSnapshotCmd
    | ProgressDeltaCmd
    | UpdateBytesToCheckCmd
    | GetUIDeltasCmd
)
