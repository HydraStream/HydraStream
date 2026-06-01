import asyncio
from typing import TypeAlias

from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.messages.base import StopMsg


@hydra_dataclass(frozen=True)
class RegisterFileCmd:
    file_id: int
    file_obj: File


@hydra_dataclass(frozen=True)
class RemoveFileCmd:
    file_id: int


@hydra_dataclass(frozen=True)
class GetSnapshotCmd:
    reply_to: asyncio.Queue[dict[int, File]]


@hydra_dataclass(frozen=True)
class ProgressDeltaCmd:
    file_id: int
    delta_bytes: int


@hydra_dataclass(frozen=True)
class GetUIDeltasCmd:
    reply_to: asyncio.Queue[dict[int, int]]


StateKeeperCmd: TypeAlias = (
    RegisterFileCmd
    | RemoveFileCmd
    | GetSnapshotCmd
    | ProgressDeltaCmd
    | GetUIDeltasCmd
    | StopMsg
)
