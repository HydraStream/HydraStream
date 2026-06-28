from typing import TypeAlias

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import NetworkStream
from hydrastream.messages.base import TerminalPill
from hydrastream.messages.io import WriteChunk


@hydra_dataclass(frozen=True)
class MaxLimitSignal:
    pass


@hydra_dataclass(frozen=True)
class ScaleUpSignal:
    pass


@hydra_dataclass(frozen=True)
class ScaleDownSignal:
    pass


TrafficSignal: TypeAlias = (
    ScaleDownSignal | ScaleUpSignal | MaxLimitSignal | TerminalPill
)


@hydra_dataclass(frozen=True)
class CheckpointReachedCmd:
    pass


@hydra_dataclass(frozen=True)
class RegisterStreamCmd:
    stream: NetworkStream


@hydra_dataclass(frozen=True)
class RemoveStreamCmd:
    stream: NetworkStream


@hydra_dataclass(frozen=True)
class DiskBufferFullSignal:
    pass


@hydra_dataclass(frozen=True)
class DiskBufferClearedSignal:
    pass


ThrottlerMsg: TypeAlias = (
    CheckpointReachedCmd
    | RegisterStreamCmd
    | RemoveStreamCmd
    | DiskBufferFullSignal
    | DiskBufferClearedSignal
    | TerminalPill
)


@hydra_dataclass(frozen=True)
class FlushCmd:
    pass


DiskMsg: TypeAlias = WriteChunk | FlushCmd | TerminalPill


@hydra_dataclass(frozen=True)
class WriteCompleted:
    pass


@hydra_dataclass(frozen=True)
class FileCompleted:
    pass
