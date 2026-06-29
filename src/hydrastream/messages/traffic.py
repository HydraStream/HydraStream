import asyncio
from typing import TypeAlias

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import NetworkStream
from hydrastream.messages.base import PoisonPill
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


@hydra_dataclass(frozen=True)
class NetworkCongestionSignal:
    pass


TrafficSignal: TypeAlias = (
    ScaleDownSignal | ScaleUpSignal | NetworkCongestionSignal | PoisonPill
)


@hydra_dataclass(frozen=True)
class CheckpointReachedCmd:
    new_btc: int


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
    | PoisonPill
)


@hydra_dataclass(frozen=True)
class FlushCmd:
    reply_to: asyncio.Event


DiskMsg: TypeAlias = WriteChunk | FlushCmd | PoisonPill


@hydra_dataclass(frozen=True)
class WriteCompleted:
    pass


@hydra_dataclass(frozen=True)
class FileCompleted:
    pass


@hydra_dataclass(frozen=True)
class AnalyzerCheckpointEvent:
    pass


@hydra_dataclass(frozen=True)
class ThrottlerCheckpointEvent:
    pass
