import asyncio

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import NetworkStream
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


type TrafficSignal = ScaleDownSignal | ScaleUpSignal | NetworkCongestionSignal


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


type ThrottlerMsg = (
    CheckpointReachedCmd
    | RegisterStreamCmd
    | RemoveStreamCmd
    | DiskBufferFullSignal
    | DiskBufferClearedSignal
)


@hydra_dataclass(frozen=True)
class FlushCmd:
    reply_to: asyncio.Future[bool]

    @classmethod
    def create_request(cls, future: asyncio.Future[bool]) -> "FlushCmd":
        return cls(reply_to=future)


type DiskMsg = WriteChunk | FlushCmd


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


@hydra_dataclass(frozen=True)
class GoToSleepPill:
    pass


@hydra_dataclass(frozen=True)
class WakeUpPill:
    pass
