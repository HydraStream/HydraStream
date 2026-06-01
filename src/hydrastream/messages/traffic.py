from typing import TypeAlias

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import NetworkStream
from hydrastream.messages.base import StopMsg


@hydra_dataclass(frozen=True)
class NetworkCongestionSignal:
    pass


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
    NetworkCongestionSignal | MaxLimitSignal | ScaleUpSignal | ScaleDownSignal
)


@hydra_dataclass
class FlushCmd:
    pass


@hydra_dataclass(frozen=True)
class FileCompleted:
    pass


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
    RegisterStreamCmd
    | RemoveStreamCmd
    | DiskBufferFullSignal
    | DiskBufferClearedSignal
    | CheckpointReachedCmd
    | StopMsg
)


@hydra_dataclass(frozen=True)
class WriteCompleted:
    pass
