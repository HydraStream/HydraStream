from typing import assert_never, override

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.entities import Chunk
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import GracefulShutdownError
from hydrastream.messages.base import (
    ActorFifoQueue,
    ActorPriorityQueue,
    PoisonPill,
    StandardPill,
    TerminalPill,
)


@hydra_dataclass
class MemoryThrottler(BaseActor[Chunk]):
    chunk_outbox: ActorPriorityQueue[Chunk | PoisonPill]
    credit_inbox: ActorFifoQueue[int | PoisonPill]

    num_workers: int
    budget: int

    @override
    async def _handle_msg(self, msg: Chunk) -> None:
        match msg:
            case Chunk() as pending_chunk:
                while pending_chunk.size > self.budget:
                    credit = await self.credit_inbox.get()
                    if isinstance(credit, (StandardPill, TerminalPill)):
                        raise GracefulShutdownError
                    self.budget += credit

                self.budget -= pending_chunk.size
                await self.chunk_outbox.send_data(
                    sort_key=(msg.file.meta.id, msg.current_pos), data=msg
                )
            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)
