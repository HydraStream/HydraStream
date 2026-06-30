import asyncio
from typing import assert_never

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.entities import Chunk
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.messages.base import (
    ActorPriorityQueue,
    PoisonPill,
)


@hydra_dataclass
class MemoryThrottler(BaseActor[Chunk]):
    chunk_outbox: ActorPriorityQueue[Chunk | PoisonPill]
    credit_inbox: asyncio.Queue[int]

    num_workers: int
    budget: int

    async def _handle_msg(self, msg: Chunk) -> None:
        match msg:
            case Chunk() as pending_chunk:
                if pending_chunk.size > self.budget:
                    credit = await self.credit_inbox.get()
                    self.budget += credit
                    return

                self.budget -= pending_chunk.size
                await self.chunk_outbox.send_data(
                    sort_key=(msg.file.meta.id, msg.current_pos), data=msg
                )
            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    async def _on_terminal_pill(self) -> None:
        await self.chunk_outbox.send_poison_pills(self.num_workers)
