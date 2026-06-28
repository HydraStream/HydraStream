import asyncio

from hydrastream.domain.entities import Chunk
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import MonitorBackend
from hydrastream.messages.base import (
    ActorPriorityQueue,
    TerminalPill,
)


@hydra_dataclass
class MemoryThrottler:
    chunk_inbox: ActorPriorityQueue[Chunk | TerminalPill]
    chunk_outbox: ActorPriorityQueue[Chunk | TerminalPill]
    credit_inbox: asyncio.Queue[int]

    num_workers: int
    budget: int

    ui: MonitorBackend
    is_debug: bool

    async def run(self) -> None:

        while True:
            msg = await self.chunk_inbox.get()

            match msg:
                case Chunk() as pending_chunk:
                    if pending_chunk.size > self.budget:
                        credit = await self.credit_inbox.get()
                        self.budget += credit
                        continue

                    self.budget -= pending_chunk.size
                    await self.chunk_outbox.send_data(
                        sort_key=(msg.file.meta.id, msg.current_pos), data=msg
                    )

                case TerminalPill():
                    await self.chunk_outbox.send_poison_pills(self.num_workers)
                    break

                case _:
                    if self.is_debug:
                        raise RuntimeError(
                            f"Unknown message type in chunk_inbox: {type(msg)}"
                        )
                    await self.ui.log(
                        f"Received unknown message: {msg}",
                        status=LogStatus.ERROR,
                    )
