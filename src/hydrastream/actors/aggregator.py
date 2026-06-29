import asyncio
from dataclasses import field

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import MonitorBackend, StorageBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    PoisonPill,
    StandardPill,
    TerminalPill,
)
from hydrastream.messages.io import WriteChunk
from hydrastream.messages.traffic import (
    DiskBufferClearedSignal,
    DiskBufferFullSignal,
    FlushCmd,
    ThrottlerMsg,
    WriteCompleted,
)
from hydrastream.utils import verify_memory_chunk


@hydra_dataclass
class DiskAggregator:
    disk_inbox: ActorFifoQueue[WriteChunk | FlushCmd | PoisonPill]
    throttler_outbox: ActorFifoQueue[ThrottlerMsg]
    ack_inbox: ActorFifoQueue[WriteCompleted | PoisonPill]
    writer_outbox: ActorFifoQueue[list[WriteChunk] | PoisonPill]
    MAX_BUFFER: int

    ui: MonitorBackend
    fs: StorageBackend

    is_debug: bool

    _current_buffer: list[WriteChunk] = field(default_factory=list[WriteChunk])
    _current_size: int = 0
    _is_writing_now: bool = False

    async def run(self) -> None:
        try:
            while True:
                msg = await self.disk_inbox.get()

                match msg:
                    case WriteChunk() as w:
                        verify_memory_chunk(data_bytes=w.data, offset=w.offset)
                        self._current_buffer.append(msg)
                        self._current_size += msg.length

                        if self._current_size >= self.MAX_BUFFER:
                            await self._persist_buffer()

                    case FlushCmd() as cmd:
                        await self._persist_buffer()

                        if self._is_writing_now:
                            try:
                                async with asyncio.timeout(60.0):
                                    await self.ack_inbox.get()
                            except TimeoutError as e:
                                raise RuntimeError(
                                    "DiskWriter hung during Flush!"
                                ) from e

                            self._is_writing_now = False

                            await self.throttler_outbox.send_data(
                                DiskBufferClearedSignal()
                            )

                        cmd.reply_to.set()

                    case StandardPill() | TerminalPill():
                        await self._persist_buffer()

                        await self.writer_outbox.send_poison_pills()
                        break

                    case _:
                        if self.is_debug:
                            raise RuntimeError(
                                f"Unknown message type in disk_inbox: {type(msg)}"
                            )
                        await self.ui.log(
                            f"Received unknown message: {msg}",
                            status=LogStatus.ERROR,
                        )
        finally:
            if self._current_buffer:
                coalesced = await self._coalesce(self._current_buffer)
                for chunk in coalesced:
                    self.fs.write_chunk_data(
                        chunk.fd, chunk.data, chunk.length, chunk.offset
                    )

    async def _persist_buffer(self) -> None:

        if self._is_writing_now:
            await self.throttler_outbox.send_data(DiskBufferFullSignal())

            try:
                async with asyncio.timeout(60.0):
                    await self.ack_inbox.get()

            except TimeoutError as e:
                raise RuntimeError(
                    "DiskWriter stopped responding! Hardware failure?"
                ) from e

            self._is_writing_now = False
            await self.throttler_outbox.send_data(DiskBufferClearedSignal())

        if self._current_buffer:
            batch = await self._coalesce(self._current_buffer)
            await self.writer_outbox.send_data(batch)

            self._is_writing_now = True
            self._current_buffer.clear()
            self._current_size = 0

    async def _coalesce(self, batch_bytes: list[WriteChunk]) -> list[WriteChunk]:

        batch_bytes.sort()

        coalesced: list[WriteChunk] = []
        curr = batch_bytes[0]

        acc_data_chunks: list[bytes] = curr.data
        acc_len = curr.length

        for next_chunk in batch_bytes[1:]:
            if (
                curr.fd == next_chunk.fd
                and (curr.offset + acc_len) == next_chunk.offset
            ):
                acc_data_chunks.extend(next_chunk.data)
                acc_len += next_chunk.length
            else:
                coalesced.append(
                    WriteChunk(
                        fd=curr.fd,
                        offset=curr.offset,
                        length=acc_len,
                        data=acc_data_chunks,
                    )
                )
                curr = next_chunk
                acc_data_chunks = curr.data
                acc_len = curr.length

        coalesced.append(
            WriteChunk(
                fd=curr.fd,
                offset=curr.offset,
                length=acc_len,
                data=acc_data_chunks,
            )
        )
        return coalesced
