# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from dataclasses import field
from typing import assert_never, override

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import StorageBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    PoisonPill,
)
from hydrastream.messages.io import WriteChunk
from hydrastream.messages.traffic import (
    DiskBufferClearedSignal,
    DiskBufferFullSignal,
    FlushCmd,
    ThrottlerMsg,
    WriteCompleted,
)


@hydra_dataclass
class DiskAggregator(BaseActor[WriteChunk | FlushCmd]):
    throttler_outbox: ActorFifoQueue[ThrottlerMsg | PoisonPill]
    ack_inbox: ActorFifoQueue[WriteCompleted | PoisonPill]
    writer_outbox: ActorFifoQueue[list[WriteChunk] | PoisonPill]
    MAX_BUFFER: int

    fs: StorageBackend

    _current_buffer: list[WriteChunk] = field(default_factory=list[WriteChunk])
    _current_size: int = 0
    _is_writing_now: bool = False

    @override
    async def _handle_msg(self, msg: WriteChunk | FlushCmd) -> None:
        match msg:
            case WriteChunk():
                self._current_buffer.append(msg)
                self._current_size += msg.length

                if self._current_size >= self.MAX_BUFFER:
                    await self._persist_buffer()

            case FlushCmd(reply_to=reply_future):
                await self._persist_buffer()

                if self._is_writing_now:
                    try:
                        async with asyncio.timeout(60.0):
                            await self.ack_inbox.get()
                    except TimeoutError as e:
                        raise RuntimeError("DiskWriter hung during Flush!") from e

                    self._is_writing_now = False

                    await self.throttler_outbox.send_data(DiskBufferClearedSignal())

                if not reply_future.cancelled():
                    reply_future.set_result(True)

            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    @override
    async def _on_terminal_pill(self) -> None:
        await self._persist_buffer()

    @override
    async def _on_stop(self) -> None:
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

    @staticmethod
    async def _coalesce(batch_bytes: list[WriteChunk]) -> list[WriteChunk]:
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
