import asyncio
import errno
import os
from typing import assert_never

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    PoisonPill,
)
from hydrastream.messages.io import WriteChunk
from hydrastream.messages.traffic import WriteCompleted


@hydra_dataclass
class DiskWriter(BaseActor[list[WriteChunk]]):
    ack_outbox: ActorFifoQueue[WriteCompleted | PoisonPill]

    fs: StorageBackend

    async def _handle_msg(self, msg: list[WriteChunk]) -> None:
        match msg:
            case list() as batch:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._write_all_sync, batch)
                await self.ack_outbox.send_data(WriteCompleted())
            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    async def _on_error(
        self, e: Exception, msg: list[WriteChunk] | PoisonPill | None = None
    ) -> None:
        _msg = self._handle_disk_error(e)
        await self.ui.log(
            f"Disk Write Failure: {_msg}",
            status=LogStatus.CRITICAL,
        )
        raise RuntimeError(_msg) from e

    def _write_all_sync(self, coalesced: list[WriteChunk]) -> None:
        for chunk in coalesced:
            self.fs.write_chunk_data(chunk.fd, chunk.data, chunk.length, chunk.offset)

    def _handle_disk_error(self, e: Exception) -> str:
        reason = "Unknown"
        if isinstance(e, OSError):
            sys_msg = os.strerror(e.errno) if e.errno else "Unknown"
            reasons = {
                errno.ENOSPC: f"STORAGE FULL: {sys_msg}. Action: Clean up disk space.",
                errno.EDQUOT: f"STORAGE FULL: {sys_msg}. Action: Clean up disk space.",
                errno.EIO: (
                    f"HARDWARE FAILURE: {sys_msg}. Action: Check drive SMART status."
                ),
                errno.EBADF: (
                    f"RUNTIME ERROR: {sys_msg}. Action: Check for file closing races."
                ),
            }
            if e.errno is not None:
                reason = reasons.get(e.errno, f"OS ERROR: {sys_msg} (code {e.errno})")

        return reason
