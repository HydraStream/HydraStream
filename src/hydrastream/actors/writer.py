import asyncio
import errno
import os

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
from hydrastream.messages.traffic import WriteCompleted


@hydra_dataclass
class DiskWriter:
    writer_inbox: ActorFifoQueue[list[WriteChunk] | PoisonPill]
    ack_outbox: ActorFifoQueue[WriteCompleted | PoisonPill]

    fs: StorageBackend
    ui: MonitorBackend

    is_debug: bool

    async def run(self) -> None:
        loop = asyncio.get_running_loop()

        while True:
            msg = await self.writer_inbox.get()

            match msg:
                case list() as batch:
                    try:
                        await loop.run_in_executor(None, self._write_all_sync, batch)

                        await self.ack_outbox.send_data(WriteCompleted())

                    except Exception as e:
                        msg = self._handle_disk_error(e)
                        await self.ui.log(
                            f"Disk Write Failure: {msg}",
                            status=LogStatus.CRITICAL,
                        )
                        raise RuntimeError(msg) from e

                case StandardPill() | TerminalPill():
                    break

                case _:
                    if self.is_debug:
                        raise RuntimeError(
                            f"Unknown message type in writer_inbox: {type(msg)}"
                        )
                    await self.ui.log(
                        f"Received unknown message: {msg}",
                        status=LogStatus.ERROR,
                    )

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
