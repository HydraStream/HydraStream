import asyncio
import errno
import os
import sys
from typing import TYPE_CHECKING, assert_never, override

from hydrastream.domain.base_actor import BaseActor, ErrorVerdict
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import GracefulShutdownError, LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    PoisonPill,
)
from hydrastream.messages.io import WriteChunk
from hydrastream.messages.traffic import WriteCompleted

ERROR_DISK_FULL = 112
ERROR_HANDLE_DISK_FULL = 39
ERROR_IO_DEVICE = 1117
ERROR_INVALID_HANDLE = 6

if sys.platform == "win32":
    import pywintypes
elif TYPE_CHECKING:
    # Заглушка исключительно для анализатора в VS Code на Linux
    import pywintypes  # type: ignore
else:
    pywintypes = None


@hydra_dataclass
class DiskWriter(BaseActor[list[WriteChunk]]):
    ack_outbox: ActorFifoQueue[WriteCompleted | PoisonPill]

    fs: StorageBackend

    @override
    async def _handle_msg(self, msg: list[WriteChunk]) -> None:
        match msg:
            case list() as batch:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self._write_all_sync, batch)
                await self.ack_outbox.send_data(WriteCompleted())
            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    @override
    async def _on_error(
        self, e: Exception, msg: list[WriteChunk] | PoisonPill | None = None
    ) -> ErrorVerdict:
        if isinstance(e, GracefulShutdownError):
            return ErrorVerdict.STOP

        msg_ = self._handle_disk_error(e)
        self.ui.log(
            f"Disk Write Failure: {msg_}",
            status=LogStatus.CRITICAL,
        )
        raise RuntimeError(msg_) from e

    def _write_all_sync(self, coalesced: list[WriteChunk]) -> None:
        for chunk in coalesced:
            self.fs.write_chunk_data(chunk.fd, chunk.data, chunk.length, chunk.offset)

    @staticmethod
    def _handle_disk_error(e: Exception) -> str:
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
        elif pywintypes and isinstance(e, pywintypes.error):
            # e.args содержит: (win_error_code, function_name, error_message)
            win_code, func_name, win_msg = e.args

            if win_code in {ERROR_DISK_FULL, ERROR_HANDLE_DISK_FULL}:
                reason = f"STORAGE FULL: {win_msg}. Action: Clean up disk space."
            elif win_code == ERROR_IO_DEVICE:
                reason = (
                    f"HARDWARE FAILURE: {win_msg}. Action: Check drive SMART status."
                )
            elif win_code == ERROR_INVALID_HANDLE:
                reason = (
                    f"RUNTIME ERROR: {win_msg}. Action: Check for file closing races."
                )
            else:
                reason = f"WIN32 ERROR: {win_msg} (code {win_code} in {func_name})"

        return reason
