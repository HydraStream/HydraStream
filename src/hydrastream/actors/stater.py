import asyncio
from collections import defaultdict
from dataclasses import field

from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import MonitorBackend, StorageBackend
from hydrastream.messages.base import ActorFifoQueue, TerminalPill
from hydrastream.messages.state import (
    GetSnapshotCmd,
    GetUIDeltasCmd,
    ProgressDeltaCmd,
    RegisterFileCmd,
    RemoveFileCmd,
    StateKeeperMsg,
)


@hydra_dataclass
class StateKeeperActor:
    stater_inbox: ActorFifoQueue[StateKeeperMsg]

    _files: dict[int, File] = field(default_factory=dict[int, File])
    _ui_deltas: defaultdict[int, int] = field(default_factory=lambda: defaultdict(int))

    _global_bytes: int = 0
    _prev_global_bytes: int = 0

    bytes_to_check: int

    analyzer_checkpoint_event: asyncio.Event
    throttler_checkpoint_event: asyncio.Event

    fs: StorageBackend
    ui: MonitorBackend

    is_stream: bool
    is_debug: bool

    async def run(self) -> None:
        try:
            while True:
                cmd = await self.stater_inbox.get()
                match cmd:
                    case RegisterFileCmd(file_id=fid, file_obj=fobj):
                        self._files[fid] = fobj

                    case RemoveFileCmd(file_id=fid):
                        self._files.pop(fid, None)

                    case GetSnapshotCmd(reply_to=queue):
                        await queue.put(self._files.copy())

                    case ProgressDeltaCmd(file_id=fid, delta_bytes=delta):
                        self._ui_deltas[fid] += delta
                        self._global_bytes += delta

                        if (
                            self._global_bytes - self._prev_global_bytes
                            >= self.bytes_to_check
                        ):
                            self._prev_global_bytes += self.bytes_to_check

                            self.analyzer_checkpoint_event.set()
                            self.throttler_checkpoint_event.set()

                    case GetUIDeltasCmd(reply_to=queue):
                        await queue.put(dict(self._ui_deltas))
                        self._ui_deltas.clear()

                    case TerminalPill():
                        break

                    case _:
                        if self.is_debug:
                            raise RuntimeError(
                                f"Unknown message type in stater_inbox: {type(cmd)}"
                            )
                        await self.ui.log(
                            f"Received unknown message: {cmd}",
                            status=LogStatus.ERROR,
                        )
        finally:
            if not self.is_stream:
                for file_obj in self._files.values():
                    if file_obj.chunks and not file_obj.is_complete:
                        self.fs.save_state(file_obj)

                    if file_obj.fd is not None:
                        self.fs.close_file(file_obj.fd)
