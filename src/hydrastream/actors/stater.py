import asyncio
from collections import defaultdict
from dataclasses import field
from typing import assert_never

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import StorageBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
)
from hydrastream.messages.state import (
    GetSnapshotCmd,
    GetUIDeltasCmd,
    ProgressDeltaCmd,
    RegisterFileCmd,
    RemoveFileCmd,
    StateKeeperMsg,
    UpdateBytesToCheckCmd,
)
from hydrastream.messages.traffic import CheckpointReachedCmd, ThrottlerMsg


@hydra_dataclass
class StateKeeperActor(BaseActor[StateKeeperMsg]):
    throttler_output: ActorFifoQueue[ThrottlerMsg]

    _files: dict[int, File] = field(default_factory=dict[int, File])
    _ui_deltas: defaultdict[int, int] = field(default_factory=lambda: defaultdict(int))

    _global_bytes: int = 0
    _prev_global_bytes: int = 0

    bytes_to_check: int

    analyzer_checkpoint_event: asyncio.Event

    fs: StorageBackend

    is_stream: bool

    async def _handle_msg(self, msg: StateKeeperMsg) -> None:
        match msg:
            case RegisterFileCmd(file_id=fid, file_obj=fobj):
                self._files[fid] = fobj

            case RemoveFileCmd(file_id=fid):
                self._files.pop(fid, None)

            case GetSnapshotCmd(reply_to=queue):
                await queue.put(self._files.copy())

            case ProgressDeltaCmd(file_id=fid, delta_bytes=delta):
                self._ui_deltas[fid] += delta
                self._global_bytes += delta

                if self._global_bytes - self._prev_global_bytes >= self.bytes_to_check:
                    self._prev_global_bytes += self.bytes_to_check

                    self.analyzer_checkpoint_event.set()
                    await self.throttler_output.send_data(
                        CheckpointReachedCmd(new_btc=self.bytes_to_check)
                    )

            case UpdateBytesToCheckCmd(bytes_to_check=btc):
                self.bytes_to_check = btc

            case GetUIDeltasCmd(reply_to=queue):
                await queue.put(dict(self._ui_deltas))
                self._ui_deltas.clear()

            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    async def _on_stop(self) -> None:
        if not self.is_stream:
            for file_obj in self._files.values():
                if file_obj.chunks and not file_obj.is_complete:
                    self.fs.save_state(file_obj)

                if file_obj.fd is not None:
                    self.fs.close_file(file_obj.fd)
                    file_obj.fd = None
