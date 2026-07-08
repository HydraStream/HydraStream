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
    PoisonPill,
)
from hydrastream.messages.state import (
    GetReadyFileCmd,
    GetSnapshotCmd,
    GetStatusCmd,
    GetUIDeltasCmd,
    JobTrace,
    LinkAddedCmd,
    ProgressDeltaCmd,
    RegisterFileCmd,
    RemoveFileCmd,
    StateKeeperMsg,
    TaskState,
    UpdateBytesToCheckCmd,
    UpdateStatusDownloading,
)
from hydrastream.messages.traffic import CheckpointReachedCmd, ThrottlerMsg


@hydra_dataclass
class StateKeeperActor(BaseActor[StateKeeperMsg]):
    is_stream: bool

    throttler_output: ActorFifoQueue[ThrottlerMsg | PoisonPill]

    bytes_to_check: int

    fs: StorageBackend

    _traces: dict[int, JobTrace] = field(default_factory=dict[int, JobTrace])
    _ui_deltas: defaultdict[int, int] = field(default_factory=lambda: defaultdict(int))
    _waiting_clients: defaultdict[int, list[asyncio.Future[File]]] = field(
        default_factory=lambda: defaultdict(list)
    )

    _global_bytes: int = 0
    _prev_global_bytes: int = 0

    async def _handle_msg(self, msg: StateKeeperMsg) -> None:
        match msg:
            case LinkAddedCmd(link_data=data):
                trace = JobTrace(file_obj=data)
                trace.transition_to(TaskState.QUEUED)
                self._traces[data.id] = trace

            case RegisterFileCmd(file_obj=fobj):
                fid = fobj.meta.id
                self._traces[fid].file_obj = fobj
                self._traces[fid].transition_to(TaskState.RESOLVING)
                # Будим ВСЕХ, кто ждал этот файл!
                if fid in self._waiting_clients:
                    for fut in self._waiting_clients.pop(fid):
                        if not fut.cancelled():
                            fut.set_result(fobj)

            case GetReadyFileCmd(file_id=fid, reply_to=reply_future):
                trace = self._traces[fid]

                if isinstance(trace.file_obj, File):
                    if not reply_future.cancelled():
                        reply_future.set_result(trace.file_obj)
                else:
                    self._waiting_clients[fid].append(reply_future)

            case RemoveFileCmd(file_id=fid):
                self._traces.pop(fid, None)
                # Отменяем ВСЕХ, кто ждал несуществующий файл
                for fut in self._waiting_clients.pop(fid, []):
                    if not fut.done():
                        fut.cancel()

            case GetSnapshotCmd(reply_to=reply_future):
                if not reply_future.cancelled():
                    snaphot: dict[int, File] = {}
                    for k, v in self._traces.items():
                        if isinstance(v.file_obj, File):
                            snaphot[k] = v.file_obj

                    reply_future.set_result(snaphot)

            case ProgressDeltaCmd(file_id=fid, delta_bytes=delta):
                self._ui_deltas[fid] += delta
                self._global_bytes += delta

                if self._global_bytes - self._prev_global_bytes >= self.bytes_to_check:
                    self._prev_global_bytes += self.bytes_to_check

                    await self.throttler_output.send_data(
                        CheckpointReachedCmd(new_btc=self.bytes_to_check)
                    )

            case UpdateBytesToCheckCmd(bytes_to_check=btc):
                self.bytes_to_check = btc

            case UpdateStatusDownloading(file_id=id):
                self._traces[id].transition_to(TaskState.DOWNLOADING)

            case GetUIDeltasCmd(reply_to=reply_future):
                if not reply_future.cancelled():
                    reply_future.set_result(dict(self._ui_deltas))

                self._ui_deltas.clear()

            case GetStatusCmd(file_id=fid, reply_to=fut):
                if (trace := self._traces.get(fid)) and not fut.cancelled():
                    fut.set_result(trace.create_task_status)

            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    async def _on_stop(self) -> None:
        if not self.is_stream:
            for trace in self._traces.values():
                if isinstance(trace.file_obj, File):
                    if trace.file_obj.chunks and not trace.file_obj.is_complete:
                        self.fs.save_state(trace.file_obj)

                    if trace.file_obj.fd is not None:
                        self.fs.close_file(trace.file_obj.fd)
                        trace.file_obj.fd = None
        else:
            for trace in self._traces.values():
                if (
                    isinstance(trace.file_obj, File)
                    and trace.file_obj._stream_queue is not None  # pyright: ignore[reportPrivateUsage]
                ):
                    trace.file_obj.stream_q.send_poison_pills_nowait()
