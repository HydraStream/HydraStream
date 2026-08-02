# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from collections import defaultdict
from dataclasses import field
from typing import assert_never, override

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import StorageBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    PoisonPill,
)
from hydrastream.messages.state import (
    AwaitFileCmd,
    FileFinishedCmd,
    GetReadyFileCmd,
    GetSnapshotCmd,
    GetStatusCmd,
    GetUIDeltasCmd,
    JobTrace,
    LinkAddedCmd,
    ProgressDeltaCmd,
    RegisterFileCmd,
    StateKeeperMsg,
    TaskState,
    TaskStatus,
    UpdateBytesToCheckCmd,
    UpdateStatusDownloading,
)
from hydrastream.messages.traffic import CheckpointReachedCmd, ThrottlerMsg


@hydra_dataclass
class StateKeeperActor(BaseActor[StateKeeperMsg]):
    is_stream: bool
    is_dry_run: bool

    throttler_output: ActorFifoQueue[ThrottlerMsg | PoisonPill]

    bytes_to_check: int

    fs: StorageBackend

    _traces: dict[int, JobTrace] = field(default_factory=dict[int, JobTrace])
    _ui_deltas: defaultdict[int, int] = field(default_factory=lambda: defaultdict(int))
    _waiting_stream: defaultdict[int, list[asyncio.Future[File]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _result_waiters: defaultdict[int, list[asyncio.Future[TaskStatus]]] = field(
        default_factory=lambda: defaultdict(list)
    )
    _finished_results: dict[int, TaskStatus] = field(
        default_factory=dict[int, TaskStatus]
    )
    _waited_dru_run: asyncio.Future[dict[int, File]] = field(init=False)
    size_history_result: int = 50

    _global_bytes: int = 0
    _prev_global_bytes: int = 0

    @override
    async def _handle_msg(self, msg: StateKeeperMsg) -> None:  # noqa: C901, PLR0912, PLR0915
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
                if fid in self._waiting_stream:
                    for fut in self._waiting_stream.pop(fid):
                        if not fut.cancelled():
                            fut.set_result(fobj)

                if self.is_dry_run:
                    trace = self._traces[fid]
                    result = trace.create_task_status
                    self._finished_results[fid] = result

                    for fut in self._result_waiters.pop(fid, []):
                        if not fut.cancelled():
                            fut.set_result(result)

            case UpdateStatusDownloading(file_id=id):
                self._traces[id].transition_to(TaskState.DOWNLOADING)

            case GetReadyFileCmd(file_id=fid, reply_to=reply_future):
                trace = self._traces[fid]

                if isinstance(trace.file_obj, File):
                    if not reply_future.cancelled():
                        reply_future.set_result(trace.file_obj)
                else:
                    self._waiting_stream[fid].append(reply_future)

            case FileFinishedCmd(file_id=fid, error=err):
                trace = self._traces.pop(fid, None)
                if not trace:
                    return

                if err:
                    trace.transition_to(TaskState.FAILED)
                    trace.error_msg = err
                else:
                    trace.transition_to(TaskState.COMPLETED)

                result = trace.create_task_status
                self._finished_results[fid] = result

                if len(self._finished_results) > self.size_history_result:
                    oldest_key = next(iter(self._finished_results))
                    self._finished_results.pop(oldest_key)
                # Отменяем ВСЕХ, кто ждал несуществующий файл
                for fut in self._waiting_stream.pop(fid, []):
                    if not fut.done():
                        fut.cancel()

                for fut in self._result_waiters.pop(fid, []):
                    if not fut.cancelled():
                        fut.set_result(result)

            case AwaitFileCmd(file_id=fid, reply_to=fut):
                if fid in self._finished_results:
                    # Файл УЖЕ скачался! Отдаем результат мгновенно.
                    fut.set_result(self._finished_results[fid])
                elif fid in self._traces:
                    # Файл еще в работе, добавляем юзера в "ждуны"
                    self._result_waiters[fid].append(fut)
                else:
                    fut.set_exception(ValueError(f"Unknown file_id: {fid}"))

            case GetSnapshotCmd(reply_to=reply_future):
                if self.is_dry_run:
                    self._waited_dru_run = reply_future
                    return
                if not reply_future.cancelled():
                    snaphot: dict[int, File] = {}
                    for k, v in self._traces.items():
                        if isinstance(v.file_obj, File):
                            snaphot[k] = v.file_obj

                    reply_future.set_result(snaphot)

            case ProgressDeltaCmd(file_id=fid, delta_bytes=delta):
                self._traces[fid].downloaded_bytes += delta
                self._ui_deltas[fid] += delta
                self._global_bytes += delta

                if self._global_bytes - self._prev_global_bytes >= self.bytes_to_check:
                    self._prev_global_bytes += self.bytes_to_check

                    await self.throttler_output.send_data(
                        CheckpointReachedCmd(new_btc=self.bytes_to_check)
                    )

            case UpdateBytesToCheckCmd(bytes_to_check=btc):
                self.bytes_to_check = btc

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

    @override
    async def _on_stop(self) -> None:  # noqa: C901, PLR0912
        if not self.is_stream:
            for trace in self._traces.values():
                if isinstance(trace.file_obj, File):
                    if trace.file_obj.fd is not None:
                        self.fs.close_file(trace.file_obj.fd)
                        trace.file_obj.fd = None

                    if trace.file_obj.chunks and not trace.file_obj.is_complete:
                        self.fs.save_state(trace.file_obj)
        else:
            for trace in self._traces.values():
                if isinstance(trace.file_obj, File):
                    trace.file_obj.stream_q.send_poison_pills_nowait()

        for fut_list in self._waiting_stream.values():
            for fut in fut_list:
                if not fut.done():
                    fut.cancel()
        for fut_list in self._result_waiters.values():
            for fut in fut_list:
                if not fut.done():
                    fut.cancel()

        if self.is_dry_run and not self._waited_dru_run.cancelled():
            snaphot: dict[int, File] = {}
            for k, v in self._traces.items():
                if isinstance(v.file_obj, File):
                    snaphot[k] = v.file_obj

            self._waited_dru_run.set_result(snaphot)
