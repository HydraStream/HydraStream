import hashlib
from collections.abc import AsyncGenerator

from hydrastream.actors.dispatcher import FileCompleted
from hydrastream.actors.stater import RemoveFileCmd, StateKeeperMsg
from hydrastream.domain.entities import Checksum, File
from hydrastream.exceptions import FileSizeMismatchError, HashMismatchError, LogStatus
from hydrastream.interfaces import Hasher, MonitorBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    PoisonPill,
    StandardPill,
    TerminalPill,
)
from hydrastream.messages.io import StreamChunk


async def file_streamer(  # noqa
    file_obj: File,
    credit_outbox: ActorFifoQueue[int],
    reg_events_outbox: ActorFifoQueue[StateKeeperMsg | PoisonPill],
    file_limit_outbox: ActorFifoQueue[FileCompleted],
    ui: MonitorBackend,
    is_debug: bool,
) -> AsyncGenerator[bytes, None]:

    total_size = file_obj.meta.content_length
    checksum = file_obj.meta.expected_checksum
    hasher: Hasher | None = hashlib.new(checksum.algorithm) if checksum else None

    buffer: dict[int, list[bytes]] = {}
    expected_offset = 0

    await ui.log(f"Streaming: {file_obj.actual_filename}", status=LogStatus.INFO)

    try:
        while expected_offset < total_size:
            msg = await file_obj.stream_q.get()

            match msg:
                case StreamChunk(start=offset, data=chunk_data):
                    if offset == expected_offset:
                        for data in chunk_data:
                            if hasher:
                                hasher.update(data)

                            yield data
                            expected_offset += len(data)
                            await credit_outbox.send_data(len(data))

                            while expected_offset in buffer:
                                next_data = buffer.pop(expected_offset)

                                for n_data in next_data:
                                    if hasher:
                                        hasher.update(n_data)

                                    yield n_data
                                    expected_offset += len(n_data)
                                    await credit_outbox.send_data(len(n_data))
                    else:
                        buffer[offset] = chunk_data

                case StandardPill() | TerminalPill():
                    break

                case _:
                    if is_debug:
                        raise RuntimeError(
                            f"Unknown message type in stream_chunk_inbox: {type(msg)}"
                        )
                    await ui.log(
                        f"Received unknown message: {msg}",
                        status=LogStatus.ERROR,
                    )

        else:
            if hasher and checksum:
                try:
                    verify_stream(
                        hasher,
                        file_obj.actual_filename,
                        checksum,
                        expected_offset,
                        total_size,
                    )
                    await ui.log(
                        "Hash Verified", status=LogStatus.SUCCESS, progress=True
                    )
                except Exception as e:
                    await ui.log(str(e), status=LogStatus.ERROR)
                    raise

            await ui.done(file_obj.meta.id, file_obj.actual_filename)

    finally:
        buffer.clear()

        await reg_events_outbox.send_data(RemoveFileCmd(file_id=file_obj.meta.id))
        await file_limit_outbox.send_data(FileCompleted())


def verify_stream(
    hasher: Hasher,
    filename: str,
    expected_checksum: Checksum,
    next_offset: int,
    total_size: int,
) -> None:
    if next_offset != total_size:
        raise FileSizeMismatchError(
            filename=filename,
            expected=total_size,
            actual=next_offset,
            message_tpl="Incomplete stream data! Yielded {actual} of {expected} bytes.",
        )

    calculated = hasher.hexdigest()
    if calculated != expected_checksum.value:
        raise HashMismatchError(
            filename=filename,
            algorithm=expected_checksum.algorithm,
            expected=expected_checksum.value,
            actual=calculated,
        )
