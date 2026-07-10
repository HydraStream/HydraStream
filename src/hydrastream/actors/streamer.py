import hashlib
from collections.abc import AsyncGenerator

from hydrastream.actors.dispatcher import FileCompleted
from hydrastream.actors.stater import FileFinishedCmd, StateKeeperMsg
from hydrastream.domain.entities import Checksum, File
from hydrastream.exceptions import FileSizeMismatchError, HashMismatchError, LogStatus
from hydrastream.interfaces import Hasher, MonitorBackend
from hydrastream.messages.base import (
    ActorFifoQueue,
    PoisonPill,
    StandardPill,
    TerminalPill,
)


async def file_streamer(  # noqa
    file_obj: File,
    credit_outbox: ActorFifoQueue[int | PoisonPill],
    reg_events_outbox: ActorFifoQueue[StateKeeperMsg | PoisonPill],
    file_limit_outbox: ActorFifoQueue[FileCompleted | PoisonPill],
    ui: MonitorBackend,
    is_debug: bool,
) -> AsyncGenerator[bytes, None]:

    total_size = file_obj.meta.content_length
    checksum = file_obj.meta.expected_checksum
    hasher: Hasher | None = hashlib.new(checksum.algorithm) if checksum else None

    buffer: dict[int, list[bytes]] = {}
    expected_offset = 0

    ui.log(f"Streaming: {file_obj.actual_filename}", status=LogStatus.INFO)

    async def process_and_yield_chunk(
        chunk_data: list[bytes],
    ) -> AsyncGenerator[bytes, None]:

        nonlocal expected_offset

        for data in chunk_data:
            if hasher:
                hasher.update(data)

            yield data
            expected_offset += len(data)
            await credit_outbox.send_data(len(data))

    try:
        while expected_offset < total_size:
            msg = await file_obj.stream_q.get()

            # 1. Избавляемся от глубокого match/case с помощью Guard Clauses
            if isinstance(msg, (StandardPill, TerminalPill)):
                break

            # 2. Обрабатываем целевой StreamChunk (код стал плоским!)
            offset, chunk_data = msg.start, msg.data

            if offset != expected_offset:
                buffer[offset] = chunk_data
                continue

            # Обрабатываем текущий чанк, который пришел вовремя
            async for data in process_and_yield_chunk(chunk_data):
                yield data

            # Разбираем накопившийся буфер (код больше не дублируется)
            while expected_offset in buffer:
                next_data = buffer.pop(expected_offset)
                async for data in process_and_yield_chunk(next_data):
                    yield data

        else:
            if hasher and checksum:
                try:
                    _verify_stream(
                        hasher,
                        file_obj.actual_filename,
                        checksum,
                        expected_offset,
                        total_size,
                    )
                    ui.log("Hash Verified", status=LogStatus.SUCCESS, progress=True)
                except Exception as e:
                    ui.log(str(e), status=LogStatus.ERROR)
                    raise

            ui.done(file_obj.meta.id, file_obj.actual_filename)

    finally:
        buffer.clear()

        await reg_events_outbox.send_data(FileFinishedCmd(file_id=file_obj.meta.id))
        await file_limit_outbox.send_data(FileCompleted())


def _verify_stream(
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
