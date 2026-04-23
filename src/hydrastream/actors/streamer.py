import hashlib
import heapq
from collections.abc import AsyncGenerator

from hydrastream.exceptions import FileSizeMismatchError, HashMismatchError, LogStatus
from hydrastream.interfaces import Hasher
from hydrastream.models import Checksum, HydraContext
from hydrastream.monitor import done, log


async def streamer(ctx: HydraContext, file_id: int) -> AsyncGenerator[bytes]:
    file_obj = ctx.files[file_id]
    total_size = file_obj.meta.content_length

    checksum = file_obj.meta.expected_checksum
    hasher: Hasher | None = hashlib.new(checksum.algorithm) if checksum else None

    ctx.next_offset = 0
    await log(ctx.ui, f"Streaming: {file_obj.meta.filename}", status=LogStatus.INFO)
    try:
        while ctx.next_offset < total_size:
            if ctx.heap and ctx.heap[0].start == ctx.next_offset:
                chunk = heapq.heappop(ctx.heap)

                if hasher:
                    hasher.update(chunk.data)

                yield chunk.data

                ctx.next_offset += len(chunk.data)

                async with ctx.sync.chunk_from_future:
                    ctx.sync.chunk_from_future.notify_all()

                continue

            envelope = await ctx.queues.stream.get()

            if envelope.is_poison_pill:
                break
            if not (chunk := envelope.payload):
                continue

            if chunk.start == ctx.next_offset:
                if hasher:
                    hasher.update(chunk.data)

                yield chunk.data

                ctx.next_offset += len(chunk.data)

                async with ctx.sync.chunk_from_future:
                    ctx.sync.chunk_from_future.notify_all()

            else:
                heapq.heappush(ctx.heap, chunk)
        else:
            await done(ctx.ui, file_obj.meta.filename)

            if hasher and checksum:
                try:
                    verify_stream(
                        hasher,
                        file_obj.meta.filename,
                        checksum,
                        ctx.next_offset,
                        total_size,
                    )
                    await log(
                        ctx.ui, "Hash Verified", status=LogStatus.SUCCESS, progress=True
                    )
                except Exception as e:
                    await log(ctx.ui, str(e), status=LogStatus.ERROR)
                    raise

    finally:
        ctx.heap.clear()
        del ctx.files[file_id]
        ctx.current_files_id.remove(file_id)
        async with ctx.sync.current_files:
            ctx.sync.current_files.notify()


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
