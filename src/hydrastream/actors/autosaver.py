# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio

from hydrastream.exceptions import LogStatus
from hydrastream.models import File, HydraContext
from hydrastream.monitor import log


async def autosaver(ctx: HydraContext, interval: float) -> None:
    loop = asyncio.get_running_loop()

    while not ctx.sync.all_complete.is_set():
        try:
            async with asyncio.timeout(interval):
                await ctx.sync.all_complete.wait()
            break
        except TimeoutError:
            try:
                await loop.run_in_executor(None, save_all_states, ctx, ctx.files)
            except Exception as e:
                if ctx.config.debug:
                    raise
                await log(
                    ctx.ui, f"Auto-save operation failed: {e}", status=LogStatus.ERROR
                )

        except asyncio.CancelledError:
            break


def save_all_states(ctx: HydraContext, files: dict[int, File]) -> None:
    for file in list(files.values()):
        if file.chunks and not all(c.current_pos > c.end for c in (file.chunks or [])):
            ctx.fs.save_state(file)
