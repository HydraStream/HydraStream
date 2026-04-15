# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio

from hydrastream.engine import save_all_states
from hydrastream.exceptions import LogStatus
from hydrastream.models import HydraContext
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
