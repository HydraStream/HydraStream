# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import time

from curl_cffi import CurlOpt

from hydrastream.exceptions import (
    LogStatus,
)
from hydrastream.models import HydraContext
from hydrastream.monitor import log


async def throttle_controller(ctx: HydraContext) -> None:

    ctx.ui.speed.last_checkpoint_time = time.monotonic()

    while not ctx.sync.all_complete.is_set():
        try:
            await ctx.ui.speed.throttler_checkpoint_event.wait()

            if ctx.sync.all_complete.is_set():
                break
            ctx.ui.speed.throttler_checkpoint_event.clear()

            now = time.monotonic()
            elapsed = min(1, now - ctx.ui.speed.last_checkpoint_time)

            if elapsed <= 0:
                continue

            if ctx.ui.speed.speed_limit:
                target_time = ctx.ui.speed.bytes_to_check / ctx.ui.speed.speed_limit

                if elapsed < target_time:
                    sleep_duration = target_time - elapsed

                    for r in ctx.active_stream:
                        if r.curl is not None:
                            r.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, 1)
                    await asyncio.sleep(sleep_duration)

                    for r in ctx.active_stream:
                        if r.curl is not None:
                            r.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, 0)

                    now = time.monotonic()
                    elapsed = now - ctx.ui.speed.last_checkpoint_time

            # Фиксируем время для следующего круга
            ctx.ui.speed.last_checkpoint_time = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if ctx.config.debug:
                raise
            await log(
                ctx.ui, f"Throttle controller failed: {e}", status=LogStatus.ERROR
            )
