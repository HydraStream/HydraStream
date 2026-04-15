# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import math
import time

from hydrastream.exceptions import (
    LogStatus,
)
from hydrastream.models import HydraContext
from hydrastream.monitor import log
from hydrastream.utils import format_size


async def adaptive_controller(ctx: HydraContext) -> None:

    ctx.ui.speed.last_checkpoint_time = time.monotonic()
    smoothed_speed = 0.0
    prev_speed = 0.0
    current_limit = ctx.dynamic_limit

    # Безопасные границы для адаптивного окна
    tau = 1.0
    min_window = 1024  # 1 КБ (чтобы не сжечь CPU)

    while not ctx.sync.all_complete.is_set():
        try:
            # Ждем пульса от Монитора (или пинка перед смертью)
            await ctx.ui.speed.checkpoint_event.wait()

            # Сразу проверяем: а не нажали ли рубильник пока мы спали?
            if ctx.sync.stop_adaptive_controller.is_set():
                break  # Выходим из цикла! Функция завершается, TaskGroup счастлив.

            ctx.ui.speed.checkpoint_event.clear()

            now = time.monotonic()
            elapsed = min(1, now - ctx.ui.speed.last_checkpoint_time)
            speed_now = ctx.ui.speed.bytes_to_check / elapsed

            if smoothed_speed == 0.0:
                smoothed_speed = speed_now
            else:
                # Магия: вычисляем вес нового замера на основе физического времени
                alpha = 1.0 - math.exp(-elapsed / tau)
                smoothed_speed = (alpha * speed_now) + ((1.0 - alpha) * smoothed_speed)

            coef = 1 / speed_now**0.2
            # Пересчитываем, когда мы хотим проснуться в следующий раз
            new_bytes_to_check = int(ctx.ui.speed.bytes_to_check * (1 - coef + elapsed))

            ctx.ui.speed.bytes_to_check = max(min_window, new_bytes_to_check)
            # Анализируем результат нашего предыдущего шага
            if smoothed_speed > prev_speed * 1.05:
                prev_speed = smoothed_speed
                # Скорость выросла! Добавляем воркеров (если не уперлись в лимит юзера)
                if current_limit < ctx.config.threads:
                    current_limit += 1
                    await log(
                        ctx.ui,
                        f"Speed increased to {format_size(speed_now)}/s. "
                        f"Scaling up to {current_limit} workers.",
                        status=LogStatus.INFO,
                        throttle_key="scale_up",
                        throttle_sec=5.0,
                    )

            elif smoothed_speed < prev_speed * 0.95:
                prev_speed = smoothed_speed
                # Скорость резко упала (сеть забилась). Скидываем воркеров.
                if current_limit > 2:
                    current_limit -= 1
                    await log(
                        ctx.ui,
                        f"Network congested. Scaling down to {current_limit} workers.",
                        status=LogStatus.WARNING,
                        throttle_key="scale_down",
                        throttle_sec=5.0,
                    )

            # Применяем лимит
            if ctx.dynamic_limit != current_limit:
                ctx.dynamic_limit = current_limit
                # Будим диспетчера только если лимит изменился
                async with ctx.sync.dynamic_limit:
                    ctx.sync.dynamic_limit.notify_all()

            # Фиксируем время для следующего круга
            ctx.ui.speed.last_checkpoint_time = time.monotonic()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if ctx.config.debug:
                raise
            await log(
                ctx.ui, f"Adaptive controller failed: {e}", status=LogStatus.ERROR
            )
