# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import contextlib
import email.utils
import random
import time
from collections.abc import AsyncIterator
from typing import Unpack

from curl_cffi import CurlError, Response
from curl_cffi.requests import RequestsError
from curl_cffi.requests.session import HttpMethod, RequestParams

from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import MonitorBackend, NetworkBackend, NetworkStream
from hydrastream.utils import redact_url


async def _evaluate_failure(
    ui: MonitorBackend,
    url: str,
    attempt: int,
    response: Response | None,
    exc: Exception | None,
) -> float | None:
    retry_codes = {408, 500, 502, 504}

    safe_url = redact_url(url)
    if response is not None:
        if response.status_code not in retry_codes:
            await ui.log(
                f"Fatal HTTP error {response.status_code} for {safe_url}",
                status=LogStatus.ERROR,
            )
            return None

        server_delay = _get_retry_after(response)

        delay = (
            server_delay if server_delay is not None else random.uniform(0, 2**attempt)
        )
        await ui.log(
            f"Attempt {attempt} failed ({response.status_code}) for {safe_url}. "
            f"Retrying in {delay:.2f}s...",
            status=LogStatus.WARNING,
            throttle_key="net_slow",
        )
        return delay

    if exc is not None:
        if isinstance(
            exc, RequestsError | CurlError | TimeoutError | asyncio.TimeoutError
        ):
            err_name = type(exc).__name__
            if isinstance(exc, CurlError):
                err_name = f"CurlError({exc.code})"

            delay = random.uniform(0, 2**attempt)
            await ui.log(
                f"Network issue ({err_name}) on {safe_url}. "
                f"Retrying in {delay:.2f}s...",
                status=LogStatus.WARNING,
                throttle_key="net_drop",
            )
            return delay

        await ui.log(
            f"Unrecoverable request error for {safe_url}: {exc}",
            status=LogStatus.ERROR,
        )
        return None

    return None


async def safe_request(
    net: NetworkBackend,
    ui: MonitorBackend,
    method: HttpMethod,
    url: str,
    max_retries: int = 3,
    **kwargs: Unpack[RequestParams],
) -> Response | None:
    for attempt in range(1, max_retries + 1):
        response = None
        try:
            resp = await net.request(method, url, **kwargs)
            if resp.status_code < 400:
                return resp

            delay = await _evaluate_failure(ui, url, attempt, response=resp, exc=None)
        except Exception as exc:
            delay = await _evaluate_failure(ui, url, attempt, response=None, exc=exc)

        if delay is None:
            if response is not None:
                raise RequestsError(
                    f"Request failed on {url}",
                    response=response,
                )
            raise RequestsError(f"Request failed on {url} before response was received")
        await asyncio.sleep(delay)

    raise RequestsError(
        f"Failed to establish request for {url} after {max_retries} attempts."
    )


@contextlib.asynccontextmanager
async def stream_chunk(
    net: NetworkBackend,
    ui: MonitorBackend,
    url: str,
    headers: dict[str, str] | None = None,
    max_retries: int = 3,
) -> AsyncIterator[NetworkStream]:
    for attempt in range(1, max_retries + 1):
        response = None
        yielded = False
        try:
            async with net.stream(url, headers=headers) as connect:
                response = connect.response()

                if headers and "Range" in headers and response.status_code == 200:
                    raise RequestsError(
                        "Server ignored Range header and returned 200 OK.",
                        response=response,
                    )

                if response.status_code < 400:
                    yielded = True
                    yield connect
                    return

            delay = await _evaluate_failure(
                ui, url, attempt, response=response, exc=None
            )

        except Exception as exc:
            if yielded:
                raise

            delay = await _evaluate_failure(ui, url, attempt, response=None, exc=exc)

        if delay is None:
            if response is not None:
                raise RequestsError(
                    f"Stream failed on {url}",
                    response=response,
                )
            raise RequestsError(f"Stream failed on {url} before response was received")
        await asyncio.sleep(delay)

    raise RequestsError(
        f"Failed to establish stream for {url} after {max_retries} attempts."
    )


def _get_retry_after(response: Response) -> float | None:
    header = response.headers.get("Retry-After")
    if not header:
        return None
    if header.isdigit():
        return float(header)
    try:
        parsed_date = email.utils.parsedate_tz(header)
        if parsed_date:
            return max(0, email.utils.mktime_tz(parsed_date) - time.time())
    except (ValueError, TypeError):
        pass
    return None
