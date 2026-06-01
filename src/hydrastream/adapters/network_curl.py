# adapters/network_curl.py
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from curl_cffi import AsyncSession, CurlOpt, Response
from curl_cffi.requests import RequestsError

from hydrastream.interfaces import NetworkBackend, NetworkStream


class CurlStreamAdapter(NetworkStream):
    def __init__(self, response: Response) -> None:
        self._response = response

    async def aiter_bytes(self, chunk_size: int) -> AsyncGenerator[bytes, None]:
        iterator = self._response.aiter_content(chunk_size=chunk_size)  # type: ignore

        async for chunk in iterator:  # type: ignore
            yield chunk

    def set_speed_limit(self, limit: int) -> None:
        if self._response.curl is not None:
            self._response.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, limit)

    def _get_error_response(self, e: RequestsError) -> Response | None:
        return e.response  # type: ignore


class CurlNetworkAdapter(NetworkBackend):
    def __init__(self, session: AsyncSession[Response]) -> None:
        self.client = session

    async def request(self, method: str, url: str, **kwargs: Any) -> Response:  # noqa: ANN401
        return await self.client.request(method, url, **kwargs)  # type: ignore

    @asynccontextmanager
    async def stream(
        self, url: str, headers: dict[str, str] | None = None
    ) -> AsyncIterator[NetworkStream]:
        async with self.client.stream("GET", url, headers=headers) as r:
            yield CurlStreamAdapter(r)

    async def close(self) -> None:
        await self.client.close()
