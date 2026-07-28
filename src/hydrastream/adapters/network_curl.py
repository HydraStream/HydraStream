# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, override

from curl_cffi import AsyncSession, BrowserTypeLiteral, CurlOpt, Headers, Response
from curl_cffi.requests import RequestsError

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import NetworkBackend, NetworkStream


@hydra_dataclass(frozen=True)
class CurlStreamAdapter(NetworkStream):
    _response: Response

    @property
    @override
    def response(self) -> Response:
        return self._response

    @property
    @override
    def headers(self) -> dict[str, str]:
        return self._response.headers  # type: ignore

    @override
    async def aiter_bytes(self, chunk_size: int) -> AsyncGenerator[bytes, None]:
        iterator = self._response.aiter_content(chunk_size=chunk_size)  # type: ignore

        async for chunk in iterator:  # type: ignore
            yield chunk

    @override
    def set_speed_limit(self, limit: int) -> None:
        if self._response.curl is not None:
            self._response.curl.setopt(CurlOpt.MAX_RECV_SPEED_LARGE, limit)


class CurlNetworkAdapter(NetworkBackend):
    def __init__(
        self,
        threads: int,
        impersonate: BrowserTypeLiteral,
        client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        options = (client_kwargs or {}).copy()

        user_headers = options.pop("headers", None)
        headers_obj = Headers(user_headers)
        headers_obj.setdefault("Accept-Encoding", "identity")
        headers_obj.setdefault("Connection", "keep-alive")
        options["headers"] = headers_obj
        options.setdefault("max_clients", threads)
        options.setdefault("impersonate", impersonate)
        options.setdefault("timeout", 30.0)

        self.client = AsyncSession(
            **options,
        )

    @override
    async def request(self, method: str, url: str, **kwargs: Any) -> Response:
        return await self.client.request(method, url, **kwargs)  # type: ignore

    @override
    @asynccontextmanager
    async def stream(
        self, url: str, headers: dict[str, str] | None = None
    ) -> AsyncGenerator[NetworkStream]:
        async with self.client.stream("GET", url, headers=headers) as r:
            yield CurlStreamAdapter(_response=r)

    @override
    @staticmethod
    def get_error_response(e: RequestsError) -> Response | None:
        return e.response  # type: ignore

    @override
    async def close(self) -> None:
        await self.client.close()
