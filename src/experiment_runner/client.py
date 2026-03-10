import json
import time

import aiohttp

from models import RequestRecord
from utils import utc_iso, sha256_hex, parse_socket_list


class HTTPClient:
    def __init__(self, base_url: str, headers: dict, timeout: float):
        self.base_url = base_url.rstrip("/")
        self.headers = headers
        self.timeout = timeout
        self.session = None

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self

    async def __aexit__(self, *args):
        await self.session.close()

    async def request(self, req_id: str, endpoint: str, headers: dict | None = None) -> RequestRecord:
        url = f"{self.base_url}/{endpoint}"
        started = utc_iso()
        t0 = time.perf_counter()

        status = 0
        raw = b""
        error = None
        resp_json = None

        sockets = []
        winner = None
        repl_error = None

        try:
            async with self.session.get(url, headers=headers) as resp:

                status = resp.status
                raw = await resp.read()

                sockets = parse_socket_list(resp.headers.get("X-Upstream-Socket"))
                winner = resp.headers.get("X-Winner-Socket")
                repl_error = resp.headers.get("X-Replication-Error")

                try:
                    resp_json = json.loads(raw.decode())
                except Exception:
                    pass

        except Exception as e:
            error = str(e)

        latency = (time.perf_counter() - t0) * 1000

        return RequestRecord(
            req_id=req_id,
            endpoint=endpoint,
            latency_ms=latency,
            status=status,
            ok=200 <= status < 300 and error is None,
            upstream={
                "sockets": sockets,
                "winner_socket": winner,
                "replication_error": repl_error,
            },
            response={
                "bytes": len(raw),
                "sha256": sha256_hex(raw) if raw else "",
                "json": resp_json,
            },
            signals=resp_json if isinstance(resp_json, dict) else {},
            error=error,
            error_kind=None,
        )
