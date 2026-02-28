from __future__ import annotations

import asyncio
import hashlib
import json
import random
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from statistics import fmean
from typing import Any, Optional

import aiohttp
import requests


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    xs = sorted(values)
    idx = int(round((len(xs) - 1) * q))
    return float(xs[idx])


def jitter_delay(delay_s: float) -> float:
    if delay_s <= 0:
        return 0.0
    return random.uniform(0.3 * delay_s, delay_s)


def parse_socket_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


@dataclass(slots=True)
class RequestRecord:
    req_id: str
    endpoint: str
    method: str
    started_at: str
    finished_at: str
    latency_ms: float
    status: int
    ok: bool
    upstream: dict[str, Any]
    response: dict[str, Any]
    signals: dict[str, Any]
    error: str | None = None


@dataclass(slots=True)
class WaveRecord:
    wave_id: int
    planned: dict[str, Any]
    started_at: str
    finished_at: str
    makespan_ms: float
    requests: list[RequestRecord]
    by_socket: dict[str, Any]


@dataclass(slots=True)
class ExperimentRun:
    schema: str
    run_id: str
    started_at: str
    base_url: str
    factors: dict[str, Any]
    waves: list[WaveRecord]
    summary: dict[str, Any]


class StatisticsBalancer:
    def __init__(
            self,
            base_url: str,
            headers: dict[str, str],
            factors: dict[str, Any],
            *,
            requests_per_wave: int = 30,
            delay_between_requests_s: float = 0.3,
            timeout_total_s: float = 60.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = headers
        self.factors = factors
        self.requests_per_wave = requests_per_wave
        self.delay_between_requests_s = delay_between_requests_s
        self.timeout_total_s = timeout_total_s

        self.started_at = utc_iso()
        self.run_id = f"{self.started_at}__{factors.get('balancer', {}).get('name')}"
        self.run_id = self.run_id.replace(":", "_")

        self.waves: list[WaveRecord] = []
        self.session: Optional[aiohttp.ClientSession] = None

        self._requested_r_total = 0
        self._effective_r_total = 0

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.timeout_total_s)
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def _one_request(self, req_id: str, endpoint: str) -> RequestRecord:
        assert self.session is not None

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        t0 = time.perf_counter()
        started_at = utc_iso()

        status = 0
        raw = b""
        err: str | None = None
        resp_json: Any = None
        upstream_sockets: list[str] = []
        winner_socket: str | None = None

        try:
            async with self.session.get(url) as resp:
                status = int(resp.status)
                raw = await resp.read()

                upstream_sockets = parse_socket_list(
                    resp.headers.get("X-Upstream-Socket")
                )

                winner_socket = resp.headers.get("X-Winner-Socket")

                r_req = resp.headers.get("X-Replica-Count")
                r_eff = resp.headers.get("X-Replica-Effective")

                if r_req:
                    self._requested_r_total += int(r_req)
                if r_eff:
                    self._effective_r_total += int(r_eff)

                try:
                    resp_json = json.loads(raw.decode("utf-8"))
                except Exception:
                    resp_json = None

        except Exception as e:
            err = f"{type(e).__name__}: {e}"

        latency_ms = (time.perf_counter() - t0) * 1000.0
        finished_at = utc_iso()
        ok = (200 <= status < 300) and (err is None)

        signals = {
            "cpu_burn": bool(resp_json.get("cpu_burn")) if isinstance(resp_json, dict) else False,
            "mem_burn": bool(resp_json.get("mem_burn")) if isinstance(resp_json, dict) else False,
            "cpu_util": resp_json.get("cpu_util") if isinstance(resp_json, dict) else None,
            "mem_util": resp_json.get("mem_util") if isinstance(resp_json, dict) else None,
            "net_in_bytes": resp_json.get("net_in_bytes") if isinstance(resp_json, dict) else None,
            "net_out_bytes": resp_json.get("net_out_bytes") if isinstance(resp_json, dict) else None,
        }

        return RequestRecord(
            req_id=req_id,
            endpoint="/" + endpoint.lstrip("/"),
            method="GET",
            started_at=started_at,
            finished_at=finished_at,
            latency_ms=float(latency_ms),
            status=status,
            ok=bool(ok),
            upstream={
                "sockets": upstream_sockets,
                "winner_socket": winner_socket,
            },
            response={
                "bytes": int(len(raw)),
                "sha256": sha256_hex(raw) if raw else "",
                "json": resp_json if isinstance(resp_json, (dict, list)) else None,
            },
            signals=signals,
            error=err,
        )

    async def wave(self, wave_id: int, endpoints: list[str]) -> None:
        planned = {
            "requests": self.requests_per_wave,
            "delay_s": self.delay_between_requests_s,
        }

        started_at = utc_iso()
        t0 = time.perf_counter()

        tasks = []

        for i in range(self.requests_per_wave):
            ep = endpoints[i % len(endpoints)]
            req_id = f"{wave_id}-{i + 1:04d}"
            tasks.append(asyncio.create_task(self._one_request(req_id, ep)))

            if i < self.requests_per_wave - 1:
                await asyncio.sleep(jitter_delay(self.delay_between_requests_s))

        reqs = await asyncio.gather(*tasks)

        makespan_ms = (time.perf_counter() - t0) * 1000.0
        finished_at = utc_iso()

        winner_counts: dict[str, int] = {}
        participation_counts: dict[str, int] = {}
        latency_by_winner: dict[str, list[float]] = {}

        for r in reqs:
            sockets = r.upstream.get("sockets", []) or []
            winner = r.upstream.get("winner_socket")

            for s in sockets:
                participation_counts[s] = participation_counts.get(s, 0) + 1

            if winner:
                winner_counts[winner] = winner_counts.get(winner, 0) + 1
                latency_by_winner.setdefault(winner, []).append(r.latency_ms)

        self.waves.append(
            WaveRecord(
                wave_id=wave_id,
                planned=planned,
                started_at=started_at,
                finished_at=finished_at,
                makespan_ms=float(makespan_ms),
                requests=reqs,
                by_socket={
                    "winner_counts": winner_counts,
                    "participation_counts": participation_counts,
                    "latency_ms": latency_by_winner,
                },
            )
        )

    def finalize(self) -> ExperimentRun:
        all_lat = [req.latency_ms for w in self.waves for req in w.requests]
        all_ok = [req.ok for w in self.waves for req in w.requests]

        summary = {
            "total_requests": len(all_lat),
            "overall": {
                "latency_ms": {
                    "mean": float(fmean(all_lat)) if all_lat else 0.0,
                    "p50": percentile(all_lat, 0.50),
                    "p95": percentile(all_lat, 0.95),
                    "max": float(max(all_lat)) if all_lat else 0.0,
                },
                "error_rate": (1.0 - (sum(1 for x in all_ok if x) / len(all_ok))) if all_ok else 0.0,
            },
            "replication": {
                "avg_requested_r": self._requested_r_total / len(all_lat) if all_lat else 0.0,
                "avg_effective_r": self._effective_r_total / len(all_lat) if all_lat else 0.0,
            },
        }

        return ExperimentRun(
            schema="fluxbalancer-eval/v1",
            run_id=self.run_id,
            started_at=self.started_at,
            base_url=self.base_url,
            factors=self.factors,
            waves=self.waves,
            summary=summary,
        )

    def dumps(self) -> str:
        return json.dumps(asdict(self.finalize()), indent=2, ensure_ascii=False)


# =========================
# MAIN
# =========================

async def main():
    base_url = "http://127.0.0.1:8000"

    balancer_name = "airm"
    replication_name = "hedged"

    factors = {
        "balancer": {"name": balancer_name},
        "weights": {"name": "entropy"},
        "replication": {
            "enabled": True,
            "strategy": replication_name,
            "replications_count": 4,
            "completion": {"strategy": "k_out_of_n", "k": 2},
            "adaptive_limit": True,
            "deadline_ms": 5500,
        },
    }

    seconds = 10
    headers = {
        "X-Balancer-Strategy": balancer_name,
        "X-Weights-Strategy": "entropy",
        "X-Balancer-Deadline": str(int(seconds * 1000 * 1.25)),
        "X-Replications-Strategy": replication_name,
        "X-Completion-Strategy": "first",
    }

    # res = requests.get(base_url + f"/cpu?seconds={seconds}", headers=headers)
    # print(res.text)

    async with StatisticsBalancer(
            base_url=base_url,
            headers=headers,
            factors=factors,
            requests_per_wave=20,
            delay_between_requests_s=0.2,
    ) as sb:
        seconds = 2
        await sb.wave(1, [f"cpu?seconds={seconds}", f"mem?seconds={seconds}"])
        await sb.wave(2, [f"cpu?seconds={seconds}", f"mem?seconds={seconds}"])

        result_json = sb.dumps()
        print(result_json)

        with open(f"experiments/{balancer_name}_{replication_name}.json", "w", encoding="utf-8") as f:
            f.write(result_json)


if __name__ == "__main__":
    asyncio.run(main())
