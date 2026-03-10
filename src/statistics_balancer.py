from __future__ import annotations

import asyncio
import hashlib
import json
import random
import time
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import fmean
from typing import Any, Optional

import aiohttp


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


def jitter_delay(mean_delay_s: float) -> float:
    if mean_delay_s <= 0:
        return 0.0
    return random.expovariate(1 / mean_delay_s)


def parse_socket_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


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
    error_kind: str | None = None


@dataclass(slots=True)
class ExperimentRun:
    schema: str
    run_id: str
    started_at: str
    finished_at: str
    base_url: str
    factors: dict[str, Any]
    requests: list[RequestRecord]
    summary: dict[str, Any]


class StatisticsBalancer:
    def __init__(
            self,
            base_url: str,
            headers: dict[str, str],
            factors: dict[str, Any],
            *,
            timeout_total_s: float,
            deadline_ms: int,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = headers
        self.factors = factors
        self.timeout_total_s = timeout_total_s
        self.deadline_ms = deadline_ms

        self.started_at = utc_iso()
        self.finished_at: str | None = None
        self.run_id = self.started_at.replace(":", "_")

        self.requests: list[RequestRecord] = []
        self.session: Optional[aiohttp.ClientSession] = None

        self._requested_r_total = 0
        self._effective_r_total = 0

    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=self.timeout_total_s)
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session is not None:
            await self.session.close()

    async def one_request(self, req_id: str, endpoint: str) -> RequestRecord:
        assert self.session is not None

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        t0 = time.perf_counter()
        started_at = utc_iso()

        status = 0
        raw = b""
        err: str | None = None
        error_kind: str | None = None
        resp_json: Any = None

        upstream_sockets: list[str] = []
        winner_socket: str | None = None
        replication_error: str | None = None

        try:
            async with self.session.get(url) as resp:
                status = int(resp.status)
                raw = await resp.read()

                upstream_sockets = parse_socket_list(resp.headers.get("X-Upstream-Socket"))
                winner_socket = resp.headers.get("X-Winner-Socket")
                replication_error = resp.headers.get("X-Replication-Error")

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

        if err is not None:
            error_kind = "transport_error"
        elif replication_error:
            error_kind = f"replication:{replication_error}"
        elif status == 504:
            error_kind = "gateway_timeout"
        elif status >= 500:
            error_kind = "upstream_5xx"
        elif 400 <= status < 500:
            error_kind = "upstream_4xx"

        signals = {
            "cpu_burn": bool(resp_json.get("cpu_burn")) if isinstance(resp_json, dict) else False,
            "mem_burn": bool(resp_json.get("mem_burn")) if isinstance(resp_json, dict) else False,
            "cpu_util": resp_json.get("cpu_util") if isinstance(resp_json, dict) else None,
            "mem_util": resp_json.get("mem_util") if isinstance(resp_json, dict) else None,
            "net_in_bytes": resp_json.get("net_in_bytes") if isinstance(resp_json, dict) else None,
            "net_out_bytes": resp_json.get("net_out_bytes") if isinstance(resp_json, dict) else None,
            "replication_degraded": replication_error == "degraded",
        }

        return RequestRecord(
            req_id=req_id,
            endpoint="/" + endpoint.lstrip("/"),
            method="GET",
            started_at=started_at,
            finished_at=finished_at,
            latency_ms=float(latency_ms),
            status=status,
            ok=ok,
            upstream={
                "sockets": upstream_sockets,
                "winner_socket": winner_socket,
                "replication_error": replication_error,
            },
            response={
                "bytes": len(raw),
                "sha256": sha256_hex(raw) if raw else "",
                "json": resp_json if isinstance(resp_json, (dict, list)) else None,
            },
            signals=signals,
            error=err,
            error_kind=error_kind,
        )

    def extend(self, reqs: list[RequestRecord]) -> None:
        self.requests.extend(reqs)

    def finalize(self) -> ExperimentRun:
        self.finished_at = utc_iso()

        all_reqs = self.requests
        all_lat = [r.latency_ms for r in all_reqs]
        all_ok = [r.ok for r in all_reqs]

        error_kinds = Counter(r.error_kind or "none" for r in all_reqs)
        statuses = Counter(str(r.status) for r in all_reqs)
        replication_errors = Counter(
            r.upstream.get("replication_error") or "none"
            for r in all_reqs
        )

        degraded_count = sum(
            1 for r in all_reqs
            if r.signals.get("replication_degraded") is True
        )

        within_deadline_count = sum(
            1 for r in all_reqs
            if r.latency_ms <= self.deadline_ms
        )

        winner_counts = Counter(
            r.upstream.get("winner_socket")
            for r in all_reqs
            if r.upstream.get("winner_socket")
        )

        participation_counts = Counter()
        for r in all_reqs:
            for s in r.upstream.get("sockets", []) or []:
                participation_counts[s] += 1

        summary = {
            "total_requests": len(all_reqs),
            "overall": {
                "latency_ms": {
                    "mean": float(fmean(all_lat)) if all_lat else 0.0,
                    "p50": percentile(all_lat, 0.50),
                    "p95": percentile(all_lat, 0.95),
                    "p99": percentile(all_lat, 0.99),
                    "max": float(max(all_lat)) if all_lat else 0.0,
                },
                "error_rate": (
                        1.0 - (sum(1 for x in all_ok if x) / len(all_ok))
                ) if all_ok else 0.0,
                "within_deadline_rate": (
                        within_deadline_count / len(all_reqs)
                ) if all_reqs else 0.0,
            },
            "replication": {
                "avg_requested_r": self._requested_r_total / len(all_reqs) if all_reqs else 0.0,
                "avg_effective_r": self._effective_r_total / len(all_reqs) if all_reqs else 0.0,
                "degraded_rate": degraded_count / len(all_reqs) if all_reqs else 0.0,
                "errors_by_reason": dict(replication_errors),
            },
            "upstreams": {
                "winner_counts": dict(winner_counts),
                "participation_counts": dict(participation_counts),
            },
            "errors": {
                "by_kind": dict(error_kinds),
                "by_status": dict(statuses),
                "degraded_count": degraded_count,
            },
        }

        return ExperimentRun(
            schema="fluxbalancer-eval/v2",
            run_id=self.run_id,
            started_at=self.started_at,
            finished_at=self.finished_at,
            base_url=self.base_url,
            factors=self.factors,
            requests=all_reqs,
            summary=summary,
        )

    def dumps(self) -> str:
        return json.dumps(asdict(self.finalize()), indent=2, ensure_ascii=False)


BASE_URL = "http://127.0.0.1:8000"
OUT_DIR = "experiments_auto"
ALGO_DELAY = 10

TOTAL_REQUESTS = 10
CONCURRENCY = 40
MEAN_INTERARRIVAL_S = 0.05
TIMEOUT_TOTAL_S = 60.0

CPU_SECONDS_CHOICES = [1, 2, 2, 2, 3, 4]
MEM_MB = 50


def build_endpoint() -> tuple[str, int]:
    seconds = random.choice(CPU_SECONDS_CHOICES)

    if random.random() < 0.5:
        return f"cpu?seconds={seconds}", seconds

    return f"mem?seconds={seconds}&mb={MEM_MB}", seconds


async def run_load(
        sb: StatisticsBalancer,
        *,
        total_requests: int,
        concurrency: int,
        mean_interarrival_s: float,
) -> None:
    sem = asyncio.Semaphore(concurrency)
    tasks: list[asyncio.Task[RequestRecord]] = []

    async def worker(req_id: str, endpoint: str) -> RequestRecord:
        async with sem:
            return await sb.one_request(req_id, endpoint)

    for i in range(total_requests):
        req_id = f"req-{i + 1:05d}"
        endpoint, seconds = build_endpoint()
        deadline_ms = int(seconds * 1000 * 1.3)
        sb.deadline_ms = deadline_ms

        tasks.append(asyncio.create_task(worker(req_id, endpoint)))

        if i < total_requests - 1:
            await asyncio.sleep(jitter_delay(mean_interarrival_s))

    results = await asyncio.gather(*tasks)
    sb.extend(results)


async def run_experiment(
        *,
        balancer: str,
        replication: str | None = None,
        adaptive: bool | None = None,
        weights: str = "entropy",
) -> None:
    replication_enabled = replication is not None

    factors = {
        "balancer": {"name": balancer},
        "weights": {"name": weights},
        "workload": {
            "total_requests": TOTAL_REQUESTS,
            "concurrency": CONCURRENCY,
            "mean_interarrival_s": MEAN_INTERARRIVAL_S,
            "mix": {
                "cpu_seconds_choices": CPU_SECONDS_CHOICES,
                "mem_mb": MEM_MB,
            },
        },
        "replication": {
            "enabled": replication_enabled,
            "strategy": replication,
            "replications_count": 2 if replication_enabled else 1,
            "completion": {"strategy": "first", "k": 1},
            "adaptive_limit": adaptive,
            "deadline_ms": DEADLINE_MS,
        },
    }

    headers = {
        "X-Balancer-Strategy": balancer,
        "X-Weights-Strategy": weights,
        "X-Balancer-Deadline": str(DEADLINE_MS),
    }

    if replication_enabled:
        headers["X-Replications-Strategy"] = replication
        headers["X-Replications-Adaptive"] = str(adaptive).lower()
        headers["X-Completion-Strategy"] = "first"

    async with StatisticsBalancer(
            base_url=BASE_URL,
            headers=headers,
            factors=factors,
            timeout_total_s=TIMEOUT_TOTAL_S,
            deadline_ms=DEADLINE_MS,
    ) as sb:
        await run_load(
            sb,
            total_requests=TOTAL_REQUESTS,
            concurrency=CONCURRENCY,
            mean_interarrival_s=MEAN_INTERARRIVAL_S,
        )

        result_json = sb.dumps()

    folder = Path(OUT_DIR)
    folder.mkdir(exist_ok=True)

    name_parts = [balancer]
    if replication:
        name_parts.append(replication)
    if adaptive is None:
        name_parts.append("baseline")
    else:
        name_parts.append("adaptive" if adaptive else "no_adaptive")

    filename = "_".join(name_parts) + ".json"
    (folder / filename).write_text(result_json, encoding="utf-8")
    print("finished:", filename)


async def clear_system(delay: int) -> None:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{BASE_URL}/clear") as resp:
                await resp.read()
        except Exception as e:
            print("clear failed:", e)
    await asyncio.sleep(delay)


async def orchestrator() -> None:
    balancers_with_replication = ["topsis", "airm", "electre"]
    balancers_without_replication = ["saw", "lc"]
    replication_strategies = ["hedged", "speculative"]

    await clear_system(delay=0)

    for balancer in balancers_with_replication + balancers_without_replication:
        await run_experiment(balancer=balancer)
        print("clear system...\n")
        await clear_system(delay=ALGO_DELAY)

    for balancer in balancers_with_replication:
        for replication in replication_strategies:
            for adaptive in [False, True]:
                await run_experiment(
                    balancer=balancer,
                    replication=replication,
                    adaptive=adaptive,
                )
                print("clear system...\n")
                await clear_system(delay=ALGO_DELAY)


if __name__ == "__main__":
    asyncio.run(orchestrator())