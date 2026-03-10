from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class RequestRecord:
    req_id: str
    endpoint: str
    latency_ms: float
    status: int
    ok: bool
    upstream: dict[str, Any]
    response: dict[str, Any]
    signals: dict[str, Any]
    error: str | None
    error_kind: str | None


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
