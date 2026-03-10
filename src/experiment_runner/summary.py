from collections import Counter
from statistics import fmean

from src.experiment_runner.models import RequestRecord
from src.experiment_runner.utils import percentile


def build_summary(reqs: list[RequestRecord], deadline_ms: int):
    lat: list[float] = [r.latency_ms for r in reqs]
    ok: list[bool] = [r.ok for r in reqs]

    statuses = Counter(str(r.status) for r in reqs)
    error_kinds = Counter(r.error_kind or "none" for r in reqs)

    within_deadline: int = sum(1 for r in reqs if r.latency_ms <= deadline_ms)

    winner_counts = Counter(
        r.upstream.get("winner_socket") for r in reqs if r.upstream.get("winner_socket")
    )

    participation = Counter()

    for r in reqs:
        for s in r.upstream.get("sockets", []):
            participation[s] += 1

    return {
        "total_requests": len(reqs),
        "overall": {
            "latency_ms": {
                "mean": fmean(lat),
                "p50": percentile(lat, 0.5),
                "p95": percentile(lat, 0.95),
                "p99": percentile(lat, 0.99),
                "max": max(lat),
            },
            "error_rate": 1 - sum(ok) / len(ok),
            "within_deadline_rate": within_deadline / len(reqs),
        },
        "upstreams": {
            "winner_counts": dict(winner_counts),
            "participation_counts": dict(participation),
        },
        "errors": {
            "by_kind": dict(error_kinds),
            "by_status": dict(statuses),
        },
    }
