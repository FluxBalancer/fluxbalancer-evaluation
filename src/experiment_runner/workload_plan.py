import random
from dataclasses import dataclass, asdict

from src.experiment_runner.config import endpoint_config, failure_config


@dataclass(slots=True)
class PlannedRequest:
    endpoint: str
    seconds: float
    sleep_before_s: float


def build_endpoint_from_rng(rng: random.Random) -> tuple[str, float]:
    seconds = rng.choices(
        endpoint_config.cpu_seconds,
        weights=endpoint_config.weights,
        k=1,
    )[0]

    if rng.random() < 0.1:
        seconds *= rng.choice([1.25, 1.5, 2])

    query = {
        "seconds": float(seconds),
        **asdict(failure_config),
    }
    query_str = "&".join(f"{k}={v}" for k, v in query.items())

    if rng.random() < 0.5:
        return f"cpu?{query_str}", float(seconds)

    return f"mem?{query_str}&mb={endpoint_config.mem_mb}", float(seconds)


def exp_delay_from_rng(rng: random.Random, mean: float) -> float:
    return rng.expovariate(1 / mean)


def build_workload_plan(
        total_requests: int,
        mean_interarrival_s: float,
        seed: int,
) -> list[PlannedRequest]:
    rng = random.Random(seed)
    plan: list[PlannedRequest] = []

    for _ in range(total_requests):
        endpoint, seconds = build_endpoint_from_rng(rng)
        sleep_before_s = exp_delay_from_rng(rng, mean_interarrival_s)
        plan.append(
            PlannedRequest(
                endpoint=endpoint,
                seconds=seconds,
                sleep_before_s=sleep_before_s,
            )
        )

    return plan
