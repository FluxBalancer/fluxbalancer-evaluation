from dataclasses import dataclass


@dataclass(slots=True)
class WorkloadConfig:
    total_requests: int = 5000
    concurrency: int = 80
    mean_interarrival_s: float = 0.04
    warmup_requests: int = 600


@dataclass(slots=True)
class DeadlineConfig:
    multiplier: float = 3


@dataclass(slots=True)
class EndpointConfig:
    cpu_seconds: tuple[int, ...] = (2, 3, 4, 9)
    weights: tuple[float, ...] = (0.5, 0.25, 0.15, 0.1)
    # cpu_seconds: tuple[int, ...] = (1, 2, 3, 4, 6)
    # cpu_seconds: tuple[int, ...] = (1, 2, 3, 5, 8, 13)
    # cpu_seconds: tuple[int, ...]= tuple(range(10, 16))
    # cpu_seconds: tuple[int, ...] = tuple(range(50, 60))
    mem_mb: int = 85


@dataclass(slots=True)
class SystemConfig:
    base_url: str = "http://127.0.0.1:8000"
    timeout_s: float = 100.0
    clear_delay_s: int = 5


@dataclass(slots=True)
class ExperimentConfig:
    balancers_replication = ["topsis"]
    balancers_baseline = []
    # replication_strategies = ["hedged", "speculative"]
    replication_strategies = ["hedged"]


@dataclass(slots=True)
class FailureConfig:
    fail_rate: float = 0.02
    slow_rate: float = 0.07
    slow_multiplier: float = 3.0
    jitter_mean: float = 0.2

    extra_delay_prob: float = 0.04
    extra_delay_min: float = 1.0
    extra_delay_max: float = 2.0


workload_config = WorkloadConfig()
deadline_config = DeadlineConfig()
endpoint_config = EndpointConfig()
system_config = SystemConfig()
experiment_config = ExperimentConfig()
failure_config = FailureConfig()
