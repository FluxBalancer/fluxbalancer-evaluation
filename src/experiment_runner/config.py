import random
from dataclasses import dataclass

random.seed(42)


@dataclass(slots=True)
class WorkloadConfig:
    total_requests: int = 1000
    concurrency: int = 50
    mean_interarrival_s: float = 0.05


@dataclass(slots=True)
class DeadlineConfig:
    multiplier: float = 1.3


@dataclass(slots=True)
class EndpointConfig:
    # cpu_seconds = [1, 2, 2, 2, 3, 4]
    # cpu_seconds = [10, 11, 11, 12, 14]
    cpu_seconds: tuple[int, ...] = tuple(range(5, 16))
    mem_mb: int = 75


@dataclass(slots=True)
class SystemConfig:
    base_url: str = "http://127.0.0.1:8000"
    timeout_s: float = 100.0
    clear_delay_s: int = 10


@dataclass(slots=True)
class ExperimentConfig:
    balancers_replication = ["topsis", "airm", "electre"]
    balancers_baseline = ["saw", "lc"]
    replication_strategies = ["hedged", "speculative"]


workload_config = WorkloadConfig()
deadline_config = DeadlineConfig()
endpoint_config = EndpointConfig()
system_config = SystemConfig()
experiment_config = ExperimentConfig()
