import asyncio
import random

from src.experiment_runner.client import HTTPClient
from src.experiment_runner.config import WorkloadConfig, DeadlineConfig
from src.experiment_runner.config import endpoint_config
from src.experiment_runner.models import RequestRecord
from src.experiment_runner.utils import jitter_delay


def build_endpoint():
    seconds = random.choice(endpoint_config.cpu_seconds)

    if random.random() < 0.5:
        return f"cpu?seconds={seconds}", seconds

    return f"mem?seconds={seconds}&mb={endpoint_config.mem_mb}", seconds


async def run_load(
    client: HTTPClient, config: WorkloadConfig, deadline_cfg: DeadlineConfig
) -> list[RequestRecord]:
    sem = asyncio.Semaphore(config.concurrency)

    async def worker(i) -> RequestRecord:
        endpoint, seconds = build_endpoint()

        deadline_ms = int(seconds * 1000 * deadline_cfg.multiplier)
        headers = {"X-Balancer-Deadline": str(deadline_ms)}

        async with sem:
            return await client.request(
                f"req-{i}",
                endpoint,
                headers=headers,
            )

    tasks = []

    for i in range(config.total_requests):
        tasks.append(asyncio.create_task(worker(i)))

        await asyncio.sleep(jitter_delay(config.mean_interarrival_s))

    return await asyncio.gather(*tasks)
