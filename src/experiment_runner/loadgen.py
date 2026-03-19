import asyncio

from src.experiment_runner.client import HTTPClient
from src.experiment_runner.config import WorkloadConfig, DeadlineConfig
from src.experiment_runner.models import RequestRecord
from src.experiment_runner.workload_plan import PlannedRequest


async def run_load(
        client: HTTPClient,
        plan: list[PlannedRequest],
        config: WorkloadConfig,
        deadline_cfg: DeadlineConfig,
) -> list[RequestRecord]:
    sem = asyncio.Semaphore(config.concurrency)

    async def worker(i: int, item: PlannedRequest) -> RequestRecord:
        deadline_ms = int(item.seconds * 1000 * deadline_cfg.multiplier)
        headers = {"X-Balancer-Deadline": str(deadline_ms)}

        async with sem:
            record = await client.request(
                f"req-{i}",
                item.endpoint,
                headers=headers,
            )
            record.deadline_ms = deadline_ms
            return record

    tasks = []

    for i, item in enumerate(plan):
        tasks.append(asyncio.create_task(worker(i, item)))
        await asyncio.sleep(item.sleep_before_s)

    return await asyncio.gather(*tasks)


async def run_warmup(
        client: HTTPClient,
        plan: list[PlannedRequest],
        config: WorkloadConfig,
        deadline_cfg: DeadlineConfig,
) -> None:
    sem = asyncio.Semaphore(config.concurrency)

    async def worker(i: int, item: PlannedRequest) -> None:
        deadline_ms = int(item.seconds * 1000 * deadline_cfg.multiplier)
        headers = {"X-Balancer-Deadline": str(deadline_ms)}

        async with sem:
            await client.request(
                f"warmup-{i}",
                item.endpoint,
                headers=headers,
            )

    tasks = []

    for i, item in enumerate(plan):
        tasks.append(asyncio.create_task(worker(i, item)))
        await asyncio.sleep(item.sleep_before_s)

    await asyncio.gather(*tasks)
