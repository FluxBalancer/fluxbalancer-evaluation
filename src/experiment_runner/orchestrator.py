import asyncio

import aiohttp

from src.experiment_runner.client import HTTPClient
from src.experiment_runner.config import *
from src.experiment_runner.experiment import build_run
from src.experiment_runner.loadgen import run_load, run_warmup
from src.experiment_runner.models import ExperimentRun, RequestRecord
from src.experiment_runner.workload_plan import build_workload_plan


async def clear_system(delay: float | None = None):
    async with aiohttp.ClientSession() as s:
        async with s.post(f"{system_config.base_url}/clear") as resp:
            pass

    await asyncio.sleep(delay or system_config.clear_delay_s)


async def run_single(balancer, replication=None, adaptive=False, seed: int = 12345) -> ExperimentRun:
    headers = {
        "X-Balancer-Strategy": balancer,
        "X-Weights-Strategy": "entropy",
    }

    if replication:
        headers["X-Replications-Strategy"] = replication

    headers["X-Replications-Adaptive"] = str(adaptive).lower()

    warmup_plan = build_workload_plan(
        total_requests=workload_config.warmup_requests,
        mean_interarrival_s=workload_config.mean_interarrival_s,
        seed=seed,
    )

    measure_plan = build_workload_plan(
        total_requests=workload_config.total_requests,
        mean_interarrival_s=workload_config.mean_interarrival_s,
        seed=seed + 1,
    )

    async with HTTPClient(
            base_url=system_config.base_url,
            headers=headers,
            timeout=system_config.timeout_s,
    ) as client:
        await run_warmup(client, warmup_plan, workload_config, deadline_config)
        reqs = await run_load(client, measure_plan, workload_config, deadline_config)

    run = build_run(
        system_config.base_url,
        {
            "balancer": balancer,
            "replication": replication,
            "adaptive": adaptive,
            "seed": seed,
        },
        reqs,
    )

    return run
