import asyncio

import aiohttp

from src.experiment_runner.client import HTTPClient
from src.experiment_runner.config import *
from src.experiment_runner.experiment import build_run
from src.experiment_runner.loadgen import run_load
from src.experiment_runner.models import ExperimentRun


async def clear_system(delay: float | None = None):
    async with aiohttp.ClientSession() as s:
        try:
            await s.post(f"{system_config.base_url}/clear")
        except:
            pass

    await asyncio.sleep(delay or system_config.clear_delay_s)


async def run_single(balancer, replication=None, adaptive=None) -> ExperimentRun:
    headers = {
        "X-Balancer-Strategy": balancer,
        "X-Weights-Strategy": "entropy",
    }

    if replication:
        headers["X-Replications-Strategy"] = replication
    if adaptive is not None:
        headers["X-Replications-Adaptive"] = str(adaptive).lower()

    async with HTTPClient(
        base_url=system_config.base_url,
        headers=headers,
        timeout=system_config.timeout_s,
    ) as client:
        reqs = await run_load(client, workload_config, deadline_config)

    run = build_run(
        system_config.base_url,
        {
            "balancer": balancer,
            "replication": replication,
            "adaptive": adaptive,
        },
        reqs,
        3000,
    )

    return run
