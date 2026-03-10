import asyncio

import aiohttp

from client import HTTPClient
from config import *
from experiment import build_run
from loadgen import run_load


async def clear_system():
    async with aiohttp.ClientSession() as s:
        try:
            await s.post(f"{SystemConfig.base_url}/clear")
        except:
            pass

    await asyncio.sleep(SystemConfig.clear_delay_s)


async def run_single(balancer, replication=None, adaptive=None):
    headers = {
        "X-Balancer-Strategy": balancer,
        "X-Weights-Strategy": "entropy",
    }

    if replication:
        headers["X-Replications-Strategy"] = replication

    async with HTTPClient(
            SystemConfig.base_url,
            headers,
            SystemConfig.timeout_s,
    ) as client:
        reqs = await run_load(client, WorkloadConfig(), DeadlineConfig())

    run = build_run(SystemConfig.base_url, {}, reqs, 3000)

    return run
