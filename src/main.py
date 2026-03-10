import asyncio

from experiment_runner.config import ExperimentConfig
from experiment_runner.orchestrator import run_single


async def main():
    for balancer in ExperimentConfig.balancers_baseline:
        await run_single(balancer)

    for balancer in ExperimentConfig.balancers_replication:
        for strategy in ExperimentConfig.replication_strategies:
            for adaptive in [True, False]:
                await run_single(balancer, strategy, adaptive)


if __name__ == "__main__":
    asyncio.run(main())
