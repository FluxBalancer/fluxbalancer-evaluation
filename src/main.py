import asyncio

from src.experiment_runner.config import experiment_config
from src.experiment_runner.orchestrator import run_single, clear_system
from src.experiment_runner.storage import create_experiment_dirs, save_experiment


async def main():
    dirs = create_experiment_dirs()

    await clear_system(delay=5)
    for balancer in (
        experiment_config.balancers_baseline + experiment_config.balancers_replication
    ):
        run = await run_single(balancer)

        save_experiment(
            dirs["baseline"],
            balancer,
            run.dumps(),
        )
        await clear_system()

    # for balancer in experiment_config.balancers_replication:
    #     for strategy in experiment_config.replication_strategies:
    #         run = await run_single(balancer, strategy, False)
    #         save_experiment(
    #             dirs["non_adaptive"],
    #             f"{balancer}_{strategy}",
    #             run.dumps(),
    #         )
    #
    #         run = await run_single(balancer, strategy, True)
    #
    #         save_experiment(
    #             dirs["adaptive"],
    #             f"{balancer}_{strategy}",
    #             run.dumps(),
    #         )
    #         await clear_system()


if __name__ == "__main__":
    asyncio.run(main())
