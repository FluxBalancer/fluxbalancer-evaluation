import asyncio

from src.experiment_runner.config import experiment_config
from src.experiment_runner.orchestrator import run_single, clear_system
from src.experiment_runner.storage import create_experiment_dirs, save_experiment


async def main():
    base_seed = 10_000
    for iteration in range(40
                           ):
        seed = base_seed + iteration
        print(f"Iteration №{iteration}, seed={seed}")

        dirs = create_experiment_dirs()

        await clear_system(delay=5)

        for balancer in experiment_config.balancers_replication:
            for strategy in experiment_config.replication_strategies:
                print(f"Algorithm {balancer}_{strategy}")

                run = await run_single(balancer, strategy, False, seed=seed)
                save_experiment(
                    dirs["non_adaptive"],
                    f"{balancer}_{strategy}__seed_{seed}",
                    run.dumps(),
                )
                await clear_system()

                run = await run_single(balancer, strategy, True, seed=seed)

                save_experiment(
                    dirs["adaptive"],
                    f"{balancer}_{strategy}__seed_{seed}",
                    run.dumps(),
                )
                await clear_system()

        for balancer in (
                experiment_config.balancers_baseline
                + experiment_config.balancers_replication
        ):
            print(f"Algorithm {balancer}")
            run = await run_single(balancer, seed=seed)

            save_experiment(
                dirs["baseline"],
                f"{balancer}__seed_{seed}",
                run.dumps(),
            )
            await clear_system()


async def main_haproxy():
    seed = 99_999
    dirs = create_experiment_dirs()
    balancer = "haproxy_random"
    run = await run_single(balancer, seed=seed)

    save_experiment(
        dirs["baseline"],
        balancer,
        run.dumps(),
    )


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(main_haproxy())
