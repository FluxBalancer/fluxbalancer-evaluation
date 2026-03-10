from src.experiment_runner.models import ExperimentRun, RequestRecord
from src.experiment_runner.summary import build_summary
from src.experiment_runner.utils import utc_iso


def build_run(base_url: str, factors: dict, reqs: list[RequestRecord], deadline: int):
    return ExperimentRun(
        schema="fluxbalancer-eval/v3",
        run_id=utc_iso().replace(":", "_"),
        started_at=utc_iso(),
        finished_at=utc_iso(),
        base_url=base_url,
        factors=factors,
        requests=reqs,
        summary=build_summary(reqs, deadline),
    )
