from models import ExperimentRun
from summary import build_summary
from utils import utc_iso


def build_run(base_url, factors, reqs, deadline):
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
