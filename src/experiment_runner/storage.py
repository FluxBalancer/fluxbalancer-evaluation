from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXPERIMENTS_ROOT = PROJECT_ROOT / "src/assets/experiments"


def create_experiment_dirs() -> dict[str, Path]:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = EXPERIMENTS_ROOT / ts

    baseline = run_dir / "baseline"
    adaptive = run_dir / "adaptive"
    non_adaptive = run_dir / "non_adaptive"

    baseline.mkdir(parents=True, exist_ok=True)
    adaptive.mkdir(parents=True, exist_ok=True)
    non_adaptive.mkdir(parents=True, exist_ok=True)

    return {
        "root": run_dir,
        "baseline": baseline,
        "adaptive": adaptive,
        "non_adaptive": non_adaptive,
    }


def save_experiment(folder: Path, name: str, json_data: str) -> None:
    path = folder / f"{name}.json"
    path.write_text(json_data, encoding="utf-8")
