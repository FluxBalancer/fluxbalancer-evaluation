import glob
import json
import math

import matplotlib.pyplot as plt
import numpy as np

try:
    from scipy import stats

    SCIPY_AVAILABLE = True
except Exception:
    SCIPY_AVAILABLE = False


# =========================
# Загрузка данных
# =========================

def load_runs(folder: str):
    runs = []
    for path in glob.glob(f"{folder}/*.json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            runs.append(data)
    return runs


def extract_latencies(run):
    return [
        req["latency_ms"]
        for w in run["waves"]
        for req in w["requests"]
    ]


def descriptive_stats(xs):
    xs = np.array(xs)

    return {
        "mean": float(np.mean(xs)),
        "median": float(np.median(xs)),
        "std": float(np.std(xs, ddof=1)),
        "p95": float(np.percentile(xs, 95)),
        "p99": float(np.percentile(xs, 99)),
        "max": float(np.max(xs)),
        "min": float(np.min(xs)),
    }


def cohens_d(x, y):
    x = np.array(x)
    y = np.array(y)

    nx = len(x)
    ny = len(y)

    pooled_std = math.sqrt(
        ((nx - 1) * np.var(x, ddof=1) + (ny - 1) * np.var(y, ddof=1))
        / (nx + ny - 2)
    )

    return (np.mean(x) - np.mean(y)) / pooled_std


def cliffs_delta(x, y):
    x = np.array(x)
    y = np.array(y)

    greater = 0
    less = 0

    for xi in x:
        greater += np.sum(xi > y)
        less += np.sum(xi < y)

    return (greater - less) / (len(x) * len(y))


# =========================
# Сравнение двух запусков
# =========================

def compare_two(name1, x, name2, y):
    print(f"\n===== {name1} vs {name2} =====")

    print("Cohen's d:", cohens_d(x, y))
    print("Cliff's delta:", cliffs_delta(x, y))

    if SCIPY_AVAILABLE:
        t_stat, t_p = stats.ttest_ind(x, y, equal_var=False)
        u_stat, u_p = stats.mannwhitneyu(x, y, alternative="two-sided")
        ks_stat, ks_p = stats.ks_2samp(x, y)

        print("Welch t-test:", t_stat, "p=", t_p)
        print("Mann–Whitney:", u_stat, "p=", u_p)
        print("KS-test:", ks_stat, "p=", ks_p)
    else:
        print("SciPy не установлен — статистические тесты пропущены")


# =========================
# Графики
# =========================

def plot_cdf(runs, latencies_by_run):
    plt.figure()

    for name, xs in latencies_by_run.items():
        xs = np.sort(xs)
        ys = np.arange(1, len(xs) + 1) / len(xs)
        plt.plot(xs, ys, label=name)

    plt.xlabel("Latency (ms)")
    plt.ylabel("CDF")
    plt.legend()
    plt.title("Latency CDF comparison")
    plt.show()


def plot_boxplot(latencies_by_run):
    plt.figure()

    data = list(latencies_by_run.values())
    labels = list(latencies_by_run.keys())

    plt.boxplot(data, labels=labels)
    plt.ylabel("Latency (ms)")
    plt.title("Latency distribution (boxplot)")
    plt.xticks(rotation=45)
    plt.show()


def plot_tail_comparison(latencies_by_run):
    plt.figure()

    labels = []
    p95_vals = []
    p99_vals = []

    for name, xs in latencies_by_run.items():
        labels.append(name)
        p95_vals.append(np.percentile(xs, 95))
        p99_vals.append(np.percentile(xs, 99))

    x = np.arange(len(labels))

    plt.bar(x - 0.2, p95_vals, width=0.4)
    plt.bar(x + 0.2, p99_vals, width=0.4)

    plt.xticks(x, labels, rotation=45)
    plt.ylabel("Latency (ms)")
    plt.title("Tail latency comparison (p95 vs p99)")
    plt.show()


# =========================
# Work Amplification
# =========================

def extract_work_amplification(run):
    replication = run["summary"].get("replication", {})
    req = replication.get("avg_requested_r", 1.0)
    eff = replication.get("avg_effective_r", 1.0)

    if req == 0:
        return 1.0

    return eff / req


# =========================
# MAIN
# =========================

def main():
    folder = "./experiments"  # поменяйте путь при необходимости

    runs = load_runs(folder)

    if not runs:
        print("Нет JSON-файлов")
        return

    latencies_by_run = {}
    stats_by_run = {}

    for run in runs:
        name = run["run_id"]
        xs = extract_latencies(run)

        latencies_by_run[name] = xs
        stats_by_run[name] = descriptive_stats(xs)

    # Вывод описательной статистики
    print("\n=== DESCRIPTIVE STATISTICS ===")
    for name, s in stats_by_run.items():
        print(f"\n{name}")
        for k, v in s.items():
            print(f"{k}: {v}")

    # Попарные сравнения
    names = list(latencies_by_run.keys())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            compare_two(
                names[i],
                latencies_by_run[names[i]],
                names[j],
                latencies_by_run[names[j]],
            )

    # Графики
    plot_cdf(runs, latencies_by_run)
    plot_boxplot(latencies_by_run)
    plot_tail_comparison(latencies_by_run)


if __name__ == "__main__":
    main()
