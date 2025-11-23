import argparse
import json
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


SUMMARY_GLOB = "peer*-summary.json"
RUN_GLOB = "run-*"


def load_runs(base_dir: Path, uc_label: str) -> pd.DataFrame:
    rows = []
    run_dirs = sorted([p for p in base_dir.glob(RUN_GLOB) if p.is_dir()])

    for run_dir in run_dirs:
        run_id = run_dir.name
        files = sorted(run_dir.glob(SUMMARY_GLOB))
        if not files:
            continue

        for f in files:
            with open(f, "r") as fh:
                s = json.load(fh)

            s["peer"] = f.stem.replace("-summary", "")
            s["uc"] = uc_label
            s["run"] = run_id
            rows.append(s)

    if not rows:
        raise RuntimeError(f"No summary files in {base_dir}")

    return pd.DataFrame(rows)


def per_run_means(peer_df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    return (
        peer_df
        .groupby(["uc", "run"], as_index=False)[cols]
        .mean(numeric_only=True)
    )


def barplot_metric(run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path):
    stat = run_df.groupby("uc")[column].agg(["mean", "std"])

    fig, ax = plt.subplots()
    x = range(len(stat.index))
    means = stat["mean"].values
    stds = stat["std"].fillna(0).values

    ax.bar(x, means, yerr=stds, capsize=4)
    ax.set_xticks(list(x))
    ax.set_xticklabels(stat.index.tolist(), rotation=15)
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def plot_latency_quantiles(run_df: pd.DataFrame, out: Path):
    lat_cols = ["lat_p50", "lat_p90", "lat_p99", "lat_max"]
    stat = run_df.groupby("uc")[lat_cols].agg(["mean", "std"])

    fig, ax = plt.subplots()
    x_base = list(range(len(lat_cols)))
    width = 0.25

    ucs = stat.index.tolist()

    for i, uc in enumerate(ucs):
        means = stat.loc[uc, (slice(None), "mean")].values.astype(float)
        stds = stat.loc[uc, (slice(None), "std")].fillna(0).values.astype(float)

        ax.bar(
            [x + i * width for x in x_base],
            means,
            width=width,
            yerr=stds,
            capsize=3,
            label=uc
        )

    ax.set_xticks([x + width for x in x_base])
    ax.set_xticklabels(lat_cols)
    ax.set_ylabel("latency (ms)")
    ax.set_title("Latency Quantiles (mean ± std across runs)")
    ax.legend()

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--uc",
        nargs="+",
        metavar="LABEL:PATH",
        help="list of UC definitions, e.g. UC1:logs/uc1-direct UC2:logs/uc2-relay UC3:logs/uc3-relay-degraded",
    )
    ap.add_argument("--out", default="docs/metrics", help="Output directory")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # --------------------------------------------
    # 1) Load all UCs dynamically
    # --------------------------------------------
    all_peers = []
    for entry in args.uc:
        label, path = entry.split(":", 1)
        df = load_runs(Path(path), label)
        all_peers.append(df)

    peer_df = pd.concat(all_peers, ignore_index=True)

    # All numerical metrics
    metrics = [
        "delivery_rate", "duplicate_rate",
        "lat_p50", "lat_p90", "lat_p99", "lat_max",
        "convergence_time_ms",
        "pr_avg_ratio",
        "rt_avg_ms", "rt_p50_ms", "rt_p90_ms", "rt_max_ms"
    ]

    for c in metrics:
        peer_df[c] = pd.to_numeric(peer_df[c], errors="coerce")

    # --------------------------------------------
    # 2) Compute one row per run
    # --------------------------------------------
    run_df = per_run_means(peer_df, metrics)
    run_df.to_csv(out_dir / "per_run_means.csv", index=False)

    # Convert CT to seconds
    run_df["convergence_time_s"] = run_df["convergence_time_ms"] / 1000.0

    # --------------------------------------------
    # 3) Plots
    # --------------------------------------------
    barplot_metric(
        run_df, "delivery_rate",
        "Delivery Rate (mean ± std)", "delivery rate",
        out_dir / "delivery_rate.png"
    )

    barplot_metric(
        run_df, "duplicate_rate",
        "Duplicate Rate (mean ± std)", "duplicate rate",
        out_dir / "duplicate_rate.png"
    )

    plot_latency_quantiles(run_df, out_dir / "latency_quantiles.png")

    barplot_metric(
        run_df, "convergence_time_s",
        "Convergence Time (mean ± std)", "CT (seconds)",
        out_dir / "convergence_time.png"
    )

    barplot_metric(
        run_df, "pr_avg_ratio",
        "Peer Reachability (mean ± std)", "PR avg ratio",
        out_dir / "peer_reachability.png"
    )

    # --------------------------------------------
    # 4) Export final summary table
    # --------------------------------------------
    summary = run_df.groupby("uc")[metrics].agg(["mean", "std", "min", "max"])
    summary.to_csv(out_dir / "summary_all_uc.csv")

    print("[OK] All plots + tables written to", out_dir.resolve())


if __name__ == "__main__":
    main()
