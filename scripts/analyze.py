"""
Post-processing script for iroh-gossip-metrics benchmark summaries.

- Loads multiple use-cases (UCs), each with many runs and peers.
- Aggregates metrics per run (mean across peers).
- Produces CSV tables and a set of comparison plots across UCs.

Run, e.g.:
    python3 scripts/analyze.py --uc uc1 uc2 uc3 uc4

This will write results to:
    docs/metrics/uc1_uc2_uc3_uc4/
"""

import argparse
import json
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


SUMMARY_GLOB = "peer*-summary.json"
RUN_GLOB = "run-*"


def load_summary_file(path: Path):
    """Extract the first JSON object from a summary file.

    The file may contain:
      - exactly one pretty-printed JSON object (normal case), plus
      - arbitrary log lines or ANSI escape codes *after* the JSON.

    We use json.JSONDecoder().raw_decode(...) to parse only the first
    JSON object and ignore everything that follows.
    """
    text = path.read_text(errors="ignore")
    if not text.strip():
        print(f"[warn] Empty summary file: {path}, skipping this peer")
        return None

    dec = json.JSONDecoder()

    # Falls vor dem JSON noch Log-Zeug steht, zum ersten '{' springen
    start = text.find("{")
    if start == -1:
        print(f"[warn] No '{{' found in {path}, skipping this peer")
        return None

    try:
        obj, end_idx = dec.raw_decode(text[start:])
    except json.JSONDecodeError as e:
        print(f"[warn] JSON decode error in {path}: {e}; skipping this peer")
        return None

    if not isinstance(obj, dict):
        print(f"[warn] Top-level JSON in {path} is not an object, skipping")
        return None

    # Optional: Wenn noch was danach steht, ignorieren wir es einfach
    # rest = text[start + end_idx:].strip()
    # if rest:
    #     print(f"[info] Ignoring trailing non-JSON content in {path}")

    return obj


def load_runs(base_dir: Path, uc_label: str) -> pd.DataFrame:
    """Load all peer summary JSON files for all runs under one UC."""
    rows = []
    run_dirs = sorted([p for p in base_dir.glob(RUN_GLOB) if p.is_dir()])

    for run_dir in run_dirs:
        run_id = run_dir.name
        files = sorted(run_dir.glob(SUMMARY_GLOB))
        if not files:
            continue

        for f in files:
            s = load_summary_file(f)
            if s is None:
                # kaputtes/leerens File -> Peer ignorieren
                continue

            s["peer"] = f.stem.replace("-summary", "")
            s["uc"] = uc_label
            s["run"] = run_id
            rows.append(s)

    if not rows:
        raise RuntimeError(f"No usable summary files in {base_dir}")

    return pd.DataFrame(rows)


def ensure_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Coerce listed columns to numeric dtype (invalid values -> NaN)."""
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def per_run_means(peer_df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Aggregate metrics per run (mean across peers)."""
    return (
        peer_df
        .groupby(["uc", "run"], as_index=False)[cols]
        .mean(numeric_only=True)
    )


def barplot_metric(run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path):
    """Create a simple bar plot (mean ± std) for one metric across UCs."""
    if column not in run_df.columns:
        print(f"[warn] Column '{column}' not in run_df, skipping plot {out.name}")
        return

    stat = run_df.groupby("uc")[column].agg(["mean", "std"])
    stat = stat.dropna(how="all")
    if stat.empty:
        print(f"[warn] No data for '{column}', skipping plot {out.name}")
        return

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


def _plot_quantiles_generic(
    run_df: pd.DataFrame,
    cols: list[str],
    title: str,
    ylabel: str,
    out: Path,
):
    """Helper to plot multiple quantile columns grouped by UC."""
    for c in cols:
        if c not in run_df.columns:
            print(f"[warn] Column '{c}' not in run_df, skipping in {out.name}")
    present_cols = [c for c in cols if c in run_df.columns]
    if not present_cols:
        print(f"[warn] No requested columns present for {out.name}, skipping.")
        return

    stat = run_df.groupby("uc")[present_cols].agg(["mean", "std"])

    fig, ax = plt.subplots()
    x_base = list(range(len(present_cols)))
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
            label=uc,
        )

    ax.set_xticks([x + width for x in x_base])
    ax.set_xticklabels(present_cols)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def plot_latency_quantiles(run_df: pd.DataFrame, out: Path):
    """Plot latency quantiles (mean ± std across runs) per UC."""
    lat_cols = ["lat_p50", "lat_p90", "lat_p99", "lat_max"]
    _plot_quantiles_generic(
        run_df,
        lat_cols,
        "Latency Quantiles (mean ± std across runs)",
        "latency (ms)",
        out,
    )


def plot_ldh_quantiles(run_df: pd.DataFrame, out: Path):
    """Plot LDH quantiles (mean ± std across runs) per UC."""
    ldh_cols = ["ldh_p50", "ldh_p90", "ldh_p99", "ldh_max"]
    _plot_quantiles_generic(
        run_df,
        ldh_cols,
        "Overlay Hop Count (LDH) Quantiles (mean ± std across runs)",
        "overlay hops",
        out,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--uc",
        nargs="+",
        metavar="UC",
        help=(
            "Use-cases to analyze. "
            "Examples: --uc uc1 uc2 uc3 "
            "Short form 'uc1' maps to logs/uc1; "
            "full form 'UC1:logs/uc1' is also accepted."
        ),
        required=True,
    )
    ap.add_argument(
        "--out",
        default="docs/metrics",
        help="Base output directory (per comparison a subdirectory is created)",
    )
    args = ap.parse_args()

    # --------------------------------------------
    # 0) Build UC specs (label, short-name, path)
    # --------------------------------------------
    uc_specs = []
    for entry in args.uc:
        if ":" in entry:
            label, path = entry.split(":", 1)
            uc_label = label  # used in plots/CSV
            uc_name = label.lower()  # used for folder naming
            uc_path = Path(path)
        else:
            uc_label = entry.upper()
            uc_name = entry.lower()
            uc_path = Path(f"logs/{uc_name}")

        uc_specs.append((uc_label, uc_name, uc_path))

    # Unterordner aus den UC-Namen, z.B. "uc1_uc2_uc3_uc4"
    combo_name = "_".join(sorted({name for _, name, _ in uc_specs}))
    out_dir = Path(args.out) / combo_name
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[info] Writing metrics to {out_dir}")

    # --------------------------------------------
    # 1) Load all UCs dynamically (peer-level data)
    # --------------------------------------------
    all_peers = []
    for uc_label, uc_name, uc_path in uc_specs:
        df = load_runs(uc_path, uc_label)
        all_peers.append(df)

    peer_df = pd.concat(all_peers, ignore_index=True)

    # --------------------------------------------
    # 2) Run-level Join-Stats: total/joined/not_joined
    # --------------------------------------------
    extra_metrics: list[str] = []
    if "joined" in peer_df.columns:
        # True/False -> 1/0 für Aggregation
        joined_numeric = peer_df["joined"].astype(int)
        peer_df = peer_df.assign(joined_numeric=joined_numeric)

        run_counts = (
            peer_df
            .groupby(["uc", "run"])
            .agg(
                total_peers=("peer", "nunique"),
                joined_peers=("joined_numeric", "sum"),
            )
            .reset_index()
        )
        run_counts["not_joined_peers"] = (
            run_counts["total_peers"] - run_counts["joined_peers"]
        )

        extra_metrics = ["total_peers", "joined_peers", "not_joined_peers"]
    else:
        run_counts = None

    # --------------------------------------------
    # 3) Ensure numeric columns (peer-level metrics)
    # --------------------------------------------
    metrics = [
        # delivery / duplicates
        "delivery_rate",
        "duplicate_rate",
        "received_unique",
        "recv_total",
        "total_expected",
        "duplicates",
        "out_of_order",
        # latency
        "lat_min",
        "lat_p50",
        "lat_p90",
        "lat_p99",
        "lat_max",
        # LDH
        "ldh_min",
        "ldh_p50",
        "ldh_p90",
        "ldh_p99",
        "ldh_max",
        # convergence time
        "convergence_time_ms",
        # peer reachability
        "pr_avg_ratio",
        # reconnect times
        "rt_avg_ms",
        "rt_p50_ms",
        "rt_p90_ms",
        "rt_max_ms",
        # startup / join
        "join_wait_ms",
    ]

    peer_df = ensure_numeric(peer_df, metrics)

    # Save raw per-peer data for further offline analysis.
    peer_df.to_csv(out_dir / "per_peer_raw.csv", index=False)

    # --------------------------------------------
    # 4) Compute one row per run (mean across peers)
    # --------------------------------------------
    run_df = per_run_means(peer_df, metrics)

    # Merge Run-Level Join-Stats, falls vorhanden
    if run_counts is not None:
        run_df = run_df.merge(run_counts, on=["uc", "run"], how="left")

    run_df.to_csv(out_dir / "per_run_means.csv", index=False)

    # Derived view: CT in seconds
    if "convergence_time_ms" in run_df.columns:
        run_df["convergence_time_s"] = run_df["convergence_time_ms"] / 1000.0

    # --------------------------------------------
    # 5) Plots
    # --------------------------------------------
    barplot_metric(
        run_df,
        "delivery_rate",
        "Delivery Rate (mean ± std)",
        "delivery rate",
        out_dir / "delivery_rate.png",
    )

    barplot_metric(
        run_df,
        "duplicate_rate",
        "Duplicate Rate (mean ± std)",
        "duplicate rate",
        out_dir / "duplicate_rate.png",
    )

    plot_latency_quantiles(run_df, out_dir / "latency_quantiles.png")
    plot_ldh_quantiles(run_df, out_dir / "ldh_quantiles.png")

    barplot_metric(
        run_df,
        "convergence_time_s",
        "Convergence Time (mean ± std)",
        "CT (seconds)",
        out_dir / "convergence_time.png",
    )

    barplot_metric(
        run_df,
        "pr_avg_ratio",
        "Peer Reachability (mean ± std)",
        "PR avg ratio",
        out_dir / "peer_reachability.png",
    )

    barplot_metric(
        run_df,
        "rt_avg_ms",
        "Reconnect Time (mean ± std)",
        "reconnect time (ms)",
        out_dir / "reconnect_time_avg.png",
    )

    # neu: nicht gejointe Peers
    if "not_joined_peers" in run_df.columns:
        barplot_metric(
            run_df,
            "not_joined_peers",
            "Non-Joined Peers (mean ± std)",
            "# peers",
            out_dir / "not_joined_peers.png",
        )

    # --------------------------------------------
    # 6) Export final summary table (per UC)
    # --------------------------------------------
    summary_cols = metrics + extra_metrics
    summary = run_df.groupby("uc")[summary_cols].agg(["mean", "std", "min", "max"])
    summary.to_csv(out_dir / "summary_all_uc.csv")

    print("[OK] All plots + tables written to", out_dir.resolve())


if __name__ == "__main__":
    main()
