#!/usr/bin/env python3
"""
Post-processing script for iroh-gossip-metrics benchmark summaries.

- Loads multiple use-cases (UCs), each with many runs and peers.
- Aggregates metrics per run (mean across peers).
- Produces CSV tables and a set of comparison plots across UCs.

Usage examples:
    python3 scripts/analyze.py --uc uc1 --peers 10 20
    python3 scripts/analyze.py --uc uc1 uc2 --peers 10

Output goes to:
    docs/metrics/<combo_name>/
where combo_name encodes uc + peers, e.g. uc1_p10_uc1_p20
"""

import argparse
import json
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


SUMMARY_GLOB = "peer*-summary.json"
RUN_GLOB = "run-*"


# ---------------------------------------------------------------------------
# Detect sender join state
# ---------------------------------------------------------------------------
def sender_joined(run_dir: Path) -> bool:
    """
    Inspect the sender's log file for the current run and check whether the
    sender reported a successful join (joined=true) during the setup phase.

    This is used to classify runs where the sender itself never joined the mesh.
    """
    send_log = run_dir / "send.jsonl"
    if not send_log.exists():
        # Missing log → assume sender did not join
        return False

    try:
        with send_log.open() as fh:
            for line in fh:
                # We only care about the setup event
                if '"event":"setup"' in line:
                    try:
                        obj = json.loads(line)
                        return bool(obj.get("extra", {}).get("joined", False))
                    except json.JSONDecodeError:
                        # Malformed JSON → treat as not joined
                        return False
    except OSError:
        # Any I/O error → treat as not joined
        return False

    return False


# ---------------------------------------------------------------------------
# JSON helper
# ---------------------------------------------------------------------------
def load_summary_file(path: Path):
    """
    Extract the first JSON object from a summary file.

    Some summary files may contain extra logs or noise before/after the JSON
    object; we scan for the first '{' and try to decode from there.
    """
    text = path.read_text(errors="ignore")
    if not text.strip():
        print(f"[warn] Empty summary file: {path}, skipping")
        return None

    dec = json.JSONDecoder()
    start = text.find("{")
    if start == -1:
        print(f"[warn] No JSON object found in {path}, skipping")
        return None

    try:
        obj, _ = dec.raw_decode(text[start:])
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        print(f"[warn] JSON decode error in {path}, skipping")
        return None


# ---------------------------------------------------------------------------
# Load runs, attach sender_joined
# ---------------------------------------------------------------------------
def load_runs(base_dir: Path, uc_label: str) -> pd.DataFrame:
    """
    Load all peer summary files for all runs under a base directory.

    For each run directory:
      - determine whether the sender joined successfully,
      - parse each peer*-summary.json file,
      - attach metadata: peer id, UC label, run id, sender_joined flag.

    Returns a DataFrame with one row per (run, peer).
    """
    rows = []
    run_dirs = sorted([p for p in base_dir.glob(RUN_GLOB) if p.is_dir()])

    for run_dir in run_dirs:
        run_id = run_dir.name

        # Sender join-state (same for all peers of this run)
        sender_ok = sender_joined(run_dir)

        files = sorted(run_dir.glob(SUMMARY_GLOB))
        if not files:
            continue

        for f in files:
            s = load_summary_file(f)
            if s is None:
                continue

            # Derive a short peer identifier from the file name
            s["peer"] = f.stem.replace("-summary", "")
            s["uc"] = uc_label
            s["run"] = run_id
            s["sender_joined"] = sender_ok

            rows.append(s)

    if not rows:
        raise RuntimeError(f"No usable summary files under {base_dir}")

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Numeric conversion helper
# ---------------------------------------------------------------------------
def ensure_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """
    Convert a set of columns to numeric dtype, coercing invalid entries to NaN.

    This is important because JSON decoding may produce strings for numeric
    fields, and missing values should not break aggregations.
    """
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def per_run_means(peer_df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Aggregate peer-level metrics to one row per (UC, run).

    For each UC and run id, we compute the mean across all peers for the given
    metric columns. This gives a per-run view that can be used for statistics
    and plotting.
    """
    return (
        peer_df
        .groupby(["uc", "run"], as_index=False)[cols]
        .mean(numeric_only=True)
    )


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------
def barplot_metric(run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path):
    """
    Bar plot with one bar per UC, each with its own color.
    Shows mean ± std error bars.
    Legend is omitted because UC labels are already shown on the x-axis.
    """

    if column not in run_df.columns:
        return

    stat = run_df.groupby("uc")[column].agg(["mean", "std"]).dropna(how="all")
    if stat.empty:
        return

    ucs = stat.index.tolist()
    means = stat["mean"].values
    stds = stat["std"].fillna(0).values

    # Matplotlib colormap (tab10 gives 10 visually distinct colors)
    cmap = plt.get_cmap("tab10")
    colors = [cmap(i % 10) for i in range(len(ucs))]

    fig, ax = plt.subplots(figsize=(8, 4))
    x = range(len(ucs))

    # Draw one bar per UC with different color
    for i, uc in enumerate(ucs):
        ax.bar(
            i,
            means[i],
            yerr=stds[i],
            color=colors[i],
            capsize=4,
        )

    ax.set_xticks(list(x))
    ax.set_xticklabels(ucs, rotation=15)
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    # No legend here (UC names already shown)
    # ax.legend()  <-- removed

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def scatter_metric(run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path):
    """
    Produce a scatter plot of per-run values for a single metric.

    X-axis: integer UC index (with small horizontal jitter per run).
    Y-axis: metric value.

    This is useful when the distribution per UC is highly variable and we
    explicitly want to see each individual run as a point.
    """
    if column not in run_df.columns:
        return

    df = run_df[["uc", column]].dropna()
    if df.empty:
        return

    ucs = sorted(df["uc"].unique())

    fig, ax = plt.subplots()

    for i, uc in enumerate(ucs):
        ys = df.loc[df["uc"] == uc, column].values
        n = len(ys)
        if n == 0:
            continue
        # Simple deterministic jitter around the UC index
        xs = [i + (j - (n - 1) / 2) * 0.03 for j in range(n)]
        ax.scatter(xs, ys, label=uc)

    ax.set_xticks(list(range(len(ucs))))
    ax.set_xticklabels(ucs, rotation=15)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def _plot_quantiles_generic(run_df: pd.DataFrame,
                            cols: list[str],
                            title: str,
                            ylabel: str,
                            out: Path):
    """
    Helper to plot multiple quantile metrics side-by-side for each UC.

    For each UC and each metric in 'cols', we compute mean ± std over runs
    and draw grouped bars. Groups are the metrics in 'cols' (e.g. p50/p90/...),
    each group containing one bar per UC.
    """
    present_cols = [c for c in cols if c in run_df.columns]
    if not present_cols:
        return

    stat = run_df.groupby("uc")[present_cols].agg(["mean", "std"])
    if stat.empty:
        return

    ucs = stat.index.tolist()
    n_ucs = len(ucs)
    n_metrics = len(present_cols)

    # Base x positions for the groups (one group per metric)
    x_base = list(range(n_metrics))

    # Total width allocated for each group on the x-axis
    total_width = 0.8
    # Width of a single bar within the group, depending on number of UCs
    width = total_width / max(n_ucs, 1)

    fig, ax = plt.subplots()

    for i, uc in enumerate(ucs):
        # Means and stds in the same order as present_cols
        means = stat.loc[uc, (slice(None), "mean")].values.astype(float)
        stds = stat.loc[uc, (slice(None), "std")].fillna(0).values.astype(float)

        # Center bars around the group position: e.g. for 3 UCs, offsets are
        # -width, 0, +width; for 5 UCs they are symmetric around 0, etc.
        offsets = [x + (i - (n_ucs - 1) / 2) * width for x in x_base]

        ax.bar(
            offsets,
            means,
            width=width * 0.9,  # small shrink to leave a tiny gap between bars
            yerr=stds,
            capsize=3,
            label=uc,
        )

    ax.set_xticks(x_base)
    ax.set_xticklabels(present_cols, rotation=15)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend()

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def plot_latency_quantiles(run_df: pd.DataFrame, out: Path):
    """
    Plot latency quantiles (p50, p90, p99, max) as grouped bar plot.

    Each group corresponds to a quantile, and within each group we draw one
    bar per UC (mean ± std over runs).
    """
    cols = ["lat_p50", "lat_p90", "lat_p99", "lat_max"]
    _plot_quantiles_generic(run_df, cols, "Latency Quantiles", "latency (ms)", out)


def plot_ldh_quantiles(run_df: pd.DataFrame, out: Path):
    """
    Plot overlay hop (LDH) quantiles (p50, p90, p99, max) as grouped bar plot.
    """
    cols = ["ldh_p50", "ldh_p90", "ldh_p99", "ldh_max"]
    _plot_quantiles_generic(run_df, cols, "LDH Quantiles", "overlay hops", out)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    """
    Command-line entry point.

    - Parses CLI arguments.
    - Resolves UC labels to log directories (optionally with peer-count subdirs).
    - Loads all runs and peers into a single DataFrame.
    - Computes per-run aggregates and writes CSV files.
    - Produces a fixed set of plots for selected metrics.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--uc", nargs="+", required=True,
                    help="Use-cases (e.g. uc1 uc2). Mapped to logs/<uc>/ ...")
    ap.add_argument("--peers", nargs="*", type=int,
                    help="Optional peer counts matching subfolders p<PEERS> under logs/<uc>/")
    ap.add_argument("--out", default="docs/metrics",
                    help="Output directory root for generated CSV and plots")
    args = ap.parse_args()

    # Build UC specs: (label used in plots, name used in combo_name, base path)
    uc_specs: list[tuple[str, str, Path]] = []

    for entry in args.uc:
        # Allow custom path via "LABEL:/path/to/logs"
        if ":" in entry:
            label, path = entry.split(":", 1)
            base_label = label.upper()
            base_name = label.lower()
            base_path = Path(path)
        else:
            base_label = entry.upper()
            base_name = entry.lower()
            base_path = Path(f"logs/{base_name}")

        if args.peers:
            # If peer counts are given, expect subdirectories p<PEERS>
            for p in args.peers:
                sub_label = f"{base_label}_P{p}"
                sub_name = f"{base_name}_p{p}"
                sub_path = base_path / f"p{p}"
                uc_specs.append((sub_label, sub_name, sub_path))
        else:
            uc_specs.append((base_label, base_name, base_path))

    # Combine all UC names into one folder name for this analysis run
    combo_name = "_".join(sorted({name for _, name, _ in uc_specs}))
    out_dir = Path(args.out) / combo_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load all UCs into a single peer-level DataFrame
    all_peers = []
    for uc_label, _, uc_path in uc_specs:
        df = load_runs(uc_path, uc_label)
        all_peers.append(df)
    peer_df = pd.concat(all_peers, ignore_index=True)

    # Sender-fail rates per UC: fraction of runs where sender_joined==False
    fail_series = (
        peer_df
        .groupby(["uc", "run"])["sender_joined"]
        .first()
        .eq(False)
    )
    fail_rates = fail_series.groupby("uc").mean()

    fail_df = (fail_rates * 100).round(2).reset_index()
    fail_df.columns = ["uc", "sender_fail_rate_percent"]
    fail_df.to_csv(out_dir / "sender_fail_rates.csv", index=False)

    # List of metrics expected in the summaries.
    # Boolean flags like joined/saw_test/sender_joined are treated as 0/1.
    metrics = [
        "delivery_rate", "duplicate_rate", "received_unique", "recv_total",
        "total_expected", "duplicates", "out_of_order",
        "lat_min", "lat_p50", "lat_p90", "lat_p99", "lat_max",
        "ldh_min", "ldh_p50", "ldh_p90", "ldh_p99", "ldh_max",
        "convergence_time_ms", "pr_avg_ratio",
        "rt_avg_ms", "rt_p50_ms", "rt_p90_ms", "rt_max_ms",
        "disconnect_events", "reconnect_events", "reconnect_samples",
        "join_wait_ms",
        "sender_joined",
        "joined",
        "saw_test",
    ]

    # Ensure numeric typing for all metrics and write raw peer-level CSV
    peer_df = ensure_numeric(peer_df, metrics)
    peer_df.to_csv(out_dir / "per_peer_raw.csv", index=False)

    # Compute per-run means across peers
    run_df = per_run_means(peer_df, metrics)

    # Convenience: also expose convergence time in seconds
    if "convergence_time_ms" in run_df.columns:
        run_df["convergence_time_s"] = run_df["convergence_time_ms"] / 1000.0

    # -----------------------------------------------------------------------
    # Special handling for "critical" percentage / ratio metrics
    # -----------------------------------------------------------------------

    # Delivery rate only over receivers that actually joined the mesh.
    # This ignores peers with joined==0 when computing per-run delivery rates.
    if {"joined", "delivery_rate"}.issubset(peer_df.columns):
        delivery_joined = (
            peer_df[peer_df["joined"] == 1]
            .groupby(["uc", "run"], as_index=False)["delivery_rate"]
            .mean()
            .rename(columns={"delivery_rate": "delivery_rate_joined_only"})
        )
        # Per-run CSV for joined-only delivery rate
        delivery_joined.to_csv(
            out_dir / "delivery_rate_joined_only_per_run.csv",
            index=False,
        )
        # Attach the joined-only metric to the run-level dataframe
        run_df = run_df.merge(delivery_joined, on=["uc", "run"], how="left")
    
    # Reconnect samples: average over all peers that joined the mesh.
    # Peers with joined == 0 are excluded; joined peers with 0 samples are included.
    if {"joined", "reconnect_samples"}.issubset(peer_df.columns):
        rs_joined = (
            peer_df[peer_df["joined"] == 1]
            .groupby(["uc", "run"], as_index=False)["reconnect_samples"]
            .mean()
            .rename(columns={"reconnect_samples": "reconnect_samples_joined_only"})
        )
        rs_joined.to_csv(
            out_dir / "reconnect_samples_joined_only_per_run.csv",
            index=False,
        )
        run_df = run_df.merge(rs_joined, on=["uc", "run"], how="left")
    
    # Join wait time: only for peers that successfully joined.
    if {"joined", "join_wait_ms"}.issubset(peer_df.columns):
        jw_joined = (
            peer_df[peer_df["joined"] == 1]
            .groupby(["uc", "run"], as_index=False)["join_wait_ms"]
            .mean()
            .rename(columns={"join_wait_ms": "join_wait_ms_joined_only"})
        )

        # Write per-run table
        jw_joined.to_csv(
            out_dir / "join_wait_ms_joined_only_per_run.csv",
            index=False,
        )

        # Add to run_df
        run_df = run_df.merge(jw_joined, on=["uc", "run"], how="left")

        # UC-level summary
        jw_summary = jw_joined.groupby("uc")["join_wait_ms_joined_only"].agg(
            total_runs="count",
            mean_join_wait_ms="mean",
            std_join_wait_ms="std",
            min_ms="min",
            max_ms="max",
        ).reset_index()

        jw_summary.to_csv(
            out_dir / "join_wait_ms_joined_only_uc_summary.csv",
            index=False,
        )

    # Per-run CSV for reconnect time
    if "rt_avg_ms" in run_df.columns:
        reconnect_per_run = run_df[["uc", "run", "rt_avg_ms"]]
        reconnect_per_run.to_csv(out_dir / "reconnect_time_per_run.csv", index=False)

    # Per-run CSV for peer reachability ratio (all runs)
    if "pr_avg_ratio" in run_df.columns:
        pr_per_run = run_df[["uc", "run", "pr_avg_ratio"]]
        pr_per_run.to_csv(out_dir / "peer_reachability_per_run.csv", index=False)

    # Filter to valid runs where the sender actually joined.
    # These are the runs we want to use for receiver ratios.
    valid_run_df = run_df[run_df["sender_joined"] == 1].copy()

    # Per-run CSV for receiver ratios (joined / saw_test), valid runs only
    if {"joined", "saw_test"}.issubset(valid_run_df.columns):
        receiver_ratios_valid = valid_run_df[["uc", "run", "joined", "saw_test"]].rename(
            columns={"joined": "joined_ratio", "saw_test": "saw_test_ratio"}
        )
        receiver_ratios_valid.to_csv(
            out_dir / "receiver_ratios_per_run.csv",
            index=False,
        )

        # UC-level summary table for receiver ratios
        runs_all = run_df.groupby("uc")["run"].nunique().rename("total_runs")
        runs_valid = valid_run_df.groupby("uc")["run"].nunique().rename("valid_runs")

        summary = receiver_ratios_valid.groupby("uc").agg(
            avg_joined_ratio=("joined_ratio", "mean"),
            std_joined_ratio=("joined_ratio", "std"),
            min_joined_ratio=("joined_ratio", "min"),
            max_joined_ratio=("joined_ratio", "max"),
            avg_saw_test_ratio=("saw_test_ratio", "mean"),
            std_saw_test_ratio=("saw_test_ratio", "std"),
            min_saw_test_ratio=("saw_test_ratio", "min"),
            max_saw_test_ratio=("saw_test_ratio", "max"),
            runs_with_full_join=("joined_ratio", lambda s: (s == 1.0).sum()),
            runs_with_any_miss=("joined_ratio", lambda s: (s < 1.0).sum()),
        )

        summary = (
            summary
            .join(runs_all, how="left")
            .join(runs_valid, how="left")
            .reset_index()
        )

        summary.to_csv(out_dir / "receiver_ratios_uc_summary.csv", index=False)
    
    # UC-level summary for delivery rate (joined receivers only, valid runs only).
    if "delivery_rate_joined_only" in run_df.columns:
        # Restrict to runs where the sender actually joined and the metric is present
        dr_valid = valid_run_df[["uc", "run", "delivery_rate_joined_only"]].dropna()

        dr_summary = dr_valid.groupby("uc")["delivery_rate_joined_only"].agg(
            total_runs="count",
            mean_delivery_rate="mean",
            std_delivery_rate="std",
            min_rate="min",
            max_rate="max",
        ).reset_index()

        dr_summary.to_csv(
            out_dir / "delivery_rate_joined_only_uc_summary.csv",
            index=False,
        )
    
    # Convergence time tables
    if "convergence_time_s" in run_df.columns:
        # 1) Per-run table
        conv_per_run = run_df[["uc", "run", "convergence_time_ms", "convergence_time_s"]]
        conv_per_run.to_csv(out_dir / "convergence_time_per_run.csv", index=False)

        # 2) UC-level summary
        conv_summary = run_df.groupby("uc")["convergence_time_s"].agg(
            total_runs="count",
            mean_convergence_s="mean",
            std_convergence_s="std",
            min_s="min",
            max_s="max"
        ).reset_index()

        conv_summary.to_csv(out_dir / "convergence_time_uc_summary.csv", index=False)


    # Write the full per-run metrics CSV (including derived columns)
    run_df.to_csv(out_dir / "per_run_means.csv", index=False)

    # -----------------------------------------------------------------------
    # Plots for selected metrics
    # -----------------------------------------------------------------------

    # Delivery rate: prefer the joined-only version if available.
    delivery_column = (
        "delivery_rate_joined_only"
        if "delivery_rate_joined_only" in run_df.columns
        else "delivery_rate"
    )
    barplot_metric(
        run_df,
        delivery_column,
        "Delivery Rate (joined receivers only)" if delivery_column == "delivery_rate_joined_only" else "Delivery Rate",
        "rate",
        out_dir / "delivery_rate.png",
    )

    # Duplicate rate: simple bar plot
    barplot_metric(run_df, "duplicate_rate", "Duplicate Rate", "duplicates",
                   out_dir / "duplicate_rate.png")

    # Latency and LDH quantiles: grouped bar plots
    plot_latency_quantiles(run_df, out_dir / "latency_quantiles.png")
    plot_ldh_quantiles(run_df, out_dir / "ldh_quantiles.png")

    # Convergence time: bar plot in seconds
    barplot_metric(run_df, "convergence_time_s", "Convergence Time", "seconds",
                   out_dir / "convergence_time.png")

    # Peer reachability ratio: bar plot
    barplot_metric(run_df, "pr_avg_ratio", "Peer Reachability", "ratio",
                   out_dir / "peer_reachability.png")

    # Reconnect time: always a scatterplot over per-run values
    scatter_metric(run_df, "rt_avg_ms", "Reconnect Time per Run", "ms",
                   out_dir / "reconnect_time_scatter.png")

    # Number of reconnect samples: bar plot.
    # Prefer the joined-only version (joined==1 and reconnect_samples>0).
    rs_column = (
        "reconnect_samples_joined_only"
        if "reconnect_samples_joined_only" in run_df.columns
        else "reconnect_samples"
    )
    barplot_metric(
        run_df,
        rs_column,
        "Reconnect Samples (joined peers only)"
        if rs_column == "reconnect_samples_joined_only"
        else "Reconnect Samples",
        "count",
        out_dir / "reconnect_samples.png",
    )

    # Receiver ratios (joined / saw_test): scatterplots over valid runs,
    # all UCs in a single plot (Variant A).
    scatter_metric(
        valid_run_df,
        "joined",
        "Receiver Join Ratio (valid runs)",
        "fraction of peers",
        out_dir / "receiver_join_ratio_scatter.png",
    )
    scatter_metric(
        valid_run_df,
        "saw_test",
        "Receiver Saw-Test Ratio (valid runs)",
        "fraction of peers",
        out_dir / "receiver_saw_test_ratio_scatter.png",
    )
    scatter_metric(
        run_df,
        "join_wait_ms_joined_only",
        "Join Wait Time (joined peers only)",
        "ms",
        out_dir / "join_wait_ms_scatter.png",
    )

    print("[OK] All metrics written to", out_dir.resolve())


if __name__ == "__main__":
    main()
