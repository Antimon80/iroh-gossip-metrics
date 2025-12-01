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
    """Return True if the sender logged joined:true in send.jsonl, else False."""
    send_log = run_dir / "send.jsonl"
    if not send_log.exists():
        return False

    try:
        with send_log.open() as fh:
            for line in fh:
                if '"event":"setup"' in line:
                    try:
                        obj = json.loads(line)
                        return bool(obj.get("extra", {}).get("joined", False))
                    except json.JSONDecodeError:
                        return False
    except OSError:
        return False

    return False


# ---------------------------------------------------------------------------
# JSON helper
# ---------------------------------------------------------------------------
def load_summary_file(path: Path):
    """Extract the first JSON object from a summary file."""
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
    rows = []
    run_dirs = sorted([p for p in base_dir.glob(RUN_GLOB) if p.is_dir()])

    for run_dir in run_dirs:
        run_id = run_dir.name

        # Sender join-state once per run
        sender_ok = sender_joined(run_dir)

        files = sorted(run_dir.glob(SUMMARY_GLOB))
        if not files:
            continue

        for f in files:
            s = load_summary_file(f)
            if s is None:
                continue

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
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
def per_run_means(peer_df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    return (
        peer_df
        .groupby(["uc", "run"], as_index=False)[cols]
        .mean(numeric_only=True)
    )


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------

def _boxplot_metric_from_runs(run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path):
    """Draw a boxplot over per-run values for each UC."""
    if column not in run_df.columns:
        return

    ucs = sorted(run_df["uc"].unique())
    data = [run_df.loc[run_df["uc"] == uc, column].dropna().values for uc in ucs]

    # Filter out UCs with no data (just in case)
    filtered_ucs = []
    filtered_data = []
    for uc, arr in zip(ucs, data):
        if len(arr) > 0:
            filtered_ucs.append(uc)
            filtered_data.append(arr)

    if not filtered_data:
        return

    fig, ax = plt.subplots()
    ax.boxplot(filtered_data, labels=filtered_ucs)
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def barplot_metric(run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path):
    """
    Plot mean ± std as bar plot.
    If the coefficient of variation (std / |mean|) is > 1.0 for any UC,
    fall back to a boxplot over the per-run values instead.
    """
    if column not in run_df.columns:
        return

    stat = run_df.groupby("uc")[column].agg(["mean", "std"]).dropna(how="all")
    if stat.empty:
        return

    # Detect "too large" variance → use boxplot instead
    # Avoid division by zero: only consider rows with non-zero mean
    nonzero = stat["mean"].abs() > 0
    high_variance = pd.Series(False, index=stat.index)
    high_variance[nonzero] = (stat.loc[nonzero, "std"] / stat.loc[nonzero, "mean"].abs()) > 1.0

    if high_variance.any():
        # Fallback: boxplot over per-run data
        _boxplot_metric_from_runs(run_df, column, title, ylabel, out)
        return

    # Normal case: bar plot mean ± std
    fig, ax = plt.subplots()
    x = range(len(stat.index))
    ax.bar(x, stat["mean"].values, yerr=stat["std"].fillna(0).values, capsize=4)
    ax.set_xticks(list(x))
    ax.set_xticklabels(stat.index.tolist(), rotation=15)
    ax.set_ylabel(ylabel)
    ax.set_title(title)

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def _plot_quantiles_generic(run_df, cols, title, ylabel, out):
    present_cols = [c for c in cols if c in run_df.columns]
    if not present_cols:
        return

    stat = run_df.groupby("uc")[present_cols].agg(["mean", "std"])
    if stat.empty:
        return

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


def plot_latency_quantiles(run_df, out):
    cols = ["lat_p50", "lat_p90", "lat_p99", "lat_max"]
    _plot_quantiles_generic(run_df, cols, "Latency Quantiles", "latency (ms)", out)


def plot_ldh_quantiles(run_df, out):
    cols = ["ldh_p50", "ldh_p90", "ldh_p99", "ldh_max"]
    _plot_quantiles_generic(run_df, cols, "LDH Quantiles", "overlay hops", out)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uc", nargs="+", required=True,
                    help="Use-cases (e.g. uc1 uc2). Mapped to logs/<uc>/ ...")
    ap.add_argument("--peers", nargs="*", type=int,
                    help="Optional peer counts matching subfolders p<PEERS> under logs/<uc>/")
    ap.add_argument("--out", default="docs/metrics")
    args = ap.parse_args()

    # Build UC specs (label, short-name, path)
    uc_specs = []

    for entry in args.uc:
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
            for p in args.peers:
                sub_label = f"{base_label}_P{p}"
                sub_name = f"{base_name}_p{p}"
                sub_path = base_path / f"p{p}"
                uc_specs.append((sub_label, sub_name, sub_path))
        else:
            uc_specs.append((base_label, base_name, base_path))

    combo_name = "_".join(sorted({name for _, name, _ in uc_specs}))
    out_dir = Path(args.out) / combo_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load all UCs → peer-level DataFrame
    all_peers = []
    for uc_label, _, uc_path in uc_specs:
        df = load_runs(uc_path, uc_label)
        all_peers.append(df)
    peer_df = pd.concat(all_peers, ignore_index=True)

    # Sender-Fails pro Run/UC
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

    # Numeric metrics (inkl. sender_joined → wird 0/1)
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
    ]

    peer_df = ensure_numeric(peer_df, metrics)
    peer_df.to_csv(out_dir / "per_peer_raw.csv", index=False)

    run_df = per_run_means(peer_df, metrics)
    run_df.to_csv(out_dir / "per_run_means.csv", index=False)

    if "convergence_time_ms" in run_df.columns:
        run_df["convergence_time_s"] = run_df["convergence_time_ms"] / 1000.0

    # Plots
    barplot_metric(run_df, "delivery_rate", "Delivery Rate", "rate",
                   out_dir / "delivery_rate.png")
    barplot_metric(run_df, "duplicate_rate", "Duplicate Rate", "duplicates",
                   out_dir / "duplicate_rate.png")
    plot_latency_quantiles(run_df, out_dir / "latency_quantiles.png")
    plot_ldh_quantiles(run_df, out_dir / "ldh_quantiles.png")
    barplot_metric(run_df, "convergence_time_s", "Convergence Time", "seconds",
                   out_dir / "convergence_time.png")
    barplot_metric(run_df, "pr_avg_ratio", "Peer Reachability", "ratio",
                   out_dir / "peer_reachability.png")
    barplot_metric(run_df, "rt_avg_ms", "Reconnect Time", "ms",
                   out_dir / "reconnect_time_avg.png")
    barplot_metric(run_df, "reconnect_samples", "Reconnect Samples", "count",
                   out_dir / "reconnect_samples.png")

    if "sender_joined" in run_df.columns:
        run_df = run_df.copy()
        run_df["sender_fail"] = 1.0 - run_df["sender_joined"]
        barplot_metric(
            run_df,
            "sender_fail",
            "Sender Join Failure Rate (per run, mean ± std)",
            "fail rate",
            out_dir / "sender_fail_rate.png",
        )

    print("[OK] All metrics written to", out_dir.resolve())


if __name__ == "__main__":
    main()
