#!/usr/bin/env python3
"""
Post-processing script for iroh-gossip-metrics benchmark summaries.

- Loads multiple use-cases (UCs), each with many runs and peers.
- Aggregates metrics per run (mean across peers).
- Produces CSV tables and a set of comparison plots across UCs.

Usage examples:
    python scripts/analyze.py --uc uc1 --peers 10 20
    python scripts/analyze.py --uc uc1 uc2 --peers 10
    python scripts/analyze.py --uc uc5 --peers 10 50 --churns 10 20 30

Output goes to:
    docs/metrics/<combo_name>/
where combo_name encodes uc + peers, e.g. uc1_p10_uc1_p20
"""

import argparse
import json
import re
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

# Optional label remapping for plots.
# Left side: what you pass on the CLI (uc7, uc8, ...)
# Right side: how it should appear in plots (UC1, UC3, ...)
UC_LABEL_REMAP = {
    "UC7": "UC1",
    "UC8": "UC2",
    "UC9": "UC3",
    "UC10": "UC4",
    "UC11": "UC7",
    "UC12": "UC8",
}

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
def load_runs(
    base_dir: Path, uc_label: str, churn_pct: int | None = None
) -> pd.DataFrame:
    """
    Load all peer summary files for all runs under a base directory.

    Supports both layouts:
      A) <base_dir>/run-*/peer*-summary.json
      B) <base_dir>/c<CHURN>/run-*/peer*-summary.json

    If churn_pct is None and layout (B) is used, churn_pct is inferred from
    the folder name (e.g., c10 -> 10) and written into the rows.
    """
    rows: list[dict] = []

    def _consume_run_dir(run_dir: Path, effective_churn: int | None):
        run_id = run_dir.name
        sender_ok = sender_joined(run_dir)

        files = sorted(run_dir.glob(SUMMARY_GLOB))
        if not files:
            return

        for f in files:
            s = load_summary_file(f)
            if s is None:
                continue

            s["peer"] = f.stem.replace("-summary", "")
            s["uc"] = uc_label
            s["run"] = run_id
            s["sender_joined"] = sender_ok

            if effective_churn is not None:
                s["churn_pct"] = effective_churn

            rows.append(s)

    # --- First try: direct runs under base_dir
    run_dirs = sorted([p for p in base_dir.glob(RUN_GLOB) if p.is_dir()])
    if run_dirs:
        for run_dir in run_dirs:
            _consume_run_dir(run_dir, churn_pct)
    else:
        # --- Fallback: one level deeper under c*/run-*
        churn_dirs = sorted([p for p in base_dir.glob("c*") if p.is_dir()])
        for cdir in churn_dirs:
            # infer churn percentage if not provided
            inferred = None
            if churn_pct is None:
                name = cdir.name
                if name.startswith("c"):
                    try:
                        inferred = int(name[1:])
                    except ValueError:
                        inferred = None
            else:
                inferred = churn_pct

            for run_dir in sorted([p for p in cdir.glob(RUN_GLOB) if p.is_dir()]):
                _consume_run_dir(run_dir, inferred)

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

    Only aggregates columns that actually exist in the dataframe. This keeps
    the script compatible with UCs that do not provide certain fields (e.g.,
    churn_pct for non-churn experiments)
    """
    present = [c for c in cols if c in peer_df.columns]
    if not present:
        raise RuntimeError("No metric columns found to aggregate.")
    return peer_df.groupby(["uc", "run"], as_index=False)[present].mean(
        numeric_only=True
    )


def remove_outliers_iqr(series: pd.Series) -> pd.Series:
    """Remove outliers using the classic IQR rule."""
    if series.empty:
        return series
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    return series[(series >= lower) & (series <= upper)]


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------
def barplot_metric(
    run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path
):
    """
    Produce a bar plot with mean ± standard deviation for a single metric.

    Each bar corresponds to one UC. The per-UC statistics are computed from
    the per-run values. UC labels on the x-axis make a legend redundant.
    Outliers are removed per UC using the IQR rule before computing stats.
    """
    if column not in run_df.columns:
        return

    # Robust per-UC stats with IQR outlier removal
    def _robust_stats(s: pd.Series) -> pd.Series:
        clean = remove_outliers_iqr(s.dropna())
        return pd.Series(
            {
                "mean": clean.mean(),
                "std": clean.std(),
                "count": len(clean),
            }
        )

    # groupby + apply returns a Series with MultiIndex; unstack to get DataFrame
    stat = run_df.groupby("uc")[column].apply(lambda s: _robust_stats(s)).unstack()

    if stat.empty:
        return

    ucs = stat.index.tolist()
    means = stat["mean"].values
    stds = stat["std"].fillna(0).values

    cmap = plt.get_cmap("tab10")
    colors = [cmap(i % 10) for i in range(len(ucs))]

    fig, ax = plt.subplots(figsize=(8, 4))
    x = range(len(ucs))

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

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def grouped_barplot_by_uc_and_peer(
    run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path
):
    """
    Group bars by UseCase (UC1..), with one bar per peer setting (P10, P50, ...).

    Expects run_df["uc"] labels like: "UC1_P10", "UC1_P50", ...
    Computes mean ± std over runs for each (UC, peer) group, with IQR outlier removal.
    """
    if column not in run_df.columns:
        return

    df = run_df[["uc", column]].dropna()
    if df.empty:
        return

    # Parse labels: "UC1_P10" -> base_uc="UC1", peer="P10"
    parsed = (
        df["uc"]
        .astype(str)
        .str.extract(r"^(?P<base_uc>UC\d+)_P(?P<peers>\d+)(?:_C(?P<churn>\d+))?$")
    )
    df = df.join(parsed)

    # Keep only rows that match the expected pattern
    df = df.dropna(subset=["base_uc", "peers"])
    if df.empty:
        return

    # If churn exists, treat UCx_Cy as group key, so each churn becomes its own x-tick group
    if "churn" in df.columns:
        df["base_uc"] = df.apply(
            lambda r: (
                f"{r['base_uc']}_C{int(r['churn'])}"
                if pd.notna(r["churn"])
                else r["base_uc"]
            ),
            axis=1,
        )

    df["peers"] = df["peers"].astype(int)
    df["peer_label"] = df["peers"].map(lambda p: f"P{p}")

    def _uc_sort_key(s: str):
        m = re.match(r"^UC(\d+)(?:_C(\d+))?$", s)
        if not m:
            return (9999, 9999)
        uc_num = int(m.group(1))
        churn_num = int(m.group(2)) if m.group(2) else -1
        return (uc_num, churn_num)

    base_ucs = sorted(df["base_uc"].unique(), key=_uc_sort_key)  # UC1, UC2, ...
    peer_levels = sorted(df["peers"].unique())  # e.g., [10, 50]
    peer_labels = [f"P{p}" for p in peer_levels]

    # Robust stats with IQR outlier removal per group
    def _robust_stats(s: pd.Series) -> pd.Series:
        clean = remove_outliers_iqr(s.dropna())
        return pd.Series({"mean": clean.mean(), "std": clean.std()})

    stat = (
        df.groupby(["base_uc", "peers"])[column]
        .apply(_robust_stats)
        .unstack()
        .reset_index()
    )
    # stat columns: base_uc, peers, mean, std

    # Build matrices aligned to (base_ucs x peer_levels)
    means = {p: [float("nan")] * len(base_ucs) for p in peer_levels}
    stds = {p: [0.0] * len(base_ucs) for p in peer_levels}

    stat_idx = {(r["base_uc"], int(r["peers"])): r for _, r in stat.iterrows()}
    for i, uc in enumerate(base_ucs):
        for p in peer_levels:
            r = stat_idx.get((uc, p))
            if r is None:
                continue
            means[p][i] = r["mean"]
            stds[p][i] = 0.0 if pd.isna(r["std"]) else float(r["std"])

    # Plot
    fig, ax = plt.subplots(figsize=(8, 4), constrained_layout=True)
    x = list(range(len(base_ucs)))

    total_width = 0.8
    bar_w = total_width / max(len(peer_levels), 1)

    for j, p in enumerate(peer_levels):
        offsets = [xi + (j - (len(peer_levels) - 1) / 2) * bar_w for xi in x]
        ax.bar(
            offsets,
            means[p],
            width=bar_w * 0.9,
            yerr=stds[p],
            capsize=4,
            label=f"P{p}",
        )

    ax.set_xticks(x)
    ax.set_xticklabels(base_ucs)
    ax.set_ylim(0, None)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(title="Peers", loc="center left", bbox_to_anchor=(1.02, 0.5))

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def scatter_metric(
    run_df: pd.DataFrame, column: str, title: str, ylabel: str, out: Path
):
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

    fig.tight_layout()
    fig.savefig(out, dpi=200)
    plt.close(fig)


def _plot_quantiles_generic(
    run_df: pd.DataFrame, cols: list[str], title: str, ylabel: str, out: Path
):
    """
    Helper to plot multiple quantile metrics side-by-side for each UC.

    For each UC and each metric in 'cols', we compute mean ± std over runs.
    Outliers are removed per (UC, metric) using the IQR rule before computing
    the statistics.
    """
    present_cols = [c for c in cols if c in run_df.columns]
    if not present_cols:
        return

    # All UCs that have at least one non-NaN value in any of the metrics
    df = run_df[["uc"] + present_cols].dropna(how="all", subset=present_cols)
    if df.empty:
        return

    ucs = sorted(df["uc"].unique())
    n_ucs = len(ucs)
    n_metrics = len(present_cols)

    # mean/std matrices: shape (n_ucs, n_metrics)
    means = [[0.0 for _ in range(n_metrics)] for _ in range(n_ucs)]
    stds = [[0.0 for _ in range(n_metrics)] for _ in range(n_ucs)]

    for uc_idx, uc in enumerate(ucs):
        uc_df = df[df["uc"] == uc]

        for m_idx, col in enumerate(present_cols):
            s = uc_df[col].dropna()
            if s.empty:
                means[uc_idx][m_idx] = float("nan")
                stds[uc_idx][m_idx] = 0.0
                continue

            clean = remove_outliers_iqr(s)
            means[uc_idx][m_idx] = clean.mean()
            stds[uc_idx][m_idx] = clean.std() if len(clean) > 1 else 0.0

    # Base x positions for the groups (one group per metric)
    x_base = list(range(n_metrics))

    # Total width allocated for each group on the x-axis
    total_width = 0.8
    # Width of a single bar within the group, depending on number of UCs
    width = total_width / max(n_ucs, 1)

    fig, ax = plt.subplots()

    for uc_idx, uc in enumerate(ucs):
        uc_means = [means[uc_idx][m_idx] for m_idx in range(n_metrics)]
        uc_stds = [stds[uc_idx][m_idx] for m_idx in range(n_metrics)]

        # Center bars around the group position
        offsets = [x + (uc_idx - (n_ucs - 1) / 2) * width for x in x_base]

        ax.bar(
            offsets,
            uc_means,
            width=width * 0.9,
            yerr=uc_stds,
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
    ap.add_argument(
        "--uc",
        nargs="+",
        required=True,
        help="Use-cases (e.g. uc1 uc2). Mapped to logs/<uc>/ ...",
    )
    ap.add_argument(
        "--peers",
        nargs="*",
        type=int,
        help="Optional peer counts matching subfolders p<PEERS> under logs/<uc>/",
    )
    ap.add_argument(
        "--churns",
        nargs="*",
        type=int,
        help="Optional churn percentages matching subfolders c<CHURN> under logs/<uc>/p<PEERS>/",
    )
    ap.add_argument(
        "--out",
        default="docs/metrics",
        help="Output directory root for generated CSV and plots",
    )
    args = ap.parse_args()

    # Build UC specs: (label used in plots, name used in combo_name, base path)
    uc_specs: list[tuple[str, str, Path, int | None]] = []

    for entry in args.uc:
        # Allow custom path via "LABEL:/path/to/logs"
        if ":" in entry:
            label, path = entry.split(":", 1)
            base_label_raw = label.upper()
            display_label = UC_LABEL_REMAP.get(base_label_raw, base_label_raw)
            base_name = label.lower()
            base_path = Path(path)
        else:
            base_label_raw = entry.upper()
            display_label = UC_LABEL_REMAP.get(base_label_raw, base_label_raw)
            base_name = entry.lower()
            base_path = Path(f"logs/{base_name}")

        if args.peers:
            # If peer counts are given, expect subdirectories p<PEERS>
            for p in args.peers:
                if args.churns:
                    for c in args.churns:
                        sub_label = f"{display_label}_P{p}_C{c}"  # shown in plots
                        sub_name = f"{base_name}_p{p}_c{c}"  # folder/tag name
                        sub_path = base_path / f"p{p}" / f"c{c}"
                        uc_specs.append((sub_label, sub_name, sub_path, c))
                else:
                    sub_label = f"{display_label}_P{p}"  # shown in plots
                    sub_name = f"{base_name}_p{p}"  # folder/tag name
                    sub_path = base_path / f"p{p}"
                    uc_specs.append((sub_label, sub_name, sub_path, None))
        else:
            if args.churns:
                for c in args.churns:
                    sub_label = f"{display_label}_C{c}"
                    sub_name = f"{base_name}_c{c}"
                    sub_path = base_path / f"c{c}"
                    uc_specs.append((sub_label, sub_name, sub_path, c))
            else:
                uc_specs.append((display_label, base_name, base_path, None))

    # Combine all UC names into one folder name for this analysis run
    combo_name = "_".join(sorted({name for _, name, _, _ in uc_specs}))
    out_dir = Path(args.out) / combo_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load all UCs into a single peer-level DataFrame
    all_peers = []
    for uc_label, _, uc_path, churn_pct in uc_specs:
        df = load_runs(uc_path, uc_label, churn_pct=churn_pct)
        all_peers.append(df)
    peer_df = pd.concat(all_peers, ignore_index=True)

    # Sender-fail rates per UC: fraction of runs where sender_joined==False
    fail_series = peer_df.groupby(["uc", "run"])["sender_joined"].first().eq(False)
    fail_rates = fail_series.groupby("uc").mean()

    fail_df = (fail_rates * 100).round(2).reset_index()
    fail_df.columns = ["uc", "sender_fail_rate_percent"]
    fail_df.to_csv(out_dir / "sender_fail_rates.csv", index=False)

    # List of metrics expected in the summaries.
    # Boolean flags like joined/saw_test/sender_joined are treated as 0/1.
    metrics = [
        # delivery / duplicates / ordering
        "delivery_rate",
        "duplicate_rate",
        "received_unique",
        "recv_total",
        "total_expected",
        "duplicates",
        "out_of_order",
        # churn parameter (constant per run; added for filtering/labeling)
        "churn_pct",
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
        # reachability & connectivity
        "pr_avg_ratio",
        "avg_connected_peers",
        # downtime
        "downtime_total_ms",
        "downtime_periods",
        "downtime_p50_ms",
        "downtime_p90_ms",
        "downtime_max_ms",
        # neighbour counts (active view churn)
        "neighbour_down",
        "neighbour_up",
        # join / run state
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

    # -----------------------------------------------------------------------
    # Special handling for "critical" percentage / ratio metrics
    # -----------------------------------------------------------------------

    # Delivery rate only over receivers that actually joined the mesh.
    if {"joined", "delivery_rate"}.issubset(peer_df.columns):
        delivery_joined = (
            peer_df[peer_df["joined"] == 1]
            .groupby(["uc", "run"], as_index=False)["delivery_rate"]
            .mean()
            .rename(columns={"delivery_rate": "delivery_rate_joined_only"})
        )
        delivery_joined.to_csv(
            out_dir / "delivery_rate_joined_only_per_run.csv",
            index=False,
        )
        run_df = run_df.merge(delivery_joined, on=["uc", "run"], how="left")

    # Per-run CSV for out-of-order events (mean across peers)
    if "out_of_order" in run_df.columns:
        ooo_per_run = run_df[["uc", "run", "out_of_order"]]
        ooo_per_run.to_csv(out_dir / "out_of_order_per_run.csv", index=False)

    # Per-run CSV for average connected peers
    if "avg_connected_peers" in run_df.columns:
        conn_per_run = run_df[["uc", "run", "avg_connected_peers"]]
        conn_per_run.to_csv(out_dir / "connected_peers_per_run.csv", index=False)

    # Per-run CSV for downtime statistics
    downtime_cols = [
        "downtime_total_ms",
        "downtime_periods",
        "downtime_p50_ms",
        "downtime_p90_ms",
        "downtime_max_ms",
    ]
    if all(c in run_df.columns for c in downtime_cols):
        downtime_per_run = run_df[["uc", "run"] + downtime_cols]
        downtime_per_run.to_csv(out_dir / "downtime_per_run.csv", index=False)

    # Per-run CSV for neighbour churn (NeighborUp/NeighborDown counts)
    if {"neighbour_down", "neighbour_up"}.issubset(run_df.columns):
        neigh_per_run = run_df[["uc", "run", "neighbour_down", "neighbour_up"]]
        neigh_per_run.to_csv(out_dir / "neighbour_counts_per_run.csv", index=False)

    # Filter to valid runs where the sender actually joined.
    valid_run_df = run_df[run_df["sender_joined"] == 1].copy()

    # Per-run CSV for receiver ratios (joined / saw_test), valid runs only
    if {"joined", "saw_test"}.issubset(valid_run_df.columns):
        receiver_ratios_valid = valid_run_df[
            ["uc", "run", "joined", "saw_test"]
        ].rename(columns={"joined": "joined_ratio", "saw_test": "saw_test_ratio"})
        receiver_ratios_valid.to_csv(
            out_dir / "receiver_ratios_per_run.csv",
            index=False,
        )

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
            summary.join(runs_all, how="left")
            .join(runs_valid, how="left")
            .reset_index()
        )

        summary.to_csv(out_dir / "receiver_ratios_uc_summary.csv", index=False)

    # UC-level summary for delivery rate (joined receivers only, valid runs only)
    if "delivery_rate_joined_only" in run_df.columns:
        dr_valid = valid_run_df[["uc", "run", "delivery_rate_joined_only"]].dropna()

        dr_summary = (
            dr_valid.groupby("uc")["delivery_rate_joined_only"]
            .agg(
                total_runs="count",
                mean_delivery_rate="mean",
                std_delivery_rate="std",
                min_rate="min",
                max_rate="max",
            )
            .reset_index()
        )

        dr_summary.to_csv(
            out_dir / "delivery_rate_joined_only_uc_summary.csv",
            index=False,
        )

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
    grouped_barplot_by_uc_and_peer(
        run_df,
        delivery_column,
        (
            "Delivery Rate (joined receivers only)"
            if delivery_column == "delivery_rate_joined_only"
            else "Delivery Rate"
        ),
        "rate",
        out_dir / "delivery_rate.png",
    )

    # Out-of-order events: simple bar plot (mean per run)
    grouped_barplot_by_uc_and_peer(
        run_df,
        "out_of_order",
        "Out-of-order Events",
        "count",
        out_dir / "out_of_order.png",
    )

    # Latency and LDH quantiles: grouped bar plots
    plot_latency_quantiles(run_df, out_dir / "latency_quantiles.png")
    plot_ldh_quantiles(run_df, out_dir / "ldh_quantiles.png")

    # Average number of connected peers
    grouped_barplot_by_uc_and_peer(
        run_df,
        "avg_connected_peers",
        "Average Peers in Active View",
        "peers",
        out_dir / "avg_connected_peers.png",
    )

    # Total downtime in ms (periods with zero neighbours)
    grouped_barplot_by_uc_and_peer(
        run_df,
        "downtime_total_ms",
        "Total Time without any Peers in Active View",
        "ms",
        out_dir / "downtime_total_ms.png",
    )

    # Neighbour churn (NeighborUp + NeighborDown)
    grouped_barplot_by_uc_and_peer(
        run_df,
        "neighbour_up",
        "NeighborUp/NeigborDown Events (per run, avg over peers)",
        "count",
        out_dir / "neighbour_up.png",
    )

    # Receiver ratios (joined): barplots over valid runs
    grouped_barplot_by_uc_and_peer(
        valid_run_df,
        "joined",
        "Receiver Join Ratio (valid runs)",
        "fraction of peers",
        out_dir / "receiver_join_ratio.png",
    )

    print("[OK] All metrics written to", out_dir.resolve())


if __name__ == "__main__":
    main()
