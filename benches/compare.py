#!/usr/bin/env python3
"""
Compare two hyperfine JSON results and print a Criterion-style table.

Usage:
    compare.py PREVIOUS.json CURRENT.json [--prev-label NAME] [--curr-label NAME]
"""

import json
import sys
import argparse

GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def supports_color() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()


def color(text: str, code: str) -> str:
    return f"{code}{text}{RESET}" if supports_color() else text


def fmt_time(seconds: float | None) -> str:
    if seconds is None:
        return "N/A"
    if seconds >= 60:
        return f"{seconds / 60:.2f} min"
    if seconds >= 1:
        return f"{seconds:.3f} s"
    if seconds >= 0.001:
        return f"{seconds * 1000:.3f} ms"
    return f"{seconds * 1e6:.3f} us"


def fmt_bytes(n: int | float | None) -> str:
    if n is None:
        return "N/A"
    for unit, threshold in (("GB", 1 << 30), ("MB", 1 << 20), ("KB", 1 << 10)):
        if n >= threshold:
            return f"{n / threshold:.1f} {unit}"
    return f"{int(n)} B"


def fmt_pct(prev: float | None, curr: float | None) -> tuple[str, float | None]:
    """Returns (formatted string, raw delta %) or ('N/A', None)."""
    if prev is None or curr is None or prev == 0:
        return "N/A", None
    delta = (curr - prev) / prev * 100
    sign = "+" if delta >= 0 else ""
    return f"{sign}{delta:.2f}%", delta


def load(path: str) -> dict:
    try:
        with open(path) as f:
            data = json.load(f)
        return data["results"][0]
    except (json.JSONDecodeError, KeyError, IndexError, OSError) as e:
        print(f"(skipping comparison: {path} is empty or incomplete — {e})")
        sys.exit(0)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("prev", help="previous baseline JSON")
    ap.add_argument("curr", help="current result JSON")
    ap.add_argument("--prev-label", default="previous")
    ap.add_argument("--curr-label", default="current")
    args = ap.parse_args()

    prev = load(args.prev)
    curr = load(args.curr)

    name = curr.get("command", "benchmark")

    def peak_mem(result: dict) -> float | None:
        samples = result.get("memory_usage_byte") or []
        return max(samples) if samples else None

    rows = [
        ("mean",    prev.get("mean"),    curr.get("mean")),
        ("median",  prev.get("median"),  curr.get("median")),
        ("min",     prev.get("min"),     curr.get("min")),
        ("max",     prev.get("max"),     curr.get("max")),
        ("stddev",  prev.get("stddev"),  curr.get("stddev")),
        ("user",    prev.get("user"),    curr.get("user")),
        ("system",  prev.get("system"),  curr.get("system")),
        ("max_mem", peak_mem(prev),       peak_mem(curr)),
    ]
    w_label  = 10
    w_time   = 14
    w_change = 10

    header_prev = args.prev_label[:w_time].rjust(w_time)
    header_curr = args.curr_label[:w_time].rjust(w_time)

    print(f"\n{color(name, BOLD)}\n")
    print(f"  {'':>{w_label}}  {header_prev}  {header_curr}  {'change':>{w_change}}")
    print(f"  {'-' * w_label}  {'-' * w_time}  {'-' * w_time}  {'-' * w_change}")

    mean_delta = None
    for label, pv, cv in rows:
        pct_str, delta = fmt_pct(pv, cv)

        if label == "mean":
            mean_delta = delta

        if delta is None or abs(delta) < 1.0:
            pct_colored = pct_str
        elif delta < 0:
            # lower is better for time; higher is worse for memory
            pct_colored = color(pct_str, RED if label == "max_mem" else GREEN)
        else:
            pct_colored = color(pct_str, GREEN if label == "max_mem" else RED)

        fmt = fmt_bytes if label == "max_mem" else fmt_time
        print(
            f"  {label:>{w_label}}  "
            f"{fmt(pv):>{w_time}}  "
            f"{fmt(cv):>{w_time}}  "
            f"{pct_colored:>{w_change}}"
        )

    # Verdict
    print()
    if mean_delta is None:
        pass
    elif abs(mean_delta) < 1.0:
        print(f"  No change (<1%).")
    elif mean_delta < 0:
        print(f"  {color('Performance has improved.', GREEN)}")
    else:
        print(f"  {color('Performance has regressed.', RED)}")
    print()


if __name__ == "__main__":
    main()
