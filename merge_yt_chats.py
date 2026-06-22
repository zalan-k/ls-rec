#!/usr/bin/env python3
"""
merge_yt_chats.py - Merge N YouTube chat JSONL files into one unified timeline.

Handles both live-recording format (isLive: true, negative offsets for pre-stream)
and post-hoc download format (clickTrackingParams at top level, offsets from 0).

When zero-point derivation disagrees between files (common with live vs post-hoc),
shift detection kicks in automatically: overlapping message IDs are used to compute
the correct offset delta via median, giving accurate alignment regardless of what
reference point yt-dlp used internally.

Usage:
    python merge_yt_chats.py <file1> <file2> [file3 ...] [-o OUTPUT]

    -o / --output       Output path. Default: first input + .merged.json suffix.
    --no-dedup          Skip message ID deduplication.
    --no-shift-detect   Disable overlap-based shift detection (use zero points only).
    --dry-run           Print stats without writing output.

Examples:
    python merge_yt_chats.py 587_live.json 587_posthoc.json
    python merge_yt_chats.py part1.json part2.json -o 587_final.json
    python merge_yt_chats.py part1.json part2.json posthoc.json -o final.json
    python merge_yt_chats.py live.json posthoc.json --dry-run
"""

import argparse
import datetime
import json
import os
import statistics
import sys


# threshold: zero-points further apart than this trigger shift detection
ZERO_MISMATCH_THRESHOLD_MS = 5 * 60 * 1000   # 5 minutes


# ═══════════════════════════════════════════════════════════════════════════
#  FORMAT DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def detect_format(path: str) -> str:
    """Returns 'live' | 'posthoc' | 'unknown'."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            if d.get("isLive"):
                return "live"
            if "clickTrackingParams" in d:
                return "posthoc"
            if "replayChatItemAction" in d:
                return "live"
            break
    return "unknown"


# ═══════════════════════════════════════════════════════════════════════════
#  FIELD EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════

def first_msg_usec(entry: dict) -> int | None:
    """Pull timestampUsec from inside replayChatItemAction.actions."""
    actions = entry.get("replayChatItemAction", {}).get("actions", [])
    for action in actions:
        for renderer in action.values():
            if not isinstance(renderer, dict):
                continue
            item = renderer.get("item", {})
            for r in item.values():
                if isinstance(r, dict):
                    ts = r.get("timestampUsec")
                    if ts:
                        try:
                            return int(ts)
                        except (ValueError, TypeError):
                            pass
    return None


def get_offset_ms(entry: dict) -> int | None:
    """
    Extract videoOffsetTimeMsec as int.
    Live format: top-level. Post-hoc: nested inside replayChatItemAction.
    """
    raw = entry.get("videoOffsetTimeMsec")
    if raw is None:
        raw = entry.get("replayChatItemAction", {}).get("videoOffsetTimeMsec")
    if raw is None:
        return None
    try:
        return int(raw)
    except (ValueError, TypeError):
        return None


def get_msg_id(entry: dict) -> str | None:
    """Extract stable message ID for deduplication."""
    actions = entry.get("replayChatItemAction", {}).get("actions", [])
    for action in actions:
        for renderer in action.values():
            if not isinstance(renderer, dict):
                continue
            item = renderer.get("item", {})
            for r in item.values():
                if isinstance(r, dict):
                    mid = r.get("id")
                    if mid:
                        return mid
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  ZERO POINT DERIVATION
# ═══════════════════════════════════════════════════════════════════════════

def derive_zero_usec(path: str, max_scan: int = 500) -> int | None:
    """
    Derive wall-clock zero (usec): timestampUsec - videoOffsetTimeMsec * 1000.
    Scans up to max_scan lines to find a usable entry.
    """
    scanned = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or scanned >= max_scan:
                break
            scanned += 1
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue
            offset_ms = get_offset_ms(d)
            if offset_ms is None:
                continue
            ts_usec = first_msg_usec(d)
            if ts_usec is not None:
                return ts_usec - offset_ms * 1000
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  FILE LOADING
# ═══════════════════════════════════════════════════════════════════════════

def load_file(path: str) -> tuple[list[dict], int]:
    """
    Load all parseable entries from a JSONL file.
    Returns (entries, skipped_count).
    Each entry: {offset_ms, msg_id, ts_usec, data}
    """
    entries = []
    skipped = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                d = json.loads(stripped)
            except json.JSONDecodeError:
                skipped += 1
                continue
            offset_ms = get_offset_ms(d)
            if offset_ms is None:
                skipped += 1
                continue
            entries.append({
                "offset_ms": offset_ms,
                "msg_id":    get_msg_id(d),
                "ts_usec":   first_msg_usec(d),
                "data":      d,
            })
    return entries, skipped


# ═══════════════════════════════════════════════════════════════════════════
#  SHIFT DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def detect_shift_ms(
    anchor_entries: list[dict],
    target_entries: list[dict],
    min_samples: int = 10,
) -> int | None:
    """
    Compute shift to add to target offsets so they align with anchor.
    Uses overlapping message IDs; returns median delta, or None if
    fewer than min_samples matches found.

        shift = median(anchor_offset - target_offset) for matched IDs
    """
    anchor_map: dict[str, int] = {
        e["msg_id"]: e["offset_ms"]
        for e in anchor_entries
        if e["msg_id"]
    }
    deltas: list[int] = []
    for e in target_entries:
        mid = e["msg_id"]
        if mid and mid in anchor_map:
            deltas.append(anchor_map[mid] - e["offset_ms"])
        if len(deltas) >= 500:
            break

    if len(deltas) < min_samples:
        return None
    return int(statistics.median(deltas))


# ═══════════════════════════════════════════════════════════════════════════
#  HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def fmt_offset(ms: int) -> str:
    sign = "-" if ms < 0 else ""
    secs = abs(ms) // 1000
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{sign}{h:02d}:{m:02d}:{s:02d}"


def fmt_ts(usec: int | None) -> str:
    if usec is None:
        return "unknown"
    return datetime.datetime.fromtimestamp(usec / 1_000_000).strftime("%Y-%m-%d %H:%M:%S")


# ═══════════════════════════════════════════════════════════════════════════
#  MERGE
# ═══════════════════════════════════════════════════════════════════════════

def merge(
    inputs: list[str],
    output: str,
    dedup: bool = True,
    shift_detect: bool = True,
    dry_run: bool = False,
) -> None:

    # ── 1. Analyse inputs ────────────────────────────────────────────────
    file_info: list[dict] = []
    for path in inputs:
        file_info.append({
            "path":   path,
            "format": detect_format(path),
            "zero":   derive_zero_usec(path),
            "size":   os.path.getsize(path),
        })

    print()
    print("  ─── Inputs " + "─" * 50)
    for i, info in enumerate(file_info):
        print(f"  [{i + 1}] {os.path.basename(info['path'])}")
        print(f"       format={info['format']}  "
              f"zero={fmt_ts(info['zero'])}  "
              f"size={info['size'] / (1024*1024):.1f}MB")
    print()

    # ── 2. Anchor = file with earliest zero ──────────────────────────────
    zeros = [(i, info["zero"]) for i, info in enumerate(file_info)
             if info["zero"] is not None]
    if not zeros:
        print("  ✗ Could not derive a zero point from any input.")
        sys.exit(1)

    anchor_idx, common_zero_usec = min(zeros, key=lambda x: x[1])
    print(f"  Anchor: [{anchor_idx + 1}]  zero={fmt_ts(common_zero_usec)}")
    print()

    # ── 3. Load all files ────────────────────────────────────────────────
    for info in file_info:
        entries, skipped = load_file(info["path"])
        info["entries"] = entries
        info["skipped"] = skipped

    anchor_entries = file_info[anchor_idx]["entries"]

    # ── 4. Compute shift per non-anchor file ─────────────────────────────
    print("  ─── Alignment " + "─" * 47)
    for i, info in enumerate(file_info):
        if i == anchor_idx:
            info["shift_ms"] = 0
            print(f"  [{i + 1}] anchor — no shift applied")
            continue

        file_zero  = info["zero"]
        zero_delta = None
        if file_zero is not None:
            zero_delta = (common_zero_usec - file_zero) // 1000

        # Use overlap detection if zeros are too far apart or unknown
        mismatch = (
            file_zero is None
            or (zero_delta is not None
                and abs(zero_delta) > ZERO_MISMATCH_THRESHOLD_MS)
        )

        if shift_detect and mismatch:
            detected = detect_shift_ms(anchor_entries, info["entries"])
            if detected is not None:
                info["shift_ms"] = detected
                note = ""
                if zero_delta is not None:
                    note = (f"  (zero-point would give {fmt_offset(zero_delta)}"
                            f" — {'matches' if abs(detected - zero_delta) < 1000 else 'DIFFERS'})")
                print(f"  [{i + 1}] overlap shift: {fmt_offset(detected)} ({detected:+,}ms){note}")
            else:
                # Not enough overlap — fall back to zero-point
                info["shift_ms"] = zero_delta or 0
                print(f"  [{i + 1}] insufficient overlap — "
                      f"zero-point fallback: {fmt_offset(info['shift_ms'])}")
        else:
            info["shift_ms"] = zero_delta or 0
            print(f"  [{i + 1}] zero-point shift: {fmt_offset(info['shift_ms'])} "
                  f"({info['shift_ms']:+,}ms)")
    print()

    # ── 5. Apply shifts, collect ─────────────────────────────────────────
    all_entries: list[tuple[int, dict]] = []
    per_file_stats = []

    for info in file_info:
        shift   = info["shift_ms"]
        entries = info["entries"]
        rebased = 0

        for e in entries:
            new_offset = e["offset_ms"] + shift
            if new_offset != e["offset_ms"]:
                d = e["data"]
                d["videoOffsetTimeMsec"] = str(new_offset)
                # Also patch nested location (post-hoc format)
                rcia = d.get("replayChatItemAction")
                if rcia and "videoOffsetTimeMsec" in rcia:
                    rcia["videoOffsetTimeMsec"] = str(new_offset)
                rebased += 1
            all_entries.append((new_offset, e["data"]))

        per_file_stats.append({
            "label":   os.path.basename(info["path"]),
            "count":   len(entries),
            "rebased": rebased,
            "skipped": info["skipped"],
        })

    # ── 6. Sort ──────────────────────────────────────────────────────────
    all_entries.sort(key=lambda x: x[0])

    # ── 7. Deduplicate ───────────────────────────────────────────────────
    seen_ids: set[str] = set()
    deduped: list[tuple[int, dict]] = []
    dup_count = 0

    for offset_ms, d in all_entries:
        if dedup:
            mid = get_msg_id(d)
            if mid:
                if mid in seen_ids:
                    dup_count += 1
                    continue
                seen_ids.add(mid)
        deduped.append((offset_ms, d))

    # ── 8. Report ────────────────────────────────────────────────────────
    print("  ─── Per-file stats " + "─" * 42)
    for s in per_file_stats:
        print(f"  {s['label']}")
        print(f"       read={s['count']}  rebased={s['rebased']}  skipped={s['skipped']}")
    print()

    total_in  = sum(s["count"] for s in per_file_stats)
    total_out = len(deduped)
    print(f"  Total input : {total_in}")
    if dedup:
        print(f"  Duplicates  : {dup_count} removed")
    print(f"  Total output: {total_out}")
    if deduped:
        print(f"  Range       : {fmt_offset(deduped[0][0])}  →  {fmt_offset(deduped[-1][0])}")
    print()

    if dry_run:
        print("  --dry-run: no output written.")
        return

    # ── 9. Write ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(os.path.abspath(output)), exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        for _, d in deduped:
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    out_mb = os.path.getsize(output) / (1024 * 1024)
    print(f"  ✔ Written: {output}  ({out_mb:.1f}MB)")
    print()
    print("  Inspect, then rename to replace the original if it looks right.")
    print()


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Merge N YouTube chat JSONL files into a unified timeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  python merge_yt_chats.py 587_live.json 587_posthoc.json
  python merge_yt_chats.py part1.json part2.json -o 587_final.json
  python merge_yt_chats.py part1.json part2.json posthoc.json -o final.json
  python merge_yt_chats.py live.json posthoc.json --dry-run
        """,
    )
    parser.add_argument("inputs", nargs="+", metavar="FILE",
                        help="Input JSONL chat files (2 or more)")
    parser.add_argument("-o", "--output", default=None,
                        help="Output path (default: first input + .merged.json)")
    parser.add_argument("--no-dedup", action="store_true",
                        help="Skip message ID deduplication")
    parser.add_argument("--no-shift-detect", action="store_true",
                        help="Disable overlap-based shift detection")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print stats without writing output")
    args = parser.parse_args()

    if len(args.inputs) < 2:
        parser.error("At least 2 input files required.")

    for path in args.inputs:
        if not os.path.exists(path):
            print(f"  ✗ File not found: {path}")
            sys.exit(1)

    output = args.output or (
        os.path.splitext(args.inputs[0])[0] + ".merged.json"
    )

    merge(
        inputs=args.inputs,
        output=output,
        dedup=not args.no_dedup,
        shift_detect=not args.no_shift_detect,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
