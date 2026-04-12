#!/usr/bin/env python3
"""
ls-audit - Audit and reconstruct Obsidian livestream entries.

Philosophy:
    The Obsidian entry is the source of truth for: date, checkbox, and
    user notes. Everything else (titles, URLs, file links, durations) is
    rebuilt from video IDs.

    Video IDs are resolved in priority order:
        1. CLI override                (--yt-id / --tw-id)
        2. Cache entry for this index  (fast path, populated by live recorder,
                                        ls-download, prior audits, injection)
        3. URL in the existing Obsidian entry
        4. NAS filename (extracts [video_id])
        5. Date-matched search in the refreshable platform pool

    Every successful resolution upserts into the cache, so the next audit
    of the same entry is O(1).

Usage:
    ls-audit <index>                        Reconstruct entry
    ls-audit <index> --yt-id ID             Override YouTube video ID
    ls-audit <index> --tw-id ID             Override Twitch video ID
    ls-audit --refresh [youtube|twitch]     Refresh pool from platform APIs
    ls-audit --inject URL                   Add a video to the cache (prompts for index)
    ls-audit --inject --manual              Manually add to cache
    ls-audit --cache-info ID|INDEX          Look up a cache entry
"""

import argparse
import datetime
import glob
import os
import re
import subprocess
import sys

import ls_common


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".ts", ".flv", ".mov")
TAG = {"youtube": "yt", "twitch": "tw"}


# ══════════════════════════════════════════════════════════════════════════════
#  CACHE INJECTION
# ══════════════════════════════════════════════════════════════════════════════

def inject_video(config, ytdlp, cache, url=None):
    """Inject a video into the cache entries table."""
    if url:
        print(f"  ⌛ Fetching metadata for: {url}")
        data = ytdlp.probe(url, timeout=60)
        if not data:
            print("  ⚠ yt-dlp probe failed. Falling back to manual entry.")
            return _inject_manual(cache)
        platform = "twitch" if "twitch.tv" in url else "youtube"
        entry = _build_entry_from_probe(data, platform)
    else:
        return _inject_manual(cache)

    _print_injection(entry)
    if input("\n  Add to cache? (y/n): ").strip().lower() != "y":
        print("  Skipped.")
        return

    index = _prompt_index(cache)
    if index is None:
        return

    tag = TAG[entry["platform"]]
    cache.upsert(
        index,
        injected=True,
        **{
            f"{tag}_id":        entry["id"],
            f"{tag}_title":     entry["title"],
            f"{tag}_starttime": entry["start_time"],
            f"{tag}_duration":  entry.get("duration"),
        },
    )
    cache.save()
    print(f"  ✔ Added to cache entry #{index:03d} ({entry['platform']}).")


def _build_entry_from_probe(data, platform):
    release_ts = data.get("release_timestamp")
    upload_date = data.get("upload_date", "")
    if release_ts:
        start_time = datetime.datetime.fromtimestamp(release_ts).isoformat()
    elif upload_date:
        start_time = datetime.datetime.strptime(upload_date, "%Y%m%d").isoformat()
    else:
        start_time = datetime.datetime.now().isoformat()
    return {
        "id":         data.get("id", "unknown"),
        "platform":   platform,
        "title":      data.get("title") or data.get("description") or "Unknown",
        "start_time": start_time,
        "duration":   data.get("duration"),
    }


def _inject_manual(cache):
    print("\n  Manual cache entry:")
    platform = input("  Platform (youtube/twitch): ").strip().lower()
    if platform not in ("youtube", "twitch"):
        print("  ✗ Invalid platform.")
        return
    vid_id = input("  Video ID: ").strip()
    if not vid_id:
        print("  ✗ ID required.")
        return
    title = input("  Title: ").strip() or "Unknown"
    date_str = input("  Start date/time (YYYY-MM-DD or ISO): ").strip()
    try:
        if "T" in date_str:
            start_time = date_str
        else:
            start_time = datetime.datetime.strptime(date_str, "%Y-%m-%d").isoformat()
    except ValueError:
        print("  ✗ Invalid date.")
        return
    dur = input("  Duration in seconds (Enter to skip): ").strip()
    duration = int(dur) if dur.isdigit() else None

    entry = {
        "id": vid_id, "platform": platform, "title": title,
        "start_time": start_time, "duration": duration,
    }
    _print_injection(entry)
    if input("\n  Add to cache? (y/n): ").strip().lower() != "y":
        print("  Skipped.")
        return

    index = _prompt_index(cache)
    if index is None:
        return

    tag = TAG[platform]
    cache.upsert(
        index,
        injected=True,
        **{
            f"{tag}_id":        vid_id,
            f"{tag}_title":     title,
            f"{tag}_starttime": start_time,
            f"{tag}_duration":  duration,
        },
    )
    cache.save()
    print(f"  ✔ Added to cache entry #{index:03d} ({platform}).")


def _prompt_index(cache):
    raw = input("  Internal index to attach to (e.g., 557): ").strip()
    if not raw.isdigit():
        print("  ✗ Invalid index.")
        return None
    return int(raw)


def _print_injection(entry):
    dur = entry.get("duration")
    dur_s = (f"{dur}s ({dur//3600}h{(dur%3600)//60:02d}m)" if dur else "unknown")
    print(f"\n  Platform : {entry['platform']}")
    print(f"  ID       : {entry['id']}")
    print(f"  Title    : {entry['title']}")
    print(f"  Start    : {entry['start_time']}")
    print(f"  Duration : {dur_s}")


def cache_info(cache, query):
    """Look up a cache entry by internal index or by yt/tw video ID."""
    if query.isdigit() and len(query) < 6:
        e = cache.get(int(query))
        if e:
            _print_cache_entry(e)
            return
    for entry in cache.data["entries"].values():
        if entry.get("yt_id") == query or entry.get("tw_id") == query:
            _print_cache_entry(entry)
            return
    print(f"  ✗ No cache entry for '{query}'.")


def _print_cache_entry(e):
    print(f"\n  Index    : {e.get('index')}")
    print(f"  Injected : {'yes' if e.get('injected') else 'no'}")
    for plat in ("youtube", "twitch"):
        tag = TAG[plat]
        vid = e.get(f"{tag}_id")
        if not vid:
            continue
        dur = e.get(f"{tag}_duration")
        dur_s = f"{dur//3600}h{(dur%3600)//60:02d}m" if dur else "—"
        print(f"  {plat:8s}: {vid}")
        print(f"    title  : {e.get(f'{tag}_title') or '—'}")
        print(f"    start  : {e.get(f'{tag}_starttime') or '—'}")
        print(f"    duration: {dur_s}")


# ══════════════════════════════════════════════════════════════════════════════
#  NAS SCANNER
# ══════════════════════════════════════════════════════════════════════════════

def _extract_video_id_from_filename(filename):
    m = re.search(r"\[([^\]]+)\]\s*@\s*\d{4}-\d{2}-\d{2}", filename)
    return m.group(1) if m else None


def _classify_video_id(vid):
    return "twitch" if vid.lstrip("v").isdigit() else "youtube"


def scan_nas(config, index):
    """Return {'yt_video','yt_chat','tw_video','tw_chat'} for files matching this index."""
    found = {"yt_video": None, "yt_chat": None, "tw_video": None, "tw_chat": None}

    if not os.path.exists(config["nas_path"]):
        print("  ⚠ NAS not mounted")
        return found

    idx_padded = f"{int(index):03d}"
    patterns = [f"{idx_padded}_*"]
    if str(index) != idx_padded:
        patterns.append(f"{index}_*")

    seen = set()
    for pat in patterns:
        for filepath in glob.glob(os.path.join(config["nas_path"], pat)):
            filename = os.path.basename(filepath)
            if filename in seen:
                continue
            seen.add(filename)
            # Skip intermediate fragment files like `title.f140.m4a`
            if re.search(r"\.f\d+\.\w+$", filename):
                continue
            m = re.match(r"^(\d+)_", filename)
            if not m or int(m.group(1)) != int(index):
                continue
            vid = _extract_video_id_from_filename(filename)
            if not vid:
                continue
            platform = _classify_video_id(vid)
            ext = os.path.splitext(filename)[1].lower()
            tag = TAG[platform]
            if ext in VIDEO_EXTS:
                existing = found[f"{tag}_video"]
                if not existing or (ext == ".mp4" and not existing.lower().endswith(".mp4")):
                    found[f"{tag}_video"] = filename
            elif ext == ".json":
                found[f"{tag}_chat"] = filename
    return found


# ══════════════════════════════════════════════════════════════════════════════
#  ID RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def resolve_id(platform, index, entry, nas, cache, ytdlp, twitch, config, cli_override=None):
    """Resolve video ID. Upserts into cache on success. Returns (id, source)."""
    tag = TAG[platform]

    # 1. CLI override
    if cli_override:
        return cli_override, "cli override"

    # 2. Cache entry (fast path)
    cached = cache.get(index)
    if cached and cached.get(f"{tag}_id"):
        return cached[f"{tag}_id"], "cache entry"

    # 3. Existing URL in Obsidian entry
    entry_id = entry.get(f"{tag}_id")
    if entry_id:
        return entry_id, "entry url"

    # 4. NAS filename
    nas_file = nas.get(f"{tag}_video")
    if nas_file:
        vid = _extract_video_id_from_filename(nas_file)
        if vid:
            return vid, "nas filename"

    # 5. Date-matched pool search (auto-refresh if stale)
    if entry.get("date_obj"):
        newest = cache.newest_pool_date(platform)
        target = entry["date_obj"].strftime("%Y-%m-%d")
        if newest is None:
            _refresh(platform, cache, ytdlp, twitch, config, full=True)
        elif target > newest:
            _refresh(platform, cache, ytdlp, twitch, config, full=False)

        match = cache.find_in_pool(platform, entry["date_obj"])
        if match:
            label = "pool"
            if match.get("_fuzzy"):
                label += f" {match['_fuzzy']}"
            return match["id"], label

    return None, None


def _refresh(platform, cache, ytdlp, twitch, config, full=False):
    if platform == "youtube":
        cache.refresh_youtube(ytdlp, config, full=full)
    else:
        cache.refresh_twitch(twitch, full=full)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _title_from_filename(filename):
    name = os.path.splitext(filename)[0]
    name = re.sub(r"^\d+_", "", name)
    name = re.sub(r"\s*\[[^\]]+\]\s*@\s*\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$", "", name)
    return name


def _get_title(video_id, platform, cache, cache_entry, nas_file, ytdlp, config):
    """Title source priority: cache entry → pool → filename → API probe."""
    if not video_id:
        return None
    tag = TAG[platform]
    if cache_entry and cache_entry.get(f"{tag}_title"):
        return cache_entry[f"{tag}_title"]
    for s in cache.data[f"{tag}_pool"]:
        if s["id"] == video_id:
            return s.get("title")
    if nas_file:
        return _title_from_filename(nas_file)
    # Last resort: fetch from API
    url = ls_common.build_stream_url(config, platform, video_id)
    data = ytdlp.probe(url, playlist_items="1")
    if data:
        return data.get("title") or data.get("description")
    return None


def _get_duration(video_id, platform, cache, cache_entry):
    """Duration source priority: cache entry → pool."""
    if not video_id:
        return None
    tag = TAG[platform]
    if cache_entry and cache_entry.get(f"{tag}_duration"):
        return cache_entry[f"{tag}_duration"]
    for s in cache.data[f"{tag}_pool"]:
        if s["id"] == video_id and s.get("duration"):
            return s["duration"]
    return None


def _build_platform_line(tag, video_id, platform, title, video_file, chat_file, config):
    vid_link  = f"[📁]({ls_common.build_shell_cmd(config, video_file)})" if video_file else "[📁]()"
    chat_link = f"[📄]({ls_common.build_shell_cmd(config, chat_file)})" if chat_file else "[📄]()"
    display = title or "untitled"
    url = ls_common.build_stream_url(config, platform, video_id) if video_id else ""
    return f"\t`{tag}` {vid_link} {chat_link} [ {display} ]({url})"


def build_entry(config, index, entry, nas, cache, ytdlp, yt_id, tw_id):
    lines = []

    cache_entry = cache.get(index)

    # Durations
    durations = []
    for vid_id, platform in [(yt_id, "youtube"), (tw_id, "twitch")]:
        dur = _get_duration(vid_id, platform, cache, cache_entry)
        if dur:
            durations.append(dur)

    if durations:
        dur = max(durations)
        h, rem = divmod(int(dur), 3600)
        m, s = divmod(rem, 60)
        dur_str = f" [{h:02d}:{m:02d}:{s:02d}]"
    elif entry.get("duration_str"):
        dur_str = f" [{entry['duration_str']}]"
    else:
        dur_str = ""

    date_str = entry["date_str"] or "UNKNOWN"
    tz_str = entry["tz_str"] or "(GMT-6)"
    lines.append(f"- {entry['checkbox']} **{int(index):03d}** : {date_str} {tz_str}{dur_str}  #stream")

    if entry["no_yt"]:
        lines.append("\t`YT` ✗")
    else:
        yt_title = _get_title(yt_id, "youtube", cache, cache_entry, nas["yt_video"], ytdlp, config) if yt_id else None
        lines.append(_build_platform_line("YT", yt_id, "youtube", yt_title, nas["yt_video"], nas["yt_chat"], config))

    if entry["no_tw"]:
        lines.append("\t`TW` ✗")
    else:
        tw_title = _get_title(tw_id, "twitch", cache, cache_entry, nas["tw_video"], ytdlp, config) if tw_id else None
        lines.append(_build_platform_line("TW", tw_id, "twitch", tw_title, nas["tw_video"], nas["tw_chat"], config))

    for note in entry.get("notes", []):
        lines.append(note.rstrip("\n"))

    return lines


def _upsert_resolved(cache, index, platform, video_id, title, duration):
    """After resolving from any non-cache source, cement the mapping."""
    if not video_id:
        return
    tag = TAG[platform]
    fields = {f"{tag}_id": video_id}
    if title:
        fields[f"{tag}_title"] = title
    if duration:
        fields[f"{tag}_duration"] = duration
    cache.upsert(index, **fields)


# ══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD INTEGRATION (direct import, no subprocess)
# ══════════════════════════════════════════════════════════════════════════════

def _identify_missing(nas, yt_id, tw_id, config):
    missing = []
    if yt_id:
        url = ls_common.build_stream_url(config, "youtube", yt_id)
        if not nas["yt_video"]:
            missing.append({"platform": "youtube", "type": "video", "url": url, "label": "YT video"})
        if not nas["yt_chat"]:
            missing.append({"platform": "youtube", "type": "chat", "url": url, "label": "YT chat"})
    if tw_id:
        url = ls_common.build_stream_url(config, "twitch", tw_id)
        if not nas["tw_video"]:
            missing.append({"platform": "twitch", "type": "video", "url": url, "label": "TW video"})
        if not nas["tw_chat"]:
            missing.append({"platform": "twitch", "type": "chat", "url": url, "label": "TW chat"})
    return missing


def _offer_downloads(config, missing, index):
    """Use PostHocDownloader directly — no subprocess shell-out."""
    # Lazy import: ls-download.py has a dash, so import via importlib.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ls_download", os.path.join(SCRIPT_DIR, "ls-download.py")
    )
    if not spec or not spec.loader:
        print("  ⚠ ls-download.py not importable")
        return False
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    print("\n  Missing files:")
    for i, m in enumerate(missing):
        print(f"    {i+1}) {m['label']}: {m['url']}")

    print("\n  Enter numbers to download (e.g. 1 3), 'a' for all, or Enter to skip:")
    choice = input("  > ").strip().lower()
    if not choice:
        print("  Skipped.")
        return False

    if choice == "a":
        selected = missing
    else:
        try:
            indices = [int(x) - 1 for x in choice.split()]
            selected = [missing[i] for i in indices if 0 <= i < len(missing)]
        except ValueError:
            print("  ✗ Invalid input.")
            return False
    if not selected:
        print("  Nothing selected.")
        return False

    # Group selected by URL
    by_url = {}
    for m in selected:
        by_url.setdefault(m["url"], {"url": m["url"], "platform": m["platform"], "types": set()})
        by_url[m["url"]]["types"].add(m["type"])

    any_success = False
    downloader = module.PostHocDownloader(config, output_dir=config["nas_path"])
    for url, group in by_url.items():
        dl_type = "both" if len(group["types"]) > 1 else next(iter(group["types"]))
        plat = group["platform"].upper()
        print(f"\n  Downloading {plat} ({dl_type})...")
        try:
            result = downloader.download(url, prefix=str(index), download_type=dl_type)
            ok = (
                (dl_type == "both" and result["video_success"] and result["chat_success"])
                or (dl_type == "video" and result["video_success"])
                or (dl_type == "chat" and result["chat_success"])
            )
            if ok:
                print(f"  ✔ {plat} complete.")
                any_success = True
            else:
                print(f"  ✗ {plat} failed.")
        except Exception as e:
            print(f"  ✗ {plat} error: {e}")

    return any_success


# ══════════════════════════════════════════════════════════════════════════════
#  AUDIT FLOW
# ══════════════════════════════════════════════════════════════════════════════

def audit(config, obsidian, cache, ytdlp, twitch, index, yt_override=None, tw_override=None):
    print(f"\n{'='*60}")
    print(f"  Auditing entry #{index}")
    print(f"{'='*60}\n")

    # ── 1. Parse entry ──
    entry = obsidian.parse_entry(index)
    if not entry["found"]:
        print(f"  ✗ Entry #{index} not found in Obsidian file.")
        return
    if not entry["date_obj"]:
        print(f"  ✗ Could not parse date for #{index}")
        if entry["date_str"]:
            print(f"    Raw: {entry['date_str']}")
        return

    print(f"  Date     : {entry['date_str']} {entry.get('tz_str') or ''}")
    print(f"  Checkbox : {entry['checkbox']}")
    if entry["no_yt"]:
        print("  YouTube  : ✗ (no stream)")
    if entry["no_tw"]:
        print("  Twitch   : ✗ (no stream)")
    print()

    # ── 2. Scan NAS ──
    print("  Scanning NAS...")
    nas = scan_nas(config, index)
    for key in ("yt_video", "yt_chat", "tw_video", "tw_chat"):
        status = f"✔ {nas[key]}" if nas[key] else "✗ not found"
        print(f"    {status}")
    print()

    # ── 3. Resolve IDs ──
    yt_id, yt_src = (None, None) if entry["no_yt"] else resolve_id(
        "youtube", index, entry, nas, cache, ytdlp, twitch, config, yt_override
    )
    tw_id, tw_src = (None, None) if entry["no_tw"] else resolve_id(
        "twitch", index, entry, nas, cache, ytdlp, twitch, config, tw_override
    )

    print("  ID Resolution:")
    if not entry["no_yt"]:
        print(f"    YT: {yt_id or '—'}" + (f"  ← {yt_src}" if yt_src else ""))
    if not entry["no_tw"]:
        print(f"    TW: {tw_id or '—'}" + (f"  ← {tw_src}" if tw_src else ""))
    print()

    # ── 4. Build entry ──
    block = build_entry(config, index, entry, nas, cache, ytdlp, yt_id, tw_id)

    print("  ┌─ Reconstructed Entry ──────────────────────────────")
    for line in block:
        print(f"  │ {line}")
    print("  └────────────────────────────────────────────────────")
    print()

    # ── 5. Write ──
    if input("  Write to Obsidian? (y/n): ").strip().lower() == "y":
        if obsidian.write_entry(index, block):
            print("  ✔ Written.")
            # Cement resolved mappings into cache.
            for vid, plat in [(yt_id, "youtube"), (tw_id, "twitch")]:
                if vid:
                    cache_entry = cache.get(index)
                    title = _get_title(vid, plat, cache, cache_entry, nas[f"{TAG[plat]}_video"], ytdlp, config)
                    duration = _get_duration(vid, plat, cache, cache_entry)
                    _upsert_resolved(cache, index, plat, vid, title, duration)
            cache.save()
        else:
            print("  ✗ Write failed.")
    else:
        print("  Skipped.")
    print()

    # ── 6. Missing files → download ──
    missing = _identify_missing(nas, yt_id, tw_id, config)
    if not missing:
        print("  ✔ All files present.\n")
        return

    downloaded = _offer_downloads(config, missing, index)
    if not downloaded:
        return

    # Re-scan and rebuild after download
    print("\n  Re-scanning NAS...")
    nas = scan_nas(config, index)
    for key in ("yt_video", "yt_chat", "tw_video", "tw_chat"):
        status = f"✔ {nas[key]}" if nas[key] else "✗ still missing"
        print(f"    {status}")
    print()

    block = build_entry(config, index, entry, nas, cache, ytdlp, yt_id, tw_id)
    print("  ┌─ Updated Entry ────────────────────────────────────")
    for line in block:
        print(f"  │ {line}")
    print("  └────────────────────────────────────────────────────")
    print()

    if input("  Write to Obsidian? (y/n): ").strip().lower() == "y":
        if obsidian.write_entry(index, block):
            print("  ✔ Written.")
        else:
            print("  ✗ Write failed.")
    print()


# ══════════════════════════════════════════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Audit and reconstruct Obsidian livestream entries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  ls-audit 515                            Reconstruct entry #515
  ls-audit 515 --yt-id dQw4w9WgXcQ       Override YouTube ID for #515
  ls-audit 515 --tw-id 2345678901        Override Twitch ID for #515
  ls-audit --refresh                      Refresh both pools
  ls-audit --refresh youtube              Refresh YouTube pool only
  ls-audit --inject URL                   Inject a video (prompts for index)
  ls-audit --inject --manual              Manual injection
  ls-audit --cache-info 557               Look up cache entry by index
  ls-audit --cache-info dQw4w9WgXcQ       Look up by video ID
        """
    )
    parser.add_argument("index", nargs="?", type=int, help="Entry index to audit")
    parser.add_argument("--yt-id", help="Override YouTube video ID for this audit")
    parser.add_argument("--tw-id", help="Override Twitch video ID for this audit")
    parser.add_argument("--refresh", nargs="?", const="all",
                        choices=["all", "youtube", "twitch"],
                        help="Refresh stream pool")
    parser.add_argument("--inject", nargs="?", const="__prompt__", metavar="URL",
                        help="Inject a video into the cache")
    parser.add_argument("--manual", action="store_true",
                        help="Use manual input for --inject")
    parser.add_argument("--cache-info", metavar="ID",
                        help="Look up a cache entry by index or video ID")

    args = parser.parse_args()

    config = ls_common.load_config()
    cache = ls_common.StreamCache()
    ytdlp = ls_common.YtDlp(config)
    twitch = ls_common.TwitchApi(config)
    obsidian = ls_common.Obsidian(config)

    if args.refresh is not None:
        if args.refresh in ("all", "youtube"):
            cache.refresh_youtube(ytdlp, config, full=True)
        if args.refresh in ("all", "twitch"):
            cache.refresh_twitch(twitch, full=True)
        print("\n  ✔ Pool refreshed.")
        return

    if args.cache_info:
        cache_info(cache, args.cache_info)
        return

    if args.inject is not None:
        if args.manual or args.inject == "__prompt__":
            inject_video(config, ytdlp, cache, url=None)
        else:
            inject_video(config, ytdlp, cache, url=args.inject)
        return

    if args.index is None:
        parser.print_help()
        return

    audit(config, obsidian, cache, ytdlp, twitch, args.index,
          yt_override=args.yt_id, tw_override=args.tw_id)


if __name__ == "__main__":
    main()
