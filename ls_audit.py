#!/usr/bin/env python3
"""
ls-audit - Audit and reconstruct Obsidian livestream entries.

Usage:
    ls-audit <index>                        Reconstruct entry
    ls-audit <index> --yt-id ID             Override YouTube video ID
    ls-audit <index> --tw-id ID             Override Twitch video ID
    ls-audit --refresh [youtube|twitch]     Refresh VOD cache
    ls-audit --inject URL                   Add video to cache from URL
    ls-audit --inject --manual              Manually add to cache
    ls-audit --cache-info ID                Look up cached video by ID
"""

import os, re, glob, sys, json, shutil, subprocess, datetime, argparse, calendar, logging
from yt_dlp.utils import sanitize_filename

import ls_common
import ls_chat
import ls_archive


# ═══════════════════════════════════════════════════════════════════════════
#  MEDIA ANALYSIS  (video duration + chat stats)
# ═══════════════════════════════════════════════════════════════════════════

def _seconds_to_hhmmss(value) -> str:
    """Convert a numeric seconds value to HH:MM:SS. Returns 'UNKNOWN' on any failure."""
    try:
        secs = int(float(value))
        if secs < 0:
            # Negative offset (pre-stream YT chat) — show with leading minus
            h, rem = divmod(-secs, 3600)
            m, s = divmod(rem, 60)
            return f"-{h:02d}:{m:02d}:{s:02d}"
        h, rem = divmod(secs, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    except Exception:
        return "UNKNOWN"


def analyze_video_file(filepath: str) -> dict:
    """
    Return video duration via ffprobe.
    Result keys: duration_secs (float|None), duration_str (str).
    Never raises.
    """
    result = {"duration_secs": None, "duration_str": "UNKNOWN"}
    try:
        dur = ls_common.probe_duration(filepath)
        if dur is not None:
            result["duration_secs"] = dur
            result["duration_str"] = _seconds_to_hhmmss(dur)
    except Exception:
        pass
    return result


def _extract_yt_chat_timestamp_secs(entry: dict) -> float | None:
    """
    Pull videoOffsetTimeMsec from a yt-dlp live_chat JSONL entry.
    Returns seconds (may be negative for pre-stream), or None.

    The field lives at the top level of each JSONL object, not nested
    inside replayChatItemAction (which only contains the action payloads).
    """
    try:
        # Primary: top-level field (standard yt-dlp live_chat format)
        raw = entry.get("videoOffsetTimeMsec")
        if raw is not None:
            return int(raw) / 1000.0
        # Fallback: some older recordings nest it differently
        raw = entry.get("replayChatItemAction", {}).get("videoOffsetTimeMsec")
        if raw is not None:
            return int(raw) / 1000.0
    except Exception:
        pass
    return None


def analyze_chat_file(filepath: str) -> dict:
    """
    Analyze a chat JSON/JSONL file.

    Supports:
      • Twitch: JSON array, ``timestamp`` field in **microseconds** relative
        to stream start (produced by ls_common.record_twitch_chat).
      • YouTube: JSONL, ``replayChatItemAction.videoOffsetTimeMsec`` in
        **milliseconds** relative to video start (yt-dlp live_chat format).

    Result keys:
      count       – int or "UNKNOWN"
      first_ts    – "HH:MM:SS" of earliest message (or "UNKNOWN")
      last_ts     – "HH:MM:SS" of latest  message (or "UNKNOWN")
      last_secs   – float or None, raw offset of the latest message
      format      – "twitch" | "youtube" | "unknown"

    Never raises; any parse failure replaces the affected value with "UNKNOWN".
    """
    result: dict = {
        "count": "UNKNOWN",
        "first_ts": "UNKNOWN",
        "last_ts": "UNKNOWN",
        "last_secs": None,
        "format": "unknown",
    }

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read().strip()
        if not raw:
            result["count"] = 0
            return result

        messages: list[dict] = []
        timestamps: list[float] = []

        # ── Try Twitch: well-formed JSON array ────────────────────────────
        parsed_as_array = False
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                parsed_as_array = True
                result["format"] = "twitch"
                for msg in data:
                    if not isinstance(msg, dict):
                        continue
                    messages.append(msg)
                    ts_raw = msg.get("timestamp")
                    if ts_raw is not None:
                        try:
                            timestamps.append(int(ts_raw) / 1_000_000.0)
                        except Exception:
                            pass
        except json.JSONDecodeError:
            pass

        # ── Try YouTube: JSONL ─────────────────────────────────────────────
        if not parsed_as_array:
            result["format"] = "youtube"
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if isinstance(entry, dict):
                        messages.append(entry)
                        secs = _extract_yt_chat_timestamp_secs(entry)
                        if secs is not None:
                            timestamps.append(secs)
                except (json.JSONDecodeError, Exception):
                    continue

        result["count"] = len(messages)

        if timestamps:
            result["last_secs"] = max(timestamps)
            result["first_ts"] = _seconds_to_hhmmss(min(timestamps))
            result["last_ts"]  = _seconds_to_hhmmss(result["last_secs"])

    except Exception:
        pass

    return result


def _print_media_analysis(config: dict, nas: dict):
    """
    Print ffmpeg duration and chat stats for all files found on NAS.
    Called after the NAS scan table inside audit(). Never raises.
    """
    nas_root = config.get("nas_path", "")
    rows = [
        ("yt_video", "YT video"),
        ("yt_chat",  "YT chat "),
        ("tw_video", "TW video"),
        ("tw_chat",  "TW chat "),
    ]

    if not any(nas.get(k) for k, _ in rows):
        return

    print("  Media analysis:")

    for key, label in rows:
        filename = nas.get(key)
        if not filename:
            print(f"    {label} : —")
            continue
        filepath = os.path.join(nas_root, filename)
        if not os.path.exists(filepath):
            print(f"    {label} : ⚠ file missing from disk")
            continue

        ext = os.path.splitext(filename)[1].lower()
        if ext in ls_common.VIDEO_EXTS:
            info = analyze_video_file(filepath)
            print(f"    {label} : {info['duration_str']}")

        elif ext == ".json":
            info = analyze_chat_file(filepath)
            count   = info["count"]
            first   = info["first_ts"]
            last    = info["last_ts"]
            count_s = str(count).rjust(4) if isinstance(count, int) else count
            print(f"    {label} : {count_s} messages  ({first} → {last})")

    print()


# ═══════════════════════════════════════════════════════════════════════════
#  NAS SCANNER
# ═══════════════════════════════════════════════════════════════════════════

def scan_nas(config: dict, index: int) -> dict:
    """Scan NAS for files matching this index prefix.

    Returns dict with yt_video, yt_chat, tw_video, tw_chat filenames.
    """
    found = {
        "yt_video": None, "yt_chat": None,
        "tw_video": None, "tw_chat": None,
    }
    nas = config["nas_path"]
    if not os.path.exists(nas):
        print("  ⚠ NAS not mounted")
        return found

    idx_padded = f"{int(index):03d}"
    patterns = [f"{idx_padded}_*"]
    if str(index) != idx_padded:
        patterns.append(f"{index}_*")

    seen: set[str] = set()
    for pat in patterns:
        for filepath in glob.glob(os.path.join(nas, pat)):
            filename = os.path.basename(filepath)
            if filename in seen:
                continue
            seen.add(filename)

            # Skip intermediate fragment files like title.f140.m4a
            if re.search(r"\.f\d+\.\w+$", filename):
                continue
            # Only accept files whose numeric prefix matches exactly
            m = re.match(r"^(\d+)_", filename)
            if not m or int(m.group(1)) != int(index):
                continue

            vid = ls_common.extract_video_id_from_filename(filename)
            if not vid:
                continue

            platform = ls_common.classify_video_id(vid)
            ext = os.path.splitext(filename)[1].lower()
            prefix = "yt" if platform == "youtube" else "tw"

            if ext in ls_common.VIDEO_EXTS:
                existing = found[f"{prefix}_video"]
                # Prefer mp4 if multiple recordings exist
                if not existing or (ext == ".mp4" and not existing.lower().endswith(".mp4")):
                    found[f"{prefix}_video"] = filename
            elif ext == ".json" and not ls_chat.is_derived(filename):
                found[f"{prefix}_chat"] = filename

    return found


# ═══════════════════════════════════════════════════════════════════════════
#  ID RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════
#
#  Priority: CLI override → entry URL → NAS filename → cache (by index)
#            → cache (by date, with auto-refresh if stale)

def resolve_id(config: dict, cache: list[dict], platform: str,
               entry: dict, nas: dict,
               cli_override: str | None = None) -> tuple[str | None, str | None]:
    """Resolve video ID for a platform. Returns (video_id, source_label)."""
    tag = "yt" if platform == "youtube" else "tw"

    # 1. CLI override
    if cli_override:
        return cli_override, "cli"

    # 2. URL already in obsidian entry
    entry_id = entry.get(f"{tag}_id")
    if entry_id:
        return entry_id, "entry"

    # 3. NAS filename
    nas_file = nas.get(f"{tag}_video")
    if nas_file:
        vid = ls_common.extract_video_id_from_filename(nas_file)
        if vid:
            return vid, "nas"

    # 4. Cache by obsidian_index
    target_index = entry.get("_index")
    if target_index is not None:
        for vod in cache:
            if (vod.get("platform") == platform
                    and vod.get("obsidian_index") == int(target_index)):
                return vod["id"], "cache (index)"

    # 5. Cache by date (auto-refresh if stale)
    if entry["date_obj"]:
        target_index = entry.get("_index")

        def _match():
            return ls_common.find_vod_by_date(
                cache, platform, entry["date_obj"], claim_index=target_index,
            )

        vod = _match()
        if vod is None:
            newest_dates = [
                v.get("start_time", "")[:10]
                for v in cache if v.get("platform") == platform
            ]
            newest = max(newest_dates) if newest_dates else None
            target_date = entry["date_obj"].strftime("%Y-%m-%d")
            if newest is None or target_date > newest:
                print(f"  ⌛ Refreshing {platform} cache...")
                if platform == "youtube":
                    ls_common.refresh_youtube_cache(config, cache, full=(newest is None))
                else:
                    ls_common.refresh_twitch_cache(config, cache, full=(newest is None))
                ls_common.save_cache(cache)
                vod = _match()

        if vod:
            label = "cache (date)"
            try:
                vdt = (datetime.datetime
                       .fromisoformat(vod["start_time"].replace("Z", "+00:00"))
                       .replace(tzinfo=None))
                delta = abs(vdt - entry["date_obj"])
                if delta > datetime.timedelta(minutes=5):
                    label += f" (~{int(delta.total_seconds() // 60)}m off)"
            except Exception:
                pass
            return vod["id"], label

    return None, None


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def _title_from_filename(filename: str) -> str:
    """Extract clean title from NAS filename."""
    name = os.path.splitext(filename)[0]
    name = re.sub(r"^\d+_", "", name)
    name = re.sub(r"\s*\[[^\]]+\]\s*@\s*\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$", "", name)
    return name


def _get_title(config: dict, cache: list[dict], video_id: str,
               platform: str, nas_file: str | None) -> str | None:
    """Resolve a display title: cache → NAS filename → API fetch."""
    # Cache
    vod = ls_common.find_vod(cache, video_id, platform)
    if vod and vod.get("title"):
        return vod["title"]
    # NAS filename
    if nas_file:
        return _title_from_filename(nas_file)
    # Fetch from API (and opportunistically cache it)
    try:
        url = ls_common.build_stream_url(config, platform, video_id)
        data = ls_common.ytdlp_probe(config, url, playlist_items="1")
        if data:
            title = data.get("title") or data.get("description")
            if title:
                ls_common.upsert_vod(cache, {
                    "id": video_id, "platform": platform, "title": title,
                    "start_time": data.get("upload_date", ""),
                })
                return title
    except Exception:
        pass
    return None


def _build_platform_line(config: dict, tag: str, video_id: str | None,
                         platform: str, title: str | None,
                         video_file: str | None,
                         chat_file: str | None,
                         video_x: bool = False,
                         chat_x: bool = False) -> str:
    if video_file:
        vid_link = f"[📁]({ls_common.build_shell_cmd(config, video_file)})"
    elif video_x or chat_file:        # explicit, or implied (chat but no video)
        vid_link = "[📁.×]()"
    else:
        vid_link = "[📁]()"

    if chat_file:
        chat_link = f"[📄]({ls_common.build_shell_cmd(config, chat_file)})"
    elif chat_x:
        chat_link = "[📄.×]()"
    else:
        chat_link = "[📄]()"

    display = title or "untitled"
    url = ls_common.build_stream_url(config, platform, video_id) if video_id else ""
    return f"\t`{tag}` {vid_link} {chat_link} [ {display} ]({url})"


def build_entry(config: dict, cache: list[dict], index: int,
                entry: dict, nas: dict,
                yt_id: str | None, tw_id: str | None) -> list[str]:
    """Assemble the full Obsidian entry block from resolved data."""
    lines = []

    # Header: checkbox, index, date, timezone, duration
    date_str = entry["date_str"] or "UNKNOWN"
    tz_str = entry["tz_str"] or "(GMT-6)"

    # Duration: longest of the two platforms.
    # ffprobe the file actually held, and only fall back to the cache when
    # there is no file. The cache is written once at record time, so after a
    # re-download or a manual repair it reports the old length forever.
    durations = []
    for prefix, vid_id, plat in [("yt", yt_id, "youtube"),
                                 ("tw", tw_id, "twitch")]:
        measured = None
        filename = nas.get(f"{prefix}_video")
        if filename:
            path = os.path.join(config.get("nas_path", ""), filename)
            if os.path.exists(path):
                measured = analyze_video_file(path)["duration_secs"]

        vod = ls_common.find_vod(cache, vid_id, plat) if vid_id else None
        if measured:
            durations.append(measured)
            if vod and abs((vod.get("duration") or 0) - measured) > 5:
                print(f"    duration corrected from cache "
                      f"{_seconds_to_hhmmss(vod.get('duration') or 0)} → "
                      f"{_seconds_to_hhmmss(measured)} ({prefix})")
                vod["duration"] = int(measured)
        elif vod and vod.get("duration"):
            durations.append(vod["duration"])
    if durations:
        dur = max(durations)
        h, rem = divmod(int(dur), 3600)
        m, s = divmod(rem, 60)
        dur_str = f" [{h:02d}:{m:02d}:{s:02d}]"
    elif entry.get("duration_str"):
        dur_str = f" [{entry['duration_str']}]"
    else:
        dur_str = ""

    lines.append(
        f"- {entry['checkbox']} **{int(index):03d}** : "
        f"{date_str} {tz_str}{dur_str}  #stream"
    )

    # YouTube line
    if entry["no_yt"]:
        lines.append("\t`YT` ✗")
    else:
        yt_title = (_get_title(config, cache, yt_id, "youtube", nas["yt_video"])
                    if yt_id else None)
        lines.append(_build_platform_line(
            config, "YT", yt_id, "youtube", yt_title,
            nas["yt_video"], nas["yt_chat"],
            video_x=entry.get("yt_video_x", False),
            chat_x=entry.get("yt_chat_x", False),
        ))

    # Twitch line
    if entry["no_tw"]:
        lines.append("\t`TW` ✗")
    else:
        tw_title = (_get_title(config, cache, tw_id, "twitch", nas["tw_video"])
                    if tw_id else None)
        lines.append(_build_platform_line(
            config, "TW", tw_id, "twitch", tw_title,
            nas["tw_video"], nas["tw_chat"],
            video_x=entry.get("tw_video_x", False),
            chat_x=entry.get("tw_chat_x", False),
        ))

    # User notes (preserved verbatim)
    for note in entry.get("notes", []):
        lines.append(note.rstrip("\n"))

    return lines


# ═══════════════════════════════════════════════════════════════════════════
#  DOWNLOADS
# ═══════════════════════════════════════════════════════════════════════════

def _identify_missing(config: dict, nas: dict,
                      yt_id: str | None, tw_id: str | None,
                      absent: dict | None = None) -> list[dict]:
    """List files that should exist but don't, skipping known-absent (.×) ones."""
    absent = absent or {}
    missing = []
    if yt_id:
        url = ls_common.build_stream_url(config, "youtube", yt_id)
        if not nas["yt_video"] and not absent.get("yt_video"):
            missing.append({"platform": "youtube", "type": "video",
                            "url": url, "label": "YT video"})
        if not nas["yt_chat"] and not absent.get("yt_chat"):
            missing.append({"platform": "youtube", "type": "chat",
                            "url": url, "label": "YT chat"})
    if tw_id:
        url = ls_common.build_stream_url(config, "twitch", tw_id)
        if not nas["tw_video"] and not absent.get("tw_video"):
            missing.append({"platform": "twitch", "type": "video",
                            "url": url, "label": "TW video"})
        if not nas["tw_chat"] and not absent.get("tw_chat"):
            missing.append({"platform": "twitch", "type": "chat",
                            "url": url, "label": "TW chat"})
    return missing


def _download_files(config: dict, missing: list[dict],
                    index: int) -> bool:
    """Offer interactive download of missing files. Returns True if any succeeded."""
    print("\n  Missing files:")
    for i, m in enumerate(missing, 1):
        print(f"    {i}) {m['label']}: {m['url']}")

    choice = input(
        "\n  Download (numbers / 'a' for all / Enter to skip): "
    ).strip().lower()
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

    nas_path = config["nas_path"]
    any_success = False

    for m in selected:
        url = m["url"]
        platform = m["platform"]
        dl_type = m["type"]

        # Probe for filename construction
        data = ls_common.ytdlp_probe(config, url, playlist_items="1")
        if data:
            title = data.get("title") or "Unknown"
            vid = data.get("id", "unknown")
            release_ts = data.get("release_timestamp")
            upload_date = data.get("upload_date", "")
            if release_ts:
                ts = datetime.datetime.fromtimestamp(release_ts).strftime(
                    "%Y-%m-%d_%H-%M",
                )
            elif upload_date:
                ts = (f"{upload_date[:4]}-{upload_date[4:6]}"
                      f"-{upload_date[6:]}_00-00")
            else:
                ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            safe_title = sanitize_filename(f"{title} [{vid}] @ {ts}")
        else:
            safe_title = sanitize_filename(f"unknown @ {datetime.datetime.now()}")

        safe_title = f"{int(index):03d}_{safe_title}"
        print(f"\n  ↓ {m['label']}: {safe_title}")

        if dl_type == "video":
            cmd = ls_common.ytdlp_vod_cmd(
                config, url, f"{safe_title}.%(ext)s",
            )
            subprocess.run(cmd, cwd=nas_path)
            any_success = True

        elif dl_type == "chat":
            tdl = config.get("twitch_downloader_cli")
            if platform == "twitch" and tdl and os.path.exists(tdl):
                vod_id = url.rstrip("/").split("/")[-1]
                subprocess.run([
                    tdl, "chatdownload", "--id", vod_id,
                    "-o", os.path.join(nas_path, f"{safe_title}.json"),
                ])
            else:
                cmd = ls_common.ytdlp_chat_cmd(
                    config, url, f"{safe_title}.%(ext)s",
                )
                subprocess.run(cmd, cwd=nas_path)
                # Rename .live_chat.json → .json
                lc = os.path.join(nas_path, f"{safe_title}.live_chat.json")
                final = os.path.join(nas_path, f"{safe_title}.json")
                if os.path.exists(lc):
                    os.rename(lc, final)
            any_success = True

    return any_success


# ═══════════════════════════════════════════════════════════════════════════
#  CACHE MANAGEMENT COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

def cmd_refresh(config: dict, platform: str):
    cache = ls_common.load_cache()
    if platform in ("all", "youtube"):
        print("  ⌛ Refreshing YouTube...")
        if ls_common.refresh_youtube_cache(config, cache, full=True):
            n = sum(1 for v in cache if v.get("platform") == "youtube")
            print(f"  ✔ YouTube: {n} VODs")
    if platform in ("all", "twitch"):
        print("  ⌛ Refreshing Twitch...")
        if ls_common.refresh_twitch_cache(config, cache, full=True):
            n = sum(1 for v in cache if v.get("platform") == "twitch")
            print(f"  ✔ Twitch: {n} VODs")
    ls_common.save_cache(cache)
    print("  ✔ Cache saved.")


def cmd_inject(config: dict, url: str | None = None):
    cache = ls_common.load_cache()
    if url:
        print(f"  ⌛ Fetching: {url}")
        data = ls_common.ytdlp_probe(config, url)
        if not data:
            print("  ⚠ Failed. Falling back to manual.")
            return _inject_manual(cache)

        platform = "twitch" if "twitch.tv" in url else "youtube"
        release_ts = data.get("release_timestamp")
        upload_date = data.get("upload_date", "")
        if release_ts:
            start = datetime.datetime.fromtimestamp(release_ts).isoformat()
        elif upload_date:
            start = datetime.datetime.strptime(upload_date, "%Y%m%d").isoformat()
        else:
            start = datetime.datetime.now().isoformat()

        vod = {
            "id":         data.get("id", "unknown"),
            "platform":   platform,
            "title":      data.get("title", "Unknown"),
            "start_time": start,
            "channel":    data.get("channel") or data.get("uploader") or "unknown",
            "duration":   data.get("duration"),
        }
    else:
        return _inject_manual(cache)

    _print_vod(vod)
    if input("\n  Add to cache? (y/n): ").strip().lower() == "y":
        ls_common.upsert_vod(cache, vod)
        ls_common.save_cache(cache)
        print("  ✔ Added.")


def _inject_manual(cache: list[dict]):
    """Interactive manual cache injection."""
    print("\n  Manual entry:")
    platform = input("  Platform (youtube/twitch): ").strip().lower()
    if platform not in ("youtube", "twitch"):
        print("  ✗ Invalid platform.")
        return
    vid_id = input("  Video ID: ").strip()
    if not vid_id:
        print("  ✗ ID required.")
        return
    title = input("  Title: ").strip() or "Unknown"
    date_str = input("  Start date (YYYY-MM-DD or ISO): ").strip()
    try:
        start = (date_str if "T" in date_str
                 else datetime.datetime.strptime(date_str, "%Y-%m-%d").isoformat())
    except ValueError:
        print("  ✗ Bad date format.")
        return
    dur = input("  Duration in seconds (Enter to skip): ").strip()
    channel = input("  Channel: ").strip() or "unknown"

    vod = {
        "id": vid_id, "platform": platform, "title": title,
        "start_time": start, "channel": channel,
        "duration": int(dur) if dur.isdigit() else None,
    }
    _print_vod(vod)
    if input("\n  Add to cache? (y/n): ").strip().lower() == "y":
        ls_common.upsert_vod(cache, vod)
        ls_common.save_cache(cache)
        print("  ✔ Added.")


def cmd_cache_info(vid_id: str):
    cache = ls_common.load_cache()
    vod = ls_common.find_vod(cache, vid_id)
    if vod:
        _print_vod(vod)
    else:
        print(f"  ✗ '{vid_id}' not in cache.")


def _print_vod(vod: dict):
    dur = vod.get("duration")
    if dur:
        dur_str = f"{dur}s ({dur // 3600}h{(dur % 3600) // 60:02d}m)"
    else:
        dur_str = "unknown"
    print(f"\n  Platform : {vod.get('platform')}")
    print(f"  ID       : {vod.get('id')}")
    print(f"  Title    : {vod.get('title')}")
    print(f"  Start    : {vod.get('start_time')}")
    print(f"  Duration : {dur_str}")
    print(f"  Channel  : {vod.get('channel', 'unknown')}")
    idx = vod.get("obsidian_index")
    if idx is not None:
        print(f"  Index    : #{idx}")


# ═══════════════════════════════════════════════════════════════════════════
#  TIMINGS SIDECAR
# ═══════════════════════════════════════════════════════════════════════════
#
#  Two instants per platform: when the broadcast started, and when we started
#  recording it. New recordings have both in the cache. Older ones are
#  reconstructed, best source first, and every value carries where it came
#  from and how accurate it is -- an unattributed timestamp is worse than a
#  missing one.

FILENAME_TS_RE = re.compile(r"@\s*(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})")


def _filename_epoch_ms(filename: str) -> int | None:
    """The `@ YYYY-MM-DD_HH-MM` stamp, to the minute."""
    m = FILENAME_TS_RE.search(filename or "")
    if not m:
        return None
    try:
        return int(datetime.datetime.strptime(
            m.group(1), "%Y-%m-%d_%H-%M").timestamp() * 1000)
    except ValueError:
        return None


def _log_record_start(config: dict, nas_file: str) -> int | None:
    """
    Scan the recorder log for this stream's first part.

    The log is written to the daemon's working directory, so it is only found
    if ls-audit runs from the same place. It also rolls over, covering the
    last few dozen recordings at most.
    """
    stem = re.sub(r"^\d+_", "", os.path.splitext(nas_file)[0])
    for cand in (config.get("log_file"),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "livestream_recorder.log"),
                 "livestream_recorder.log"):
        if not cand or not os.path.exists(cand):
            continue
        pat = re.compile(r"^([\d\-]{10} [\d:]{8}),\d+ .*Part 01 started: "
                         + re.escape(stem))
        try:
            with open(cand, encoding="utf-8", errors="replace") as f:
                for line in f:
                    m = pat.match(line)
                    if m:
                        return int(datetime.datetime.strptime(
                            m.group(1), "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        except OSError:
            continue
    return None


def _iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000).isoformat(timespec="seconds")


def _platform_timings(config: dict, cache: list[dict], nas: dict,
                      prefix: str, platform: str) -> dict | None:
    """Best-effort timings for one platform, with provenance on every value."""
    nas_root = config.get("nas_path", "")
    chat_file = nas.get(f"{prefix}_chat")
    video_file = nas.get(f"{prefix}_video")
    if not (chat_file or video_file):
        return None

    vid = ls_common.extract_video_id_from_filename(chat_file or video_file)
    vod = (ls_common.find_vod(cache, vid, platform) or {}) if vid else {}

    stream_ms = stream_src = None
    record_ms = record_src = None

    # 1. cache — written at record time, exact
    if vod.get("stream_start_epoch_ms"):
        stream_ms, stream_src = vod["stream_start_epoch_ms"], "cache"
    if vod.get("record_start_epoch_ms"):
        record_ms, record_src = vod["record_start_epoch_ms"], "cache"

    # 2. the chat file itself — exact, and works for the whole back catalogue.
    #    What the zero means depends on the format, so trust the source label.
    if chat_file and (stream_ms is None or record_ms is None):
        zero, zsrc = ls_chat.peek_zero(os.path.join(nas_root, chat_file))
        if zero is not None:
            if zsrc in ("yt:timestampUsec", "tdc:created_at") and stream_ms is None:
                stream_ms, stream_src = zero, f"chat ({zsrc})"
            elif zsrc == "irc:tmi_sent_ts" and record_ms is None:
                record_ms, record_src = zero, f"chat ({zsrc})"

    # 3. recorder log — second accurate, but only the recent past
    if record_ms is None and (video_file or chat_file):
        hit = _log_record_start(config, video_file or chat_file)
        if hit:
            record_ms, record_src = hit, "log"

    # 4. filename — minute only, and ambiguous: the recorder stamps the
    #    detection time, an ls-audit re-download stamps the broadcast start.
    #    Only usable as a record start once it is clearly not the latter.
    fname_ms = _filename_epoch_ms(chat_file or video_file)
    if record_ms is None and fname_ms is not None:
        if stream_ms is None or abs(fname_ms - stream_ms) > 60_000:
            record_ms, record_src = fname_ms, "filename (minute)"

    duration = vod.get("duration")
    if video_file:
        vp = os.path.join(nas_root, video_file)
        if os.path.exists(vp):
            duration = analyze_video_file(vp).get("duration_secs") or duration

    def acc(src):
        if src is None:
            return None
        return "minute" if "filename" in src else "exact"

    return {
        "video_id": vid,
        "stream_start_epoch_ms": stream_ms,
        "stream_start_iso": _iso(stream_ms),
        "stream_start_source": stream_src,
        "stream_start_accuracy": acc(stream_src),
        "record_start_epoch_ms": record_ms,
        "record_start_iso": _iso(record_ms),
        "record_start_source": record_src,
        "record_start_accuracy": acc(record_src),
        "duration_secs": duration,
        "filename_epoch_ms": fname_ms,
        "files": {"video": video_file, "chat": chat_file},
    }


def cmd_timings(config: dict, index: int, output: str | None = None,
                dry_run: bool = False):
    """Write a timings sidecar for one entry."""
    print(f"\n{'=' * 60}")
    print(f"  Timings for entry #{index}")
    print(f"{'=' * 60}")

    nas = scan_nas(config, index)
    cache = ls_common.load_cache()

    doc = {"schema": 1, "index": int(index),
           "generated_at": datetime.datetime.now().isoformat(timespec="seconds")}
    any_found = False

    for prefix, platform in (("yt", "youtube"), ("tw", "twitch")):
        t = _platform_timings(config, cache, nas, prefix, platform)
        if not t:
            print(f"  {platform:<8} no files")
            continue
        any_found = True
        doc[platform] = t
        print(f"  {platform}")
        for label, key in (("stream start", "stream_start"),
                           ("record start", "record_start")):
            iso, src = t[f"{key}_iso"], t[f"{key}_source"]
            if iso:
                print(f"    {label}  {iso}  [{src}]")
            else:
                print(f"    {label}  UNKNOWN")
        if t["duration_secs"]:
            print(f"    duration      {_seconds_to_hhmmss(t['duration_secs'])}")

    if not any_found:
        print("\n  Nothing to record.\n")
        return

    if dry_run:
        print("\n  --dry-run: nothing written.\n")
        return

    if not output:
        src_name = (doc.get("youtube") or doc.get("twitch"))["files"]
        stem = _title_from_filename(src_name["chat"] or src_name["video"])
        output = os.path.join(config.get("nas_path", ""),
                              f"{int(index):03d}_{stem}.meta.json")
    with open(output, "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)
    print(f"\n  ✔ {output}\n")


# ═══════════════════════════════════════════════════════════════════════════
#  YOUTUBE CHAT COVERAGE
# ═══════════════════════════════════════════════════════════════════════════
#
#  A live_chat writer that dies mid-stream leaves a chat that just stops --
#  two hours of an eight hour VOD. Flagged when the shortfall exceeds an hour
#  or half the video, whichever is smaller, so short streams are judged
#  proportionally and long ones by an absolute bar.

CHAT_SHORTFALL_MAX_SECS = 3600
CHAT_SHORTFALL_FRACTION = 0.5


def _yt_chat_shortfall(config: dict, cache: list[dict], nas: dict,
                       yt_id: str | None) -> dict | None:
    """Return details if the YouTube chat stops well short of the video."""
    chat_file = nas.get("yt_chat")
    if not chat_file:
        return None
    chat_path = os.path.join(config.get("nas_path", ""), chat_file)
    if not os.path.exists(chat_path):
        return None

    info = analyze_chat_file(chat_path)
    last = info.get("last_secs")
    if last is None or not isinstance(info["count"], int) or not info["count"]:
        return None

    # ffprobe first: it measures the file actually held, whereas the cache
    # holds the published length.
    duration = None
    video_file = nas.get("yt_video")
    if video_file:
        vp = os.path.join(config.get("nas_path", ""), video_file)
        if os.path.exists(vp):
            duration = analyze_video_file(vp).get("duration_secs")
    if not duration and yt_id:
        vod = ls_common.find_vod(cache, yt_id, "youtube") or {}
        duration = vod.get("duration")
    if not duration or duration <= 0:
        return None

    shortfall = duration - last
    limit = min(CHAT_SHORTFALL_MAX_SECS, duration * CHAT_SHORTFALL_FRACTION)
    if shortfall <= limit:
        return None

    return {"chat_file": chat_file, "chat_path": chat_path, "video_id": yt_id,
            "count": info["count"], "last_secs": last,
            "duration_secs": duration, "shortfall_secs": shortfall,
            "limit_secs": limit}


def _backfill_yt_chat(config: dict, item: dict) -> bool:
    """
    Download the post-hoc chat and merge it into the live capture.

    The merge is refused if it would shrink the file, so a bad download
    cannot destroy the partial capture already held.
    """
    nas_path = config["nas_path"]
    if not item["video_id"]:
        print("  ✗ No YouTube ID; cannot backfill.")
        return False

    live = item["chat_path"]
    base = os.path.splitext(item["chat_file"])[0]
    posthoc = os.path.join(nas_path, f"{base}.posthoc.json")
    written = os.path.join(nas_path, f"{base}.posthoc.live_chat.json")

    print(f"\n  ↓ Post-hoc chat: {base}.posthoc.json")
    url = ls_common.build_stream_url(config, "youtube", item["video_id"])
    subprocess.run(ls_common.ytdlp_chat_cmd(
        config, url, f"{base}.posthoc.%(ext)s"), cwd=nas_path)

    if os.path.exists(written):
        os.rename(written, posthoc)
    if not os.path.exists(posthoc):
        print("  ✗ No post-hoc chat produced (replay chat may be disabled).")
        return False

    def _lines(path):
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                return sum(1 for ln in f if ln.strip())
        except OSError:
            return 0

    tmp = live + ".merging"
    r = subprocess.run([sys.executable,
                        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "merge_yt_chats.py"),
                        live, posthoc, "-o", tmp])

    if r.returncode == 0 and os.path.exists(tmp) and _lines(tmp) >= _lines(live):
        print(f"  merged {_lines(live):,} + {_lines(posthoc):,} "
              f"→ {_lines(tmp):,} lines")
        os.replace(tmp, live)
        os.remove(posthoc)
        print(f"  ✔ {item['chat_file']}")
        return True

    if os.path.exists(tmp):
        os.remove(tmp)
    print("  ✗ Merge failed or would shrink the file; both kept for manual merge.")
    return False


def _offer_chat_backfill(config: dict, item: dict,
                         interactive: bool = True) -> bool:
    print("\n  ⚠ YouTube chat looks truncated:")
    print(f"      {item['count']:,} messages, ending at "
          f"{_seconds_to_hhmmss(item['last_secs'])} of "
          f"{_seconds_to_hhmmss(item['duration_secs'])}")
    print(f"      short by {_seconds_to_hhmmss(item['shortfall_secs'])} "
          f"(flags above {_seconds_to_hhmmss(item['limit_secs'])})")

    if not interactive:
        return _backfill_yt_chat(config, item)
    if input("\n  Download post-hoc chat and merge? [y/N]: ").strip().lower() \
            not in ("y", "yes"):
        print("  Skipped.")
        return False
    return _backfill_yt_chat(config, item)


# ═══════════════════════════════════════════════════════════════════════════
#  MERGE CHAT
# ═══════════════════════════════════════════════════════════════════════════

def _cache_zeros(cache: list[dict], yt_id: str | None,
                 tw_id: str | None) -> dict:
    """
    Known zeros for captures that carry none, from the recording cache.

    Twitch IRC offsets are relative to when the recorder attached, YouTube's
    videoOffsetTimeMsec to the broadcast start. TDC dumps and post-tmi_sent_ts
    captures have their own zero, so these are only ever a fallback.
    """
    out = {}
    if yt_id:
        vod = ls_common.find_vod(cache, yt_id, "youtube") or {}
        if vod.get("stream_start_epoch_ms"):
            out["youtube"] = vod["stream_start_epoch_ms"]
    if tw_id:
        vod = ls_common.find_vod(cache, tw_id, "twitch") or {}
        if vod.get("record_start_epoch_ms"):
            out["twitch"] = vod["record_start_epoch_ms"]
    return out


def _archive_raw_chats(config: dict, sources: list[str], res: dict) -> set[str]:
    """
    Move raw captures to deep storage once the merge holds them.

    A raw is only moved if its platform actually landed messages in the
    output and the source had a real zero -- otherwise the merge dropped
    content and the raw is the only copy of it.

    Returns the platforms whose raw left, so the caller can tell the archive
    to stop pointing at a file that is no longer where it says.
    """
    moved: set[str] = set()
    dest = config.get("chat_archive_path")
    if not dest:
        print("  Raw chats left in place (set chat_archive_path to archive).")
        return moved

    placed: dict[str, int] = {}
    for m in res["messages"]:
        placed[m["origin"]] = placed.get(m["origin"], 0) + 1
    by_name = {os.path.basename(p): p for p in sources}

    os.makedirs(dest, exist_ok=True)
    for src in res["metadata"]["sources"]:
        path = by_name.get(src["file"])
        if not path:
            continue
        if src["zero_ms"] is None:
            print(f"  ⚠ {src['file']}: no zero, messages unplaced — keeping")
            continue
        if not placed.get(src["platform"]):
            print(f"  ⚠ {src['file']}: contributed nothing — keeping")
            continue
        target = os.path.join(dest, src["file"])
        if os.path.exists(target):
            print(f"  ⚠ {src['file']}: already in archive — keeping")
            continue
        try:
            shutil.move(path, target)
            moved.add(src["platform"])
            print(f"  → archived {src['file']}")
        except OSError as e:
            print(f"  ✗ archive failed for {src['file']}: {e}")
    return moved


def cmd_merge_chat(config: dict, index: int, ref="youtube",
                   zeros: list | None = None, output: str | None = None,
                   dry_run: bool = False, keep_raw: bool = False):
    """Merge this entry's chat captures into one origin-tagged file."""
    nas_root = config.get("nas_path", "")
    print(f"\n{'=' * 60}")
    print(f"  Merging chat for entry #{index}")
    print(f"{'=' * 60}")

    nas = scan_nas(config, index)
    sources = []
    for key, label in (("yt_chat", "YT"), ("tw_chat", "TW")):
        filename = nas.get(key)
        path = os.path.join(nas_root, filename) if filename else None
        if path and os.path.exists(path):
            print(f"  {label} chat : {filename}")
            sources.append(path)
        else:
            print(f"  {label} chat : not found")

    if not sources:
        print("\n  No chat files to merge.\n")
        return

    # IDs come straight off the filenames being merged, so the cache lookup
    # is guaranteed to describe these exact captures.
    cache = ls_common.load_cache()
    fallback = _cache_zeros(
        cache,
        ls_common.extract_video_id_from_filename(nas["yt_chat"]) if nas.get("yt_chat") else None,
        ls_common.extract_video_id_from_filename(nas["tw_chat"]) if nas.get("tw_chat") else None)
    if fallback:
        print("  cache zeros: " + ", ".join(
            f"{k}={v}" for k, v in sorted(fallback.items())))
    print()

    try:
        res = ls_chat.merge(sources, ref=ref,
                            zeros=ls_chat.parse_zero_args(zeros),
                            fallback_zeros=fallback)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"  ✗ {e}\n")
        return

    md = res["metadata"]
    for src in md["sources"]:
        when = (datetime.datetime.fromtimestamp(src["zero_ms"] / 1000)
                .strftime("%Y-%m-%d %H:%M:%S") if src["zero_ms"] else "UNKNOWN")
        print(f"  {src['platform']:<8} {src['messages']:>7,}  "
              f"zero {when} ({src['zero_source']})")
    print(f"  merged   {md['messages']:,} from "
          f"{datetime.datetime.fromtimestamp(md['zero_epoch_ms'] / 1000):%Y-%m-%d %H:%M:%S}")
    if md["duplicates_removed"]:
        print(f"  dupes    {md['duplicates_removed']:,}")
    if md["unplaced_no_abs"]:
        print(f"  ⚠ {md['unplaced_no_abs']:,} messages had no absolute time and "
              f"were omitted.\n    Supply a zero with --zero PLATFORM=<epoch|ISO|+secs>")

    if dry_run:
        print("\n  --dry-run: nothing written.\n")
        return

    if not output:
        output = os.path.join(nas_root, f"{int(index):03d}_merged-chat.json")
    with open(output, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=1)
    size = os.path.getsize(output) / (1024 * 1024)
    print(f"\n  ✔ {os.path.basename(output)}  ({size:.1f}MB)")

    if keep_raw:
        print("  Raw chats kept (--keep-raw).\n")
        return {"merged": output, "moved": set()}
    moved = _archive_raw_chats(config, sources, res)
    print()
    return {"merged": output, "moved": moved}


# ═══════════════════════════════════════════════════════════════════════════
#  POST-STREAM PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def _recent_indices(config: dict, n: int = 5) -> list[int]:
    """Highest n entry indices present on the NAS."""
    nas = config.get("nas_path", "")
    if not os.path.exists(nas):
        return []
    idxs = set()
    for name in os.listdir(nas):
        m = re.match(r"^(\d+)_", name)
        if m:
            idxs.add(int(m.group(1)))
    return sorted(idxs)[-n:]


def _pipeline(config: dict, cache: list[dict], index: int,
              interactive: bool = True) -> bool:
    """
    Bring one entry to its finished shape: meta sidecar, merged chat, raws
    archived.

    Order is not negotiable: the meta sidecar derives exact zeros by reading
    the raw captures, so it must be written while they are still in place.

    Anything not ready yet is left alone and picked up by a later run, so
    this is safe to call repeatedly.
    """
    nas_root = config.get("nas_path", "")
    nas = scan_nas(config, index)
    merged = os.path.join(nas_root, f"{int(index):03d}_merged-chat.json")
    changed = False

    # 1. Meta sidecar, ensured first and independently of the merge: it reads
    #    the raw captures for exact zeros, and an entry merged before this
    #    step existed would otherwise never get one.
    meta_glob = os.path.join(nas_root, f"{int(index):03d}_*.meta.json")
    if not glob.glob(meta_glob):
        cmd_timings(config, index)
        # Only count it if a file actually appeared. An entry with nothing
        # left to describe writes none, and claiming otherwise would make
        # every sweep report work it did not do.
        changed = bool(glob.glob(meta_glob))

    if os.path.exists(merged):
        return changed                     # chat side already finished

    chats = [k for k in ("yt_chat", "tw_chat") if nas.get(k)]
    if not chats:
        print("    no chats present — nothing to merge")
        return changed

    # A truncated YouTube chat blocks the merge: merging now would archive
    # the raws with hours of chat still missing.
    yt_id = (ls_common.extract_video_id_from_filename(nas["yt_chat"])
             if nas.get("yt_chat") else None)
    short = _yt_chat_shortfall(config, cache, nas, yt_id)
    if short:
        if not _offer_chat_backfill(config, short, interactive=interactive):
            print("    repair unavailable (replay chat not ready?) — will retry")
            return changed
        nas = scan_nas(config, index)
        if _yt_chat_shortfall(config, cache, nas, yt_id):
            print("    still short after repair — will retry")
            return changed

    cmd_merge_chat(config, index)          # merges, then archives the raws
    return True


def cmd_sweep(config: dict, count: int = 5, interactive: bool = False):
    """Run the full audit over the most recent entries. Safe to run hourly."""
    idxs = _recent_indices(config, count)
    print(f"\n{'=' * 60}")
    print(f"  Sweep: {len(idxs)} most recent entries — "
          f"{', '.join(str(i) for i in idxs) or 'none'}")
    print(f"{'=' * 60}")
    for idx in idxs:
        try:
            audit(config, idx, interactive=interactive)
        except Exception as e:
            print(f"  ✗ #{idx:03d} {type(e).__name__}: {e}")
    print()


# ═══════════════════════════════════════════════════════════════════════════
#  AUDIT
# ═══════════════════════════════════════════════════════════════════════════

def audit(config: dict, index: int,
          yt_override: str | None = None,
          tw_override: str | None = None,
          push_archive: bool = True,
          interactive: bool = True):
    """
    Reconstruct entry #index.

    1. Parse Obsidian entry → checkbox, date, notes, existing IDs
    2. Scan NAS → existing files
    3. Resolve IDs (override → entry → NAS → cache)
    4. Build reconstructed entry
    5. Write to Obsidian
    6. Offer downloads for missing files
    """
    print(f"\n{'=' * 60}")
    print(f"  Auditing entry #{index}")
    print(f"{'=' * 60}\n")

    # 1. Parse
    entry = ls_common.obsidian_parse_entry(config, index)
    if not entry["found"]:
        print(f"  ✗ Entry #{index} not found.")
        return
    if not entry["date_obj"]:
        print(f"  ✗ Cannot parse date for #{index}")
        if entry["date_str"]:
            print(f"    Raw: {entry['date_str']}")
        return

    # Stash index for cache-by-index lookup in resolve_id
    entry["_index"] = index

    print(f"  Date     : {entry['date_str']} {entry.get('tz_str') or ''}")
    print(f"  Checkbox : {entry['checkbox']}")
    if entry["no_yt"]:
        print("  YouTube  : ✗ (no stream)")
    if entry["no_tw"]:
        print("  Twitch   : ✗ (no stream)")
    print()

    # 2. NAS scan
    print("  Archive scan:")
    nas = scan_nas(config, index)
    for key, label in [("yt_video", "YT video"), ("yt_chat", "YT chat"),
                       ("tw_video", "TW video"), ("tw_chat", "TW chat")]:
        status = f"✓ {nas[key]}" if nas[key] else "✗ not found"
        print(f"    {status}")
    print()

    # 2b. Media analysis (duration + chat stats)
    _print_media_analysis(config, nas)

    # 3. Resolve IDs
    cache = ls_common.load_cache()

    yt_id, yt_src = ((None, None) if entry["no_yt"]
                     else resolve_id(config, cache, "youtube", entry, nas, yt_override))
    tw_id, tw_src = ((None, None) if entry["no_tw"]
                     else resolve_id(config, cache, "twitch", entry, nas, tw_override))

    print("  IDs:")
    if not entry["no_yt"]:
        print(f"    [YT] {yt_id or '—'}")
    if not entry["no_tw"]:
        print(f"    [TW] {tw_id or '—'}")
    print()

    # 4. Build entry
    block = build_entry(config, cache, index, entry, nas, yt_id, tw_id)
    print("  ┌─ Reconstructed ────────────────────────────────────")
    for line in block:
        print(f"  │ {line}")
    print("  └────────────────────────────────────────────────────\n")

    # 5. Write
    # Headless runs write without asking. The reconstruction is deterministic
    # and the vault is in git; a timer that stops to ask a question nobody is
    # there to answer just never runs at all, which is what --sweep did.
    if not interactive or input("  Write to Obsidian? (y/n): ").strip().lower() == "y":
        if ls_common.obsidian_write_entry(config, index, block):
            print("  ✓ Written.")
        else:
            print("  ✗ Write failed.")
    else:
        print("  Skipped.")
    print()

    # 6. Missing files → download
    absent = {
        "yt_video": entry.get("yt_video_x", False),
        "yt_chat":  entry.get("yt_chat_x", False),
        "tw_video": entry.get("tw_video_x", False),
        "tw_chat":  entry.get("tw_chat_x", False),
    }

    missing = _identify_missing(config, nas, yt_id, tw_id, absent)
    changed = False

    if missing:
        if not interactive:
            # A missing VOD can be many GB; never pull one unattended.
            for m in missing:
                print(f"  ✗ missing: {m['label']}")
        elif _download_files(config, missing, index):
            changed = True
            nas = scan_nas(config, index)   # coverage needs the new files
    else:
        print("  ✔ All files present.\n")

    # 7. meta sidecar, merged chat, archive raws
    if _pipeline(config, cache, index, interactive=interactive):
        changed = True
        nas = scan_nas(config, index)

    if not changed:
        # Save cache (may have been updated by title lookups)
        ls_common.save_cache(cache)
        if push_archive:
            cmd_archive(config, index, entry=entry, nas=nas, cache=cache,
                        yt_id=yt_id, tw_id=tw_id, interactive=interactive)
        return

    # Re-scan and rebuild after download
    print("\n  Re-scanning NAS...")
    nas = scan_nas(config, index)
    for key in ("yt_video", "yt_chat", "tw_video", "tw_chat"):
        status = f"✔ {nas[key]}" if nas[key] else "✗ still missing"
        print(f"    {status}")
    print()

    # Re-run media analysis on freshly downloaded files
    _print_media_analysis(config, nas)

    block = build_entry(config, cache, index, entry, nas, yt_id, tw_id)
    print("  ┌─ Updated ──────────────────────────────────────────")
    for line in block:
        print(f"  │ {line}")
    print("  └────────────────────────────────────────────────────\n")

    if not interactive or input("  Write to Obsidian? (y/n): ").strip().lower() == "y":
        if ls_common.obsidian_write_entry(config, index, block):
            print("  ✔ Written.")
        else:
            print("  ✗ Write failed.")

    ls_common.save_cache(cache)

    # Last, and only after the vault write: the archive learns what this run
    # settled, not what it was still deciding.
    if push_archive:
        cmd_archive(config, index, entry=entry, nas=nas, cache=cache,
                    yt_id=yt_id, tw_id=tw_id, interactive=interactive)
    print()


# ═══════════════════════════════════════════════════════════════════════════
#  ARCHIVE
# ═══════════════════════════════════════════════════════════════════════════
#
#  Push what this audit reconstructed to the tenma archive: create the stream
#  if it has never seen this broadcast, otherwise fill in blanks and ask about
#  anything that collides. One direction only. Nothing in the archive is ever
#  read back into the vault, because two systems writing to each other is how
#  both end up wrong.

TZ_RE = re.compile(r"GMT\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?", re.I)


def _tz_offset_min(tz_str: str | None) -> int | None:
    """`(GMT-6)` → -360. Bare `(GMT)` → 0. Anything else → None."""
    if not tz_str:
        return None
    m = TZ_RE.search(tz_str)
    if not m:
        return 0 if "gmt" in tz_str.lower() else None
    sign = -1 if m.group(1) == "-" else 1
    return sign * (int(m.group(2)) * 60 + int(m.group(3) or 0))


def _vault_epoch(date_obj, tz_min: int | None) -> int | None:
    """The vault writes a wall-clock reading; the offset says where."""
    if not date_obj:
        return None
    return int(calendar.timegm(date_obj.timetuple()) - (tz_min or 0) * 60)


MERGED_CHAT_HEAD_BYTES = 8 * 1024 * 1024


def _merged_chat_sources(path: str) -> str | None:
    """Which platforms are inside a merged chat, without reading the messages.

    ls_chat.merge() serialises `metadata` before `messages`, and cmd_merge_chat
    writes it with indent=1, so the sources are in the first few kilobytes of a
    file that can be hundreds of megabytes. Read up to the messages array,
    close the object, and parse that.

    Deliberately best-effort. If the format ever changes this returns None and
    the caller simply does not send chat_sources, which is a gap rather than a
    lie.
    """
    try:
        buf, size = [], 0
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.rstrip("\n") == ' "messages": [':
                    buf.append(' "messages": []}')
                    break
                size += len(line)
                if size > MERGED_CHAT_HEAD_BYTES:
                    return None
                buf.append(line)
            else:
                return None
        meta = json.loads("".join(buf))["metadata"]
        # ls_chat names platforms 'youtube', 'twitch' and 'unknown'. Map by
        # name, never by truncation: 'youtube'[:2] is 'YO', which silently
        # dropped YouTube from the list and left the archive recording that a
        # dual-platform stream had Twitch chat only.
        plats = {ls_archive.PLATFORM.get(str(src.get("platform", "")).strip().lower())
                 for src in meta.get("sources", [])
                 if src.get("messages") and src.get("zero_ms") is not None}
        # 'unknown' maps to None and is dropped: a source ls_chat could not
        # identify is not evidence that either platform had chat.
        return ",".join(sorted(p for p in plats if p)) or None
    except Exception:
        return None


def _archive_inputs(config: dict, cache: list[dict], entry: dict, nas: dict,
                    index: int, yt_id: str | None, tw_id: str | None):
    """Everything the archive could learn from this audit."""
    caps, timings = [], {}

    # The merged chat is a property of the broadcast, not of one platform, so
    # it lives on the stream. It is also the only chat the site serves.
    nas_root = config.get("nas_path", "")
    merged_name = f"{int(index):03d}_merged-chat.json"
    merged_rel = (ls_archive.archive_path(config, merged_name)
                  if os.path.exists(os.path.join(nas_root, merged_name)) else None)

    for prefix, platform, vid, no_it in (
            ("yt", "youtube", yt_id, entry.get("no_yt")),
            ("tw", "twitch",  tw_id, entry.get("no_tw"))):
        # A platform the vault says had no stream gets no capture. Absence is
        # how the archive already spells "there was nothing here".
        if no_it or not vid:
            continue
        t = _platform_timings(config, cache, nas, prefix, platform) or {}
        timings[platform] = t
        cap = {
            "platform": platform,
            "remote_id": vid,
            "url": ls_common.build_stream_url(config, platform, vid),
            "title": _get_title(config, cache, vid, platform, nas[f"{prefix}_video"]),
            "video_path": ls_archive.archive_path(config, nas[f"{prefix}_video"]),
            "chat_path": ls_archive.archive_path(config, nas[f"{prefix}_chat"]),
        }
        # Once the merge holds this platform's messages the raw goes to deep
        # storage, and the archive is told to stop pointing at it rather than
        # left holding a path that now reads `lost`. Only when the raw is
        # actually gone: with chat_archive_path unset the pipeline leaves them
        # in place and there is nothing to forget.
        if merged_rel and not nas[f"{prefix}_chat"]:
            cap["clear"] = ["chat_path"]
        if t.get("duration_secs"):
            cap["duration_s"] = int(t["duration_secs"])
        if t.get("stream_start_epoch_ms"):
            cap["broadcast_started_at"] = t["stream_start_epoch_ms"] // 1000
        if t.get("record_start_epoch_ms"):
            cap["record_started_at"] = t["record_start_epoch_ms"] // 1000
            # ls-audit's own word for how well it knows this, carried through
            # so the archive can refuse to let a filename guess overwrite a
            # measurement — and so the theater can draw the difference.
            cap["local_start_precision_s"] = (
                60 if t.get("record_start_accuracy") == "minute" else 1)
        caps.append(cap)

    tz_min = _tz_offset_min(entry.get("tz_str"))
    # The broadcast start, when something measured it, beats the vault's
    # hand-typed minute. Both are offered; the archive decides nothing, the
    # human answering the prompt does.
    measured = [t["stream_start_epoch_ms"] // 1000 for t in timings.values()
                if t.get("stream_start_epoch_ms")]
    started = min(measured) if measured else _vault_epoch(entry.get("date_obj"), tz_min)

    stream_fields = {}
    title = next((c["title"] for c in caps if c.get("title")), None)
    if title:
        stream_fields["title"] = title
    if started:
        stream_fields["started_at"] = started
    if tz_min is not None:
        stream_fields["tz_offset_min"] = tz_min
    if merged_rel:
        stream_fields["chat_path"] = merged_rel
        sources = _merged_chat_sources(os.path.join(nas_root, merged_name))
        if sources:
            stream_fields["chat_sources"] = sources
    return stream_fields, caps


def cmd_archive(config: dict, index: int, *, entry: dict | None = None,
                nas: dict | None = None, cache: list[dict] | None = None,
                yt_id: str | None = None, tw_id: str | None = None,
                interactive: bool = True) -> bool:
    """Reconcile entry #index with the archive. Returns True if anything was
    sent. Never raises: the audit is the point, this is a courtesy."""
    if not ls_archive.enabled(config):
        return False
    try:
        if entry is None:
            entry = ls_common.obsidian_parse_entry(config, index)
            if not entry["found"]:
                print(f"  ✗ Entry #{index} not found.")
                return False
            entry["_index"] = index
        if nas is None:
            nas = scan_nas(config, index)
        if cache is None:
            cache = ls_common.load_cache()
        if yt_id is None and not entry.get("no_yt"):
            yt_id, _ = resolve_id(config, cache, "youtube", entry, nas, None)
        if tw_id is None and not entry.get("no_tw"):
            tw_id, _ = resolve_id(config, cache, "twitch", entry, nas, None)

        stream_fields, caps = _archive_inputs(
            config, cache, entry, nas, index, yt_id, tw_id)
        if not caps and not stream_fields:
            return False

        print("\n  ┌─ Archive ──────────────────────────────────────────")
        plan = ls_archive.build_plan(config, idx=index,
                                     stream_fields=stream_fields, captures=caps)
        print(ls_archive.render_plan(plan))
        print("  └────────────────────────────────────────────────────")
        if not plan.get("ok") or not plan["items"]:
            return False

        plan = ls_archive.confirm_plan(plan, interactive=interactive)
        if not any(i.get("accepted") for i in plan["items"]):
            print("  Nothing sent.\n")
            return False
        if interactive and input("\n  Send to archive? (y/n): ").strip().lower() != "y":
            print("  Skipped.\n")
            return False

        results = ls_archive.push_plan(config, plan)
        for r in results:
            print(f"  ✔ #{r.get('index')} "
                  f"vod={r.get('vod_state')} chat={r.get('chat_state')}"
                  + ("  (created)" if r.get("created") else ""))
        print()
        return bool(results)
    except Exception as e:
        print(f"  ✗ Archive step failed: {e}")
        logging.getLogger(__name__).warning(f"archive reconcile failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Audit and reconstruct Obsidian livestream entries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  ls-audit 515                        Reconstruct entry #515
  ls-audit 515 --yt-id dQw4w9WgXcQ   Override YouTube ID
  ls-audit 515 --tw-id 2345678901     Override Twitch ID
  ls-audit --refresh                  Refresh all caches
  ls-audit --refresh youtube          Refresh YouTube only
  ls-audit --inject URL               Inject video from URL
  ls-audit --inject --manual          Manual cache injection
  ls-audit --cache-info dQw4w9WgXcQ   Look up cached video
  ls-audit 515 --no-archive           Audit without touching the archive
  ls-audit --sweep [N]                Audit N recent entries (hourly timer)
  ls-audit 515 --archive              Reconcile with the archive only
  ls-audit 515 --archive --yes        ...filling blanks, asking nothing
        """,
    )
    parser.add_argument("index", nargs="?", type=int,
                        help="Entry index to audit")
    parser.add_argument("--yt-id", help="Override YouTube video ID")
    parser.add_argument("--tw-id", help="Override Twitch video ID")
    parser.add_argument("--refresh", nargs="?", const="all",
                        choices=["all", "youtube", "twitch"],
                        help="Refresh VOD cache")
    parser.add_argument("--inject", nargs="?", const="__prompt__",
                        metavar="URL",
                        help="Inject video into cache")
    parser.add_argument("--manual", action="store_true",
                        help="Use manual input for --inject")
    parser.add_argument("--cache-info", metavar="ID",
                        help="Look up a video ID in the cache")
    parser.add_argument("--timings", action="store_true",
                        help="Write a timings sidecar for this entry")
    parser.add_argument("--merge-chat", action="store_true",
                        help="Merge this entry's chats into one tagged file")
    parser.add_argument("--ref", default="youtube",
                        help="--merge-chat: reference timeline "
                             "(youtube | twitch | epoch ms)")
    parser.add_argument("--zero", action="append", metavar="PLATFORM=VALUE",
                        help="--merge-chat: define a source's zero "
                             "(epoch, ISO, or +/-seconds). Repeatable.")
    parser.add_argument("-o", "--output", metavar="PATH",
                        help="--merge-chat: output path")
    parser.add_argument("--dry-run", action="store_true",
                        help="--merge-chat: report without writing")
    parser.add_argument("--archive", action="store_true",
                        help="Reconcile with the archive and do nothing else")
    parser.add_argument("--no-archive", action="store_true",
                        help="Audit without touching the archive")
    parser.add_argument("--sweep", nargs="?", const=5, type=int, metavar="N",
                        help="Audit the N most recent entries (default 5), "
                             "non-interactive. Intended for an hourly timer.")
    parser.add_argument("--yes", action="store_true",
                        help="--archive: accept new values, leave collisions "
                             "alone, ask nothing")

    args = parser.parse_args()
    config = ls_common.load_config()

    if args.sweep is not None:
        cmd_sweep(config, args.sweep)
        return
    if args.refresh is not None:
        cmd_refresh(config, args.refresh)
        return
    if args.cache_info:
        cmd_cache_info(args.cache_info)
        return
    if args.inject is not None:
        url = None if args.manual or args.inject == "__prompt__" else args.inject
        cmd_inject(config, url)
        return
    if args.index is None:
        parser.print_help()
        return

    if args.timings:
        cmd_timings(config, args.index, output=args.output,
                    dry_run=args.dry_run)
        return

    if args.merge_chat:
        ref = int(args.ref) if args.ref.lstrip("-").isdigit() else args.ref
        cmd_merge_chat(config, args.index, ref=ref, zeros=args.zero,
                       output=args.output, dry_run=args.dry_run)
        return

    if args.archive:
        if not ls_archive.enabled(config):
            print("  Archive is off — set archive_url and archive_token.")
            return
        cmd_archive(config, args.index, yt_id=args.yt_id, tw_id=args.tw_id,
                    interactive=not args.yes)
        return

    audit(config, args.index, yt_override=args.yt_id, tw_override=args.tw_id,
          push_archive=not args.no_archive, interactive=not args.yes)


if __name__ == "__main__":
    main()
