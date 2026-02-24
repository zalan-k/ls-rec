#!/usr/bin/env python3
"""
ls-audit - Audit and reconstruct Obsidian livestream entries.

Philosophy:
    The Obsidian entry is the source of truth, but only for: date, checkbox,
    and user notes. Everything else (titles, URLs, file links) is rebuilt
    from video IDs — resolved via URL > NAS filename > date-based cache.

Usage:
    ls-audit <index>                        Reconstruct entry
    ls-audit <index> --yt-id ID             Override YouTube video ID
    ls-audit <index> --tw-id ID             Override Twitch video ID
    ls-audit --refresh [youtube|twitch]     Refresh stream cache
    ls-audit --inject URL                   Add external video to cache
    ls-audit --inject --manual              Manually add to cache
    ls-audit --cache-info ID                Look up a cached video by ID
"""

import os, re, sys, json, glob, subprocess, shutil, datetime, argparse
import urllib.parse, urllib.request

# ── Config ────────────────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(SCRIPT_DIR, ".stream_cache.json")
with open(os.path.join(SCRIPT_DIR, "config.json")) as f:
    CONFIG = json.load(f)

# ══════════════════════════════════════════════════════════════════════════════
#  CACHE
# ══════════════════════════════════════════════════════════════════════════════
#
#  Flat lists keyed by platform. Each entry:
#    id, platform, title, start_time (ISO), duration (seconds), channel, injected
#
#  YouTube entries also carry upload_date (YYYYMMDD) for compat.
#  Injected entries are preserved across refreshes.

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                raw = json.load(f)

            # Migrate old nested format: {"youtube": {"streams": [...]}}
            cache = {}
            for platform in ("youtube", "twitch"):
                data = raw.get(platform, [])
                if isinstance(data, dict):
                    cache[platform] = data.get("streams", data.get("vods", []))
                elif isinstance(data, list):
                    cache[platform] = data
                else:
                    cache[platform] = []
            return cache
        except (json.JSONDecodeError, IOError):
            pass
    return {"youtube": [], "twitch": []}


def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def _newest_date(streams):
    """Get newest start_time or upload_date from a list of cache entries."""
    dates = []
    for s in streams:
        st = s.get("start_time", "")[:10]
        if st:
            dates.append(st)
        ud = s.get("upload_date", "")
        if ud:
            dates.append(f"{ud[:4]}-{ud[4:6]}-{ud[6:8]}")
    return max(dates) if dates else None


# ── YouTube refresh ───────────────────────────────────────────────────────────

def refresh_youtube(cache, full=False):
    count = 100 if full else 10
    print(f"  ⌛ Refreshing YouTube cache ({'full' if full else 'incremental'}, last {count})...")

    cmd = [
        os.path.join(CONFIG["venv"], "bin", "yt-dlp"), "--dump-json",
        "--playlist-items", f"1:{count}",
        "--cookies-from-browser", "firefox",
        f"https://www.youtube.com/{CONFIG['youtube_handle']}/streams"
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0 and not result.stdout.strip():
            stderr_tail = result.stderr.strip().split('\n')[-3:] if result.stderr else []
            print(f"  ⚠ yt-dlp failed (exit {result.returncode})")
            for line in stderr_tail:
                print(f"    {line}")
            return False
    except subprocess.TimeoutExpired:
        print("  ⚠ yt-dlp timed out")
        return False
    except FileNotFoundError:
        print("  ⚠ yt-dlp not found")
        return False

    new_streams = []
    for line in result.stdout.strip().split('\n'):
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            vid_id = data.get("id", "")
            if not vid_id:
                continue

            release_ts = data.get("release_timestamp")
            upload_date = data.get("upload_date", "")

            if release_ts:
                start_time = datetime.datetime.fromtimestamp(release_ts).isoformat()
            elif upload_date:
                start_time = datetime.datetime.strptime(upload_date, "%Y%m%d").isoformat()
            else:
                continue

            new_streams.append({
                "id":          vid_id,
                "platform":    "youtube",
                "title":       data.get("title", "Unknown"),
                "start_time":  start_time,
                "upload_date": upload_date,
                "duration":    data.get("duration"),
                "channel":     data.get("channel") or CONFIG["youtube_handle"],
                "injected":    False,
            })
        except json.JSONDecodeError:
            continue

    if not new_streams:
        print("  ⚠ No streams parsed")
        return False

    # Merge: preserve injected, overwrite non-injected by ID
    existing = {}
    for s in cache.get("youtube", []):
        if s.get("injected"):
            existing[s["id"]] = s          # injected entries kept as-is
        else:
            existing.setdefault(s["id"], s)
    for s in new_streams:
        existing[s["id"]] = s              # fresh data wins

    cache["youtube"] = sorted(existing.values(), key=lambda s: s.get("start_time", ""), reverse=True)
    save_cache(cache)
    print(f"  ✔ YouTube cache: {len(cache['youtube'])} streams")
    return True


# ── Twitch refresh ────────────────────────────────────────────────────────────

def _twitch_get_token():
    data = urllib.parse.urlencode({
        "client_id":     CONFIG["twitch_client_id"],
        "client_secret": CONFIG["twitch_client_secret"],
        "grant_type":    "client_credentials",
    }).encode()
    req = urllib.request.Request("https://id.twitch.tv/oauth2/token", data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())["access_token"]
    except Exception as e:
        print(f"  ⚠ Twitch auth failed: {e}")
        return None


def _parse_twitch_duration(dur_str):
    """Parse '3h24m18s' → seconds."""
    if not dur_str:
        return None
    total = 0
    for m in re.finditer(r'(\d+)([hms])', dur_str):
        val, unit = int(m.group(1)), m.group(2)
        total += val * {'h': 3600, 'm': 60, 's': 1}[unit]
    return total or None


def refresh_twitch(cache, full=False):
    token = _twitch_get_token()
    if not token:
        return False

    limit = 200 if full else 100
    print(f"  ⌛ Refreshing Twitch cache ({'full' if full else 'incremental'}, last {limit})...")

    headers = {
        "Client-ID":     CONFIG["twitch_client_id"],
        "Authorization": f"Bearer {token}",
    }

    all_vods = []
    cursor = None
    fetched = 0

    while fetched < limit:
        batch = min(100, limit - fetched)
        url = (f"https://api.twitch.tv/helix/videos"
               f"?user_id={CONFIG['twitch_user_id']}&type=archive&first={batch}")
        if cursor:
            url += f"&after={cursor}"

        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
        except Exception as e:
            print(f"  ⚠ Twitch API error: {e}")
            break

        videos = data.get("data", [])
        if not videos:
            break

        for v in videos:
            all_vods.append({
                "id":        v["id"],
                "platform":  "twitch",
                "title":     v["title"],
                "start_time": v["created_at"],
                "duration":  _parse_twitch_duration(v.get("duration")),
                "channel":   CONFIG["twitch_user"],
                "injected":  False,
            })

        fetched += len(videos)
        cursor = data.get("pagination", {}).get("cursor")
        if not cursor:
            break

    if not all_vods:
        print("  ⚠ No VODs returned")
        return False

    existing = {}
    for v in cache.get("twitch", []):
        if v.get("injected"):
            existing[v["id"]] = v
        else:
            existing.setdefault(v["id"], v)
    for v in all_vods:
        existing[v["id"]] = v

    cache["twitch"] = sorted(existing.values(), key=lambda v: v.get("start_time", ""), reverse=True)
    save_cache(cache)
    print(f"  ✔ Twitch cache: {len(cache['twitch'])} VODs")
    return True


# ── Cache injection ───────────────────────────────────────────────────────────

def inject_video(url=None):
    """Add a video to the cache. Fetches metadata via yt-dlp, or prompts manually."""
    cache = load_cache()

    if url:
        print(f"  ⌛ Fetching metadata for: {url}")
        try:
            cmd = [os.path.join(CONFIG["venv"], "bin", "yt-dlp"), "--dump-json", "--cookies-from-browser", "firefox", url]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

            if result.returncode != 0:
                print(f"  ⚠ yt-dlp failed. Falling back to manual entry.")
                return _inject_manual(cache)

            data = json.loads(result.stdout.strip())
            platform = "twitch" if "twitch.tv" in url else "youtube"

            release_ts = data.get("release_timestamp")
            upload_date = data.get("upload_date", "")
            if release_ts:
                start_time = datetime.datetime.fromtimestamp(release_ts).isoformat()
            elif upload_date:
                start_time = datetime.datetime.strptime(upload_date, "%Y%m%d").isoformat()
            else:
                start_time = datetime.datetime.now().isoformat()

            entry = {
                "id":         data.get("id", "unknown"),
                "platform":   platform,
                "title":      data.get("title", "Unknown"),
                "start_time": start_time,
                "duration":   data.get("duration"),
                "channel":    data.get("channel") or data.get("uploader") or "unknown",
                "injected":   True,
            }
            if platform == "youtube":
                entry["upload_date"] = upload_date

        except Exception as e:
            print(f"  ⚠ Error: {e}")
            return _inject_manual(cache)
    else:
        return _inject_manual(cache)

    # Show and confirm
    _print_cache_entry(entry)
    if input("\n  Add to cache? (y/n): ").strip().lower() != 'y':
        print("  Skipped.")
        return

    _upsert_cache(cache, entry)
    print(f"  ✔ Added to {entry['platform']} cache.")


def _inject_manual(cache):
    """Interactive manual cache entry."""
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
        if 'T' in date_str:
            start_time = date_str
        else:
            start_time = datetime.datetime.strptime(date_str, "%Y-%m-%d").isoformat()
    except ValueError:
        print("  ✗ Invalid date.")
        return

    dur = input("  Duration in seconds (Enter to skip): ").strip()
    duration = int(dur) if dur.isdigit() else None

    channel = input("  Channel/uploader: ").strip() or "unknown"

    entry = {
        "id":         vid_id,
        "platform":   platform,
        "title":      title,
        "start_time": start_time,
        "duration":   duration,
        "channel":    channel,
        "injected":   True,
    }
    if platform == "youtube":
        entry["upload_date"] = start_time[:10].replace("-", "")

    _print_cache_entry(entry)
    if input("\n  Add to cache? (y/n): ").strip().lower() != 'y':
        print("  Skipped.")
        return

    _upsert_cache(cache, entry)
    print(f"  ✔ Added to {platform} cache.")


def _upsert_cache(cache, entry):
    """Insert or update a cache entry, dedup by ID, save."""
    platform = entry["platform"]
    streams = cache.get(platform, [])
    by_id = {s["id"]: s for s in streams}
    by_id[entry["id"]] = entry
    cache[platform] = sorted(by_id.values(), key=lambda s: s.get("start_time", ""), reverse=True)
    save_cache(cache)


def _print_cache_entry(entry):
    """Pretty-print a cache entry."""
    dur = f"{entry['duration']}s ({entry['duration']//3600}h{(entry['duration']%3600)//60:02d}m)" if entry.get('duration') else "unknown"
    print(f"\n  Platform : {entry['platform']}")
    print(f"  ID       : {entry['id']}")
    print(f"  Title    : {entry['title']}")
    print(f"  Start    : {entry['start_time']}")
    print(f"  Duration : {dur}")
    print(f"  Channel  : {entry.get('channel', 'unknown')}")
    print(f"  Injected : {'yes' if entry.get('injected') else 'no'}")


def cache_info(vid_id):
    """Look up a video by ID in cache."""
    cache = load_cache()
    for platform in ("youtube", "twitch"):
        for s in cache.get(platform, []):
            if s["id"] == vid_id:
                _print_cache_entry(s)
                return
    print(f"  ✗ ID '{vid_id}' not found in cache.")


# ══════════════════════════════════════════════════════════════════════════════
#  OBSIDIAN PARSER  (tag-based, defensive)
# ══════════════════════════════════════════════════════════════════════════════
#
#  Extracts ONLY: checkbox, date, timezone, video IDs from URLs,
#  no-stream markers (✗), and user notes.
#  Everything else is discarded and rebuilt.

def _extract_video_id_from_url(url):
    """Extract video ID + platform from a YouTube or Twitch URL."""
    if not url:
        return None, None

    m = re.search(r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})', url)
    if m:
        return m.group(1), "youtube"

    m = re.search(r'twitch\.tv/[^/]+/videos?/(\d+)', url)
    if m:
        return m.group(1), "twitch"

    return None, None


def parse_entry(index):
    """
    Minimal parse of Obsidian entry #index.

    Returns dict with: found, checkbox, date_str, date_obj, tz_str,
    yt_id, tw_id, no_yt, no_tw, notes.
    """
    result = {
        "found": False,
        "checkbox": "[ ]",
        "date_str": None, "date_obj": None, "tz_str": None,
        "yt_id": None, "tw_id": None,
        "no_yt": False, "no_tw": False,
        "notes": [],
    }

    if not os.path.exists(CONFIG["obsidian"]):
        print(f"  ✗ Obsidian file not found: {CONFIG['obsidian']}")
        return result

    with open(CONFIG["obsidian"], 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Find header
    start = None
    for i, line in enumerate(lines):
        if re.search(rf'\*\*{index}\*\*\s*:', line):
            start = i
            break
    if start is None:
        return result

    result["found"] = True
    header = lines[start]

    # Checkbox
    cb = re.search(r'\[([ x])\]', header)
    if cb:
        result["checkbox"] = f"[{cb.group(1)}]"

    # Date
    dm = re.search(r'(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2})', header)
    if dm:
        result["date_str"] = dm.group(1)
        try:
            result["date_obj"] = datetime.datetime.strptime(dm.group(1), "%Y.%m.%d %H:%M")
        except ValueError:
            pass

    # Timezone
    tz = re.search(r'(\(GMT[^)]*\))', header)
    if tz:
        result["tz_str"] = tz.group(1)

    # Duration
    dur_m = re.search(r'\[(\d{2}:\d{2}:\d{2})\]', header)
    if dur_m:
        result["duration_str"] = dur_m.group(1)

    # Scan indented lines — tag-based, order-independent
    i = start + 1
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Entry boundary
        if stripped == '---' or re.match(r'^-\s*\[.\]\s*\*\*\d+\*\*', line):
            break

        # YouTube platform line
        if re.match(r'^\t`YT`', line):
            if re.search(r'[✗✘]', line):
                result["no_yt"] = True
            else:
                for url in re.findall(r'\]\(([^)]+)\)', line):
                    vid, plat = _extract_video_id_from_url(url)
                    if vid and plat == "youtube":
                        result["yt_id"] = vid
                        break

        # Twitch platform line
        elif re.match(r'^\t`TW`', line):
            if re.search(r'[✗✘]', line):
                result["no_tw"] = True
            else:
                for url in re.findall(r'\]\(([^)]+)\)', line):
                    vid, plat = _extract_video_id_from_url(url)
                    if vid and plat == "twitch":
                        result["tw_id"] = vid
                        break

        # Anything else under this entry = user note (preserved verbatim)
        else:
            result["notes"].append(line)

        i += 1

    return result


def write_entry(index, new_lines):
    """Replace entry #index in the Obsidian file with new_lines."""
    if not os.path.exists(CONFIG["obsidian"]):
        print(f"  ✗ Obsidian file not found")
        return False

    with open(CONFIG["obsidian"], 'r', encoding='utf-8') as f:
        lines = f.readlines()

    start = None
    for i, line in enumerate(lines):
        if re.search(rf'\*\*{index}\*\*\s*:', line):
            start = i
            break
    if start is None:
        print(f"  ✗ Entry #{index} not found")
        return False

    end = start + 1
    while end < len(lines):
        stripped = lines[end].strip()
        if stripped == '---' or re.match(r'^-\s*\[.\]\s*\*\*\d+\*\*', lines[end]):
            break
        end += 1

    replacement = [(l if l.endswith('\n') else l + '\n') for l in new_lines]
    lines[start:end] = replacement

    with open(CONFIG["obsidian"], 'w', encoding='utf-8') as f:
        f.writelines(lines)
    return True


# ══════════════════════════════════════════════════════════════════════════════
#  ID RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════
#
#  Priority:  CLI override → URL in entry → NAS filename → date-based cache

def _extract_video_id_from_filename(filename):
    """Extract [video_id] from NAS filename like '516_title [ID] @ 2026-02-08_04-15.ext'"""
    m = re.search(r'\[([^\]]+)\]\s*@\s*\d{4}-\d{2}-\d{2}', filename)
    return m.group(1) if m else None


def _classify_video_id(vid):
    """Guess platform from a video ID."""
    return "twitch" if vid.lstrip('v').isdigit() else "youtube"

def _find_in_cache(cache, platform, target_date):
    """Search cache for a stream within ±1 hour of target_date."""
    streams = cache.get(platform, [])
    
    best_match = None
    best_delta = None
    max_delta = datetime.timedelta(hours=1)

    for s in streams:
        st = s.get("start_time", "")
        if not st:
            continue
        try:
            stream_dt = datetime.datetime.fromisoformat(st.replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            continue

        delta = abs(stream_dt - target_date)
        if delta <= max_delta and (best_delta is None or delta < best_delta):
            best_match = s
            best_delta = delta

    if best_match:
        if best_delta > datetime.timedelta(minutes=5):
            best_match = dict(best_match)
            best_match["_fuzzy"] = f"~{int(best_delta.total_seconds()//60)}min off"
        return best_match

    return None


def resolve_id(platform, entry, nas, cache, cli_override=None):
    """
    Resolve video ID for a platform.
    Returns (video_id, source_description) or (None, None).
    """
    tag = "yt" if platform == "youtube" else "tw"

    # 1. CLI override
    if cli_override:
        return cli_override, "cli override"

    # 2. Existing URL in Obsidian entry
    entry_id = entry.get(f"{tag}_id")
    if entry_id:
        return entry_id, "entry url"

    # 3. NAS filename
    nas_file = nas.get(f"{tag}_video")
    if nas_file:
        vid = _extract_video_id_from_filename(nas_file)
        if vid:
            return vid, "nas filename"

    # 4. Date → cache (auto-refresh if needed)
    if entry["date_obj"]:
        newest = _newest_date(cache.get(platform, []))
        target = entry["date_obj"].strftime("%Y-%m-%d")

        if newest is None:
            if platform == "youtube":
                refresh_youtube(cache, full=True)
            else:
                refresh_twitch(cache, full=True)
        elif target > newest:
            if platform == "youtube":
                refresh_youtube(cache, full=False)
            else:
                refresh_twitch(cache, full=False)

        match = _find_in_cache(cache, platform, entry["date_obj"])
        if match:
            label = "cache"
            if match.get("_fuzzy"):
                label += f" {match['_fuzzy']}"
            return match["id"], label

    return None, None


# ══════════════════════════════════════════════════════════════════════════════
#  NAS SCANNER
# ══════════════════════════════════════════════════════════════════════════════

def scan_nas(index):
    """Scan NAS for files matching this index prefix."""
    found = {"yt_video": None, "yt_chat": None, "tw_video": None, "tw_chat": None}

    if not os.path.exists(CONFIG["nas_path"]):
        print("  ⚠ NAS not mounted")
        return found

    for filepath in glob.glob(os.path.join(CONFIG["nas_path"], f"{index}_*")):
        filename = os.path.basename(filepath)
        # -- skip intermediate files
        if re.search(r'\.f\d+\.\w+$', filename):
            continue
        vid = _extract_video_id_from_filename(filename)
        if not vid:
            continue

        platform = _classify_video_id(vid)
        ext = os.path.splitext(filename)[1].lower()
        prefix = "yt" if platform == "youtube" else "tw"

        if ext == ".mp4":
            found[f"{prefix}_video"] = filename
        elif ext == ".json":
            found[f"{prefix}_chat"] = filename

    return found


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _build_stream_url(platform, video_id):
    if platform == "youtube":
        return f"https://www.youtube.com/watch?v={video_id}"
    return f"https://www.twitch.tv/{CONFIG['twitch_user']}/video/{video_id}"


def _build_shell_cmd(filename):
    encoded = urllib.parse.quote(filename, safe='')
    return (f"obsidian://shell-commands/?vault={CONFIG['obsidian_vault']}"
            f"&execute={CONFIG['shellcmd_id']}&_arg0=raws/{encoded}")


def _title_from_filename(filename):
    """Extract clean title from NAS filename."""
    name = os.path.splitext(filename)[0]
    name = re.sub(r'^\d+_', '', name)
    name = re.sub(r'\s*\[[^\]]+\]\s*@\s*\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$', '', name)
    return name


def _get_title(video_id, platform, cache, nas_file):
    for s in cache.get(platform, []):
        if s["id"] == video_id:
            return s.get("title")
    if nas_file:
        return _title_from_filename(nas_file)
    # Last resort: fetch from API rather than show "untitled"
    try:
        url = _build_stream_url(platform, video_id)
        cmd = [os.path.join(CONFIG["venv"], "bin", "yt-dlp"), "--dump-json", "--cookies-from-browser", "firefox",
               "--playlist-items", "1", url]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            data = json.loads(result.stdout.strip())
            title = data.get("title") or data.get("description")
            if title:
                # Opportunistically cache it
                _upsert_cache(cache, {
                    "id": video_id, "platform": platform, "title": title,
                    "start_time": data.get("upload_date", ""), "injected": False,
                })
                return title
    except Exception:
        pass
    return None

def _build_platform_line(tag, video_id, platform, title, video_file, chat_file):
    """Build one platform subline for the Obsidian entry."""
    vid_link  = f"[📁]({_build_shell_cmd(video_file)})" if video_file else "[📁]()"
    chat_link = f"[📄]({_build_shell_cmd(chat_file)})" if chat_file else "[📄]()"

    display = title or "untitled"
    url = _build_stream_url(platform, video_id) if video_id else ""
    title_link = f"[ {display} ]({url})"

    return f"\t`{tag}` {vid_link} {chat_link} {title_link}"


def build_entry(index, entry, nas, cache, yt_id, tw_id):
    """Assemble the full Obsidian entry block."""
    lines = []

    # Header (preserved: checkbox, date, timezone)
    date_str = entry["date_str"] or "UNKNOWN"
    tz_str = entry["tz_str"] or "(GMT-6)"
    # Resolve duration (longer of the two platforms)
    durations = []
    for vid_id, platform in [(yt_id, "youtube"), (tw_id, "twitch")]:
        if not vid_id:
            continue
        for s in cache.get(platform, []):
            if s["id"] == vid_id and s.get("duration"):
                durations.append(s["duration"])
                break

    if durations:
        dur = max(durations)
        h, rem = divmod(int(dur), 3600)
        m, s = divmod(rem, 60)
        dur_str = f" [{h:02d}:{m:02d}:{s:02d}]"
    elif entry.get("duration_str"):
        dur_str = f" [{entry['duration_str']}]"
    else:
        dur_str = ""

    lines.append(f"- {entry['checkbox']} **{index}** : {date_str} {tz_str}{dur_str}  #stream")

    # YouTube
    if entry["no_yt"]:
        lines.append("\t`YT` ✗")
    else:
        yt_title = _get_title(yt_id, "youtube", cache, nas["yt_video"]) if yt_id else None
        lines.append(_build_platform_line("YT", yt_id, "youtube", yt_title, nas["yt_video"], nas["yt_chat"]))

    # Twitch
    if entry["no_tw"]:
        lines.append("\t`TW` ✗")
    else:
        tw_title = _get_title(tw_id, "twitch", cache, nas["tw_video"]) if tw_id else None
        lines.append(_build_platform_line("TW", tw_id, "twitch", tw_title, nas["tw_video"], nas["tw_chat"]))

    # User notes (preserved verbatim)
    for note in entry.get("notes", []):
        lines.append(note.rstrip('\n'))

    return lines


# ══════════════════════════════════════════════════════════════════════════════
#  DOWNLOAD INTEGRATION
# ══════════════════════════════════════════════════════════════════════════════

def _find_ls_download():
    path = os.path.join(SCRIPT_DIR, "ls-download.py")
    return path if os.path.exists(path) else None


def _identify_missing(nas, yt_id, tw_id):
    """Determine which files are missing from NAS."""
    missing = []
    if yt_id:
        url = _build_stream_url("youtube", yt_id)
        if not nas["yt_video"]:
            missing.append({"platform": "youtube", "type": "video", "url": url, "label": "YT video"})
        if not nas["yt_chat"]:
            missing.append({"platform": "youtube", "type": "chat", "url": url, "label": "YT chat"})
    if tw_id:
        url = _build_stream_url("twitch", tw_id)
        if not nas["tw_video"]:
            missing.append({"platform": "twitch", "type": "video", "url": url, "label": "TW video"})
        if not nas["tw_chat"]:
            missing.append({"platform": "twitch", "type": "chat", "url": url, "label": "TW chat"})
    return missing


def _offer_downloads(missing, index):
    """Offer to download missing files via ls-download.py."""
    ls_download = _find_ls_download()
    if not ls_download:
        print("  ⚠ ls-download.py not found")
        return False

    print("\n  Missing files:")
    for m in missing:
        print(f"    ↓ {m['label']}: {m['url']}")

    if input("\n  Download? (y/n): ").strip().lower() != 'y':
        print("  Skipped.")
        return False

    # Group by URL to avoid redundant downloads
    by_url = {}
    for m in missing:
        if m["url"] not in by_url:
            by_url[m["url"]] = {"url": m["url"], "platform": m["platform"], "types": set()}
        by_url[m["url"]]["types"].add(m["type"])

    any_success = False
    for url, group in by_url.items():
        dl_type = "both" if len(group["types"]) > 1 else next(iter(group["types"]))
        plat = group["platform"].upper()
        print(f"\n  Downloading {plat} ({dl_type})...")

        cmd = [
            os.path.join(CONFIG["venv"], "bin", "python3"), ls_download,
            "--url", url,
            "--prefix", str(index),
            "--type", dl_type,
            "--output", CONFIG["nas_path"],
        ]
        try:
            result = subprocess.run(cmd)
            if result.returncode == 0:
                print(f"  ✔ {plat} complete.")
                any_success = True
            else:
                print(f"  ✗ {plat} failed (exit {result.returncode})")
        except Exception as e:
            print(f"  ✗ {plat} error: {e}")

    return any_success


# ══════════════════════════════════════════════════════════════════════════════
#  AUDIT FLOW
# ══════════════════════════════════════════════════════════════════════════════

def audit(index, yt_override=None, tw_override=None):
    """
    Reconstruct entry #index.

    1. Parse Obsidian → checkbox, date, notes, existing video IDs
    2. Scan NAS → existing files
    3. Resolve IDs (override → entry URL → NAS → cache)
    4. Build entry from resolved IDs
    5. Write to Obsidian
    6. Offer downloads for missing files
    """
    print(f"\n{'='*60}")
    print(f"  Auditing entry #{index}")
    print(f"{'='*60}\n")

    # ── 1. Parse entry ──
    entry = parse_entry(index)
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
        print(f"  YouTube  : ✗ (no stream)")
    if entry["no_tw"]:
        print(f"  Twitch   : ✗ (no stream)")
    print()

    # ── 2. Scan NAS ──
    print("  Scanning NAS...")
    nas = scan_nas(index)
    for key, label in [("yt_video", "YT video"), ("yt_chat", "YT chat"),
                       ("tw_video", "TW video"), ("tw_chat", "TW chat")]:
        status = f"✔ {nas[key]}" if nas[key] else "✗ not found"
        print(f"    {status}")
    print()

    # ── 3. Resolve IDs ──
    cache = load_cache()

    yt_id, yt_src = (None, None) if entry["no_yt"] else resolve_id("youtube", entry, nas, cache, yt_override)
    tw_id, tw_src = (None, None) if entry["no_tw"] else resolve_id("twitch",  entry, nas, cache, tw_override)

    print("  ID Resolution:")
    if not entry["no_yt"]:
        print(f"    YT: {yt_id or '—'}" + (f"  ← {yt_src}" if yt_src else ""))
    if not entry["no_tw"]:
        print(f"    TW: {tw_id or '—'}" + (f"  ← {tw_src}" if tw_src else ""))
    print()

    # ── 4. Build entry ──
    block = build_entry(index, entry, nas, cache, yt_id, tw_id)

    print("  ┌─ Reconstructed Entry ──────────────────────────────")
    for line in block:
        print(f"  │ {line}")
    print("  └────────────────────────────────────────────────────")
    print()

    # ── 5. Write ──
    if input("  Write to Obsidian? (y/n): ").strip().lower() == 'y':
        if write_entry(index, block):
            print("  ✔ Written.")
        else:
            print("  ✗ Write failed.")
    else:
        print("  Skipped.")
    print()

    # ── 6. Missing files → download ──
    missing = _identify_missing(nas, yt_id, tw_id)
    if not missing:
        print("  ✔ All files present.\n")
        return

    downloaded = _offer_downloads(missing, index)
    if not downloaded:
        return

    # Re-scan and rebuild after download
    print("\n  Re-scanning NAS...")
    nas = scan_nas(index)
    for key, label in [("yt_video", "YT video"), ("yt_chat", "YT chat"),
                       ("tw_video", "TW video"), ("tw_chat", "TW chat")]:
        status = f"✔ {nas[key]}" if nas[key] else "✗ still missing"
        print(f"    {status}")
    print()

    block = build_entry(index, entry, nas, cache, yt_id, tw_id)
    print("  ┌─ Updated Entry ────────────────────────────────────")
    for line in block:
        print(f"  │ {line}")
    print("  └────────────────────────────────────────────────────")
    print()

    if input("  Write to Obsidian? (y/n): ").strip().lower() == 'y':
        if write_entry(index, block):
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
  ls-audit --refresh                      Refresh all caches
  ls-audit --refresh youtube              Refresh YouTube only
  ls-audit --inject URL                   Inject video into cache from URL
  ls-audit --inject --manual              Manually inject into cache
  ls-audit --cache-info dQw4w9WgXcQ       Look up cached video by ID
        """
    )

    parser.add_argument("index", nargs="?", type=int,
                        help="Entry index to audit")
    parser.add_argument("--yt-id",
                        help="Override YouTube video ID for this audit")
    parser.add_argument("--tw-id",
                        help="Override Twitch video ID for this audit")
    parser.add_argument("--refresh", nargs="?", const="all",
                        choices=["all", "youtube", "twitch"],
                        help="Refresh stream cache")
    parser.add_argument("--inject", nargs="?", const="__prompt__",
                        metavar="URL",
                        help="Inject video into cache (pass URL, or omit for manual)")
    parser.add_argument("--manual", action="store_true",
                        help="Use manual input for --inject")
    parser.add_argument("--cache-info", metavar="ID",
                        help="Look up a video ID in the cache")

    args = parser.parse_args()

    # ── Dispatch ──
    if args.refresh is not None:
        cache = load_cache()
        if args.refresh in ("all", "youtube"):
            refresh_youtube(cache, full=True)
        if args.refresh in ("all", "twitch"):
            refresh_twitch(cache, full=True)
        print("\n  ✔ Cache refreshed.")
        return

    if args.cache_info:
        cache_info(args.cache_info)
        return

    if args.inject is not None:
        if args.manual or args.inject == "__prompt__":
            inject_video(url=None)
        else:
            inject_video(url=args.inject)
        return

    if args.index is None:
        parser.print_help()
        return

    audit(args.index, yt_override=args.yt_id, tw_override=args.tw_id)


if __name__ == "__main__":
    main()
