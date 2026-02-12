#!/usr/bin/env python3
"""
ls-audit - Audit and fix Obsidian livestream log entries.

Usage:
    ls-audit <index>            Audit entry #index
    ls-audit <index> --fix      Audit + offer to rewrite entry
    ls-audit --refresh          Force-refresh the stream cache
    ls-audit --refresh youtube  Force-refresh YouTube cache only
    ls-audit --refresh twitch   Force-refresh Twitch cache only
"""

import os, re, sys, json, glob, subprocess, datetime, urllib.parse, urllib.request

# ── Config ────────────────────────────────────────────────────────────────────

CONFIG = {
    "obsidian"       : "/mnt/nas/edit-video_library/Tenma Maemi/archives/Tenma Maemi Livestreams.md",
    "obsidian_vault" : "archives",
    "shellcmd_id"    : "4gtship619",
    "nas_path"       : "/mnt/nas/edit-video_library/Tenma Maemi/archives/raws",
    "youtube_handle" : "@TenmaMaemi",
    "twitch_user"    : "tenma",
    "twitch_user_id" : "664177022",
    "twitch_client_id"     : "xk0o9l63z2mtf854e509jxhzd75u0k",
    "twitch_client_secret" : "li5r50yetf66oct982iw1m6x42ww0u",
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(SCRIPT_DIR, ".stream_cache.json")

# ── Helpers ───────────────────────────────────────────────────────────────────

def classify_video_id(vid):
    """YouTube IDs are 11 alphanumeric chars; Twitch VOD IDs are long numeric."""
    if vid.isdigit() and len(vid) > 11:
        return "twitch"
    return "youtube"

def extract_video_id_from_filename(filename):
    """Extract [video_id] from filename like '516_title [ID] @ 2026-02-08_04-15.ext'"""
    m = re.search(r'\[([^\]]+)\]\s*@\s*\d{4}-\d{2}-\d{2}', filename)
    return m.group(1) if m else None

def build_shell_cmd(filename):
    """Build obsidian://shell-commands URI for a file."""
    encoded = urllib.parse.quote(filename, safe='')
    return (f"obsidian://shell-commands/?vault={CONFIG['obsidian_vault']}"
            f"&execute={CONFIG['shellcmd_id']}&_arg0=raws/{encoded}")

def build_stream_url(platform, video_id):
    if platform == "youtube":
        return f"https://www.youtube.com/watch?v={video_id}"
    return f"https://www.twitch.tv/{CONFIG['twitch_user']}/video/{video_id}"

# ── Stream Cache ──────────────────────────────────────────────────────────────
#
# Cache layout (.stream_cache.json):
# {
#   "youtube": {
#     "streams": [
#       {"id": "60L3NObe9M8", "title": "...", "upload_date": "20260208"}, ...
#     ]
#   },
#   "twitch": {
#     "vods": [
#       {"id": "316307569766", "title": "...", "created_at": "2026-02-08T10:15:00Z"}, ...
#     ]
#   }
# }
#
# Refresh strategy:
#   - Each platform stores a flat list sorted newest-first.
#   - To look up a date, we derive the newest cached date from the list.
#   - If target_date > newest cached date  →  incremental refresh (fetch recent, merge).
#   - If target_date <= newest cached date →  cache is authoritative; no API call.
#   - If cache is empty                   →  full initial fetch.
#

def load_cache():
    """Load the stream cache from disk, or return empty structure."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"youtube": {"streams": []}, "twitch": {"vods": []}}

def save_cache(cache):
    """Write cache to disk."""
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def _yt_newest_date(cache):
    """Return newest upload_date string (YYYYMMDD) in YT cache, or None."""
    streams = cache.get("youtube", {}).get("streams", [])
    if not streams:
        return None
    return max(s["upload_date"] for s in streams)

def _tw_newest_date(cache):
    """Return newest created_at date (YYYY-MM-DD) in TW cache, or None."""
    vods = cache.get("twitch", {}).get("vods", [])
    if not vods:
        return None
    return max(v["created_at"][:10] for v in vods)

# ── YouTube: yt-dlp based lookup ──────────────────────────────────────────────

def refresh_youtube(cache, full=False):
    """
    Fetch YouTube streams list via yt-dlp and merge into cache.
    
    Full refresh: grab up to 200 entries (covers months of history).
    Incremental:  grab last 30 entries and merge.
    """
    count = 100 if full else 30
    print(f"  ⌛ Refreshing YouTube cache ({'full' if full else 'incremental'}, last {count})...")
    
    cmd = [
        "yt-dlp", "--flat-playlist", "--dump-json",
        "--playlist-items", f"1:{count}",
        "--cookies-from-browser", "firefox",
        f"https://www.youtube.com/{CONFIG['youtube_handle']}/streams"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
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
    
    # Parse JSON lines
    new_streams = []
    for line in result.stdout.strip().split('\n'):
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            vid_id = data.get("id", "")
            title = data.get("title", "Unknown")
            upload_date = data.get("upload_date", "")
            
            if vid_id and upload_date:
                new_streams.append({
                    "id": vid_id,
                    "title": title,
                    "upload_date": upload_date,
                })
        except json.JSONDecodeError:
            continue
    
    if not new_streams:
        print(f"  ⚠ No streams parsed from yt-dlp output")
        return False
    
    # Merge: dedup by id, keep newest info
    existing = {s["id"]: s for s in cache.get("youtube", {}).get("streams", [])}
    for s in new_streams:
        existing[s["id"]] = s
    
    # Sort newest first
    merged = sorted(existing.values(), key=lambda s: s["upload_date"], reverse=True)
    cache["youtube"] = {"streams": merged}
    save_cache(cache)
    
    print(f"  ✔ YouTube cache: {len(merged)} streams (newest: {merged[0]['upload_date']})")
    return True

def find_youtube_by_date(cache, target_date):
    """
    Search cache for a YouTube stream on target_date.
    Refreshes cache if target_date is newer than newest cached entry.
    
    Returns: {"id": ..., "title": ..., "url": ...} or None
    """
    target_str = target_date.strftime("%Y%m%d")
    newest = _yt_newest_date(cache)
    
    # Refresh if cache is empty or target is newer than cache
    if newest is None:
        refresh_youtube(cache, full=True)
        newest = _yt_newest_date(cache)
    elif target_str > newest:
        refresh_youtube(cache, full=False)
        newest = _yt_newest_date(cache)
    
    # Search (exact date, then ±1 day)
    streams = cache.get("youtube", {}).get("streams", [])
    
    for s in streams:
        if s["upload_date"] == target_str:
            return {
                "id": s["id"],
                "title": s["title"],
                "url": build_stream_url("youtube", s["id"]),
            }
    
    # ±1 day (timezone edge cases)
    for delta in [-1, 1]:
        alt_str = (target_date + datetime.timedelta(days=delta)).strftime("%Y%m%d")
        for s in streams:
            if s["upload_date"] == alt_str:
                return {
                    "id": s["id"],
                    "title": s["title"],
                    "url": build_stream_url("youtube", s["id"]),
                    "note": f"(matched ±1 day: {alt_str})",
                }
    
    return None

# ── Twitch: Helix API based lookup ───────────────────────────────────────────

def _twitch_get_token():
    """Get OAuth token using client credentials flow."""
    data = urllib.parse.urlencode({
        "client_id": CONFIG["twitch_client_id"],
        "client_secret": CONFIG["twitch_client_secret"],
        "grant_type": "client_credentials",
    }).encode()
    
    req = urllib.request.Request("https://id.twitch.tv/oauth2/token", data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())["access_token"]
    except Exception as e:
        print(f"  ⚠ Twitch auth failed: {e}")
        return None

def refresh_twitch(cache, full=False):
    """
    Fetch Twitch VOD list via Helix API and merge into cache.
    
    Full refresh: paginate up to 200 VODs.
    Incremental:  fetch last 100 VODs.
    """
    token = _twitch_get_token()
    if not token:
        return False
    
    limit = 200 if full else 100
    print(f"  ⌛ Refreshing Twitch cache ({'full' if full else 'incremental'}, last {limit})...")
    
    headers = {
        "Client-ID": CONFIG["twitch_client_id"],
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
                "id": v["id"],
                "title": v["title"],
                "created_at": v["created_at"],
            })
        
        fetched += len(videos)
        cursor = data.get("pagination", {}).get("cursor")
        if not cursor:
            break
    
    if not all_vods:
        print(f"  ⚠ No VODs returned from Twitch API")
        return False
    
    # Merge: dedup by id
    existing = {v["id"]: v for v in cache.get("twitch", {}).get("vods", [])}
    for v in all_vods:
        existing[v["id"]] = v
    
    # Sort newest first
    merged = sorted(existing.values(), key=lambda v: v["created_at"], reverse=True)
    cache["twitch"] = {"vods": merged}
    save_cache(cache)
    
    print(f"  ✔ Twitch cache: {len(merged)} VODs (newest: {merged[0]['created_at'][:10]})")
    return True

def find_twitch_by_date(cache, target_date):
    """
    Search cache for a Twitch VOD on target_date.
    Refreshes cache if target_date is newer than newest cached entry.
    
    Returns: {"id": ..., "title": ..., "url": ...} or None
    """
    target_str = target_date.strftime("%Y-%m-%d")
    newest = _tw_newest_date(cache)
    
    if newest is None:
        refresh_twitch(cache, full=True)
        newest = _tw_newest_date(cache)
    elif target_str > newest:
        refresh_twitch(cache, full=False)
        newest = _tw_newest_date(cache)
    
    vods = cache.get("twitch", {}).get("vods", [])
    
    # Exact date match (created_at is UTC; entry date is GMT-6, so also check ±1 day)
    for v in vods:
        vod_date = v["created_at"][:10]
        if vod_date == target_str:
            return {
                "id": v["id"],
                "title": v["title"],
                "url": build_stream_url("twitch", v["id"]),
            }
    
    for delta in [-1, 1]:
        alt_str = (target_date + datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
        for v in vods:
            if v["created_at"][:10] == alt_str:
                return {
                    "id": v["id"],
                    "title": v["title"],
                    "url": build_stream_url("twitch", v["id"]),
                    "note": f"(matched ±1 day: {alt_str})",
                }
    
    return None

# ── Obsidian Entry Parser ─────────────────────────────────────────────────────

def find_entry(index):
    """
    Find entry #index in the Obsidian file.
    Returns (block_lines, line_start, line_end) or (None, None, None).
    """
    with open(CONFIG["obsidian"], 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    start = None
    for i, line in enumerate(lines):
        # Match: - [ ] **516** : ...  or  - [x] **516** : ...
        if re.search(rf'\*\*{index}\*\*\s*:', line):
            start = i
            break
        # Old format: - [ ] [516_title](url)
        if re.match(rf'^-\s*\[.\]\s*\[{index}_', line):
            start = i
            break
    
    if start is None:
        return None, None, None
    
    # Find end: next --- or next entry header
    end = len(lines)
    for i in range(start + 1, len(lines)):
        stripped = lines[i].strip()
        if stripped == '---':
            end = i + 1   # include the ---
            break
        # Next entry starts (new format)
        if re.match(r'^-\s*\[.\]\s*\*\*\d+\*\*', lines[i]):
            end = i
            break
        # Next entry starts (old format)
        if re.match(r'^-\s*\[.\]\s*\[\d+_', lines[i]):
            end = i
            break
    
    return lines[start:end], start, end

def parse_entry(block, index):
    """
    Parse an entry block into structured data.
    
    Returns dict with:
        checkbox, date_str, date_obj, tz_str,
        yt_url, yt_title, tw_url, tw_title,
        notes (sub-checkbox lines), format_ok (bool)
    """
    result = {
        "checkbox": "[ ]",
        "date_str": None,
        "date_obj": None,
        "tz_str": "(GMT-6)",
        "yt_url": None, "yt_title": None,
        "tw_url": None, "tw_title": None,
        "notes": [],
        "format_ok": True,
        "raw": block,
    }
    
    if not block:
        result["format_ok"] = False
        return result
    
    header = block[0]
    
    # ── Checkbox ──
    cb = re.search(r'\[([ x])\]', header)
    if cb:
        result["checkbox"] = f"[{cb.group(1)}]"
    
    # ── Date + timezone ──
    dm = re.search(r'(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2})', ''.join(block[:5]))
    if dm:
        result["date_str"] = dm.group(1)
        try:
            result["date_obj"] = datetime.datetime.strptime(dm.group(1), "%Y.%m.%d %H:%M")
        except ValueError:
            pass
    
    tz = re.search(r'(\(GMT[^\)]*\))', ''.join(block[:5]))
    if tz:
        result["tz_str"] = tz.group(1)
    
    # ── Platform lines ──
    yt_line_found = False
    tw_line_found = False
    platform_end = 1  # line index after last platform line
    
    for i, line in enumerate(block):
        if i == 0:
            continue
        stripped = line.strip()
        
        if stripped.startswith('`YT`'):
            yt_line_found = True
            platform_end = i + 1
            url_m = re.search(r'\]\((https?://[^\s\)]+)\)', stripped)
            # Title: last [ ... ]( pattern → the stream title link
            title_m = re.search(r'\[\s*([^\]]+?)\s*\]\(https?://', stripped)
            if url_m:
                result["yt_url"] = url_m.group(1)
            if title_m:
                result["yt_title"] = title_m.group(1).strip()
            continue
        
        if stripped.startswith('`TW`'):
            tw_line_found = True
            platform_end = i + 1
            url_m = re.search(r'\]\((https?://[^\s\)]+)\)', stripped)
            title_m = re.search(r'\[\s*([^\]]+?)\s*\]\(https?://', stripped)
            if url_m:
                result["tw_url"] = url_m.group(1)
            if title_m:
                result["tw_title"] = title_m.group(1).strip()
            continue
    
    # For old-format entries, scrape URLs from wherever
    if not yt_line_found and not tw_line_found:
        result["format_ok"] = False
        for line in block:
            for m in re.finditer(r'https://www\.youtube\.com/(?:watch\?v=|live/)([^\s\)]+)', line):
                if not result["yt_url"]:
                    result["yt_url"] = m.group(0).rstrip(')')
            for m in re.finditer(r'https://www\.twitch\.tv/\S+/video/(\d+)', line):
                if not result["tw_url"]:
                    result["tw_url"] = m.group(0).rstrip(')')
    
    # ── Format validation ──
    # Expected: header line matches  - [x] **NNN** : YYYY.MM.DD HH:MM (GMT-N)
    if not re.match(r'^-\s*\[.\]\s*\*\*\d+\*\*\s*:', header):
        result["format_ok"] = False
    if not yt_line_found or not tw_line_found:
        result["format_ok"] = False
    
    # ── Sub-checkbox notes (everything after platform lines that isn't ---) ──
    for i in range(platform_end, len(block)):
        stripped = block[i].strip()
        if stripped == '---' or stripped == '':
            continue
        result["notes"].append(block[i])
    
    return result

# ── NAS Scanner ───────────────────────────────────────────────────────────────

def scan_nas(index):
    """
    Scan NAS for files belonging to entry #index.
    Returns dict: {yt_video, yt_chat, tw_video, tw_chat} → filename or None.
    """
    nas = CONFIG["nas_path"]
    found = {"yt_video": None, "yt_chat": None, "tw_video": None, "tw_chat": None}
    
    if not os.path.exists(nas):
        print("  ⚠ NAS not mounted, skipping file scan")
        return found
    
    pattern = os.path.join(nas, f"{index}_*")
    files = glob.glob(pattern)
    
    for filepath in files:
        filename = os.path.basename(filepath)
        vid = extract_video_id_from_filename(filename)
        if not vid:
            continue
        
        platform = classify_video_id(vid)
        ext = os.path.splitext(filename)[1].lower()
        
        if ext == '.mp4':
            key = f"{'yt' if platform == 'youtube' else 'tw'}_video"
        elif ext == '.json':
            key = f"{'yt' if platform == 'youtube' else 'tw'}_chat"
        else:
            continue
        
        found[key] = filename
    
    return found

# ── Entry Builder ─────────────────────────────────────────────────────────────

def build_platform_line(tag, url, title, video_file, chat_file):
    """
    Build a single platform line in the expected format:
        \t`YT` [📁](shell_cmd) [📄](shell_cmd) [ title ](url)
    """
    # File icon links
    vid_link = f"[📁]({build_shell_cmd(video_file)})" if video_file else "[📁]()"
    chat_link = f"[📄]({build_shell_cmd(chat_file)})" if chat_file else "[📄]()"
    
    # Title + URL link
    display = title or "untitled"
    if url:
        title_link = f"[ {display} ]({url})"
    else:
        title_link = f"[ {display} ]()"
    
    return f"\t`{tag}` {vid_link} {chat_link} {title_link}\n"

def build_fixed_entry(index, entry, nas, yt_found, tw_found):
    """
    Build the corrected full entry text.
    
    Priority for URLs/titles:
        1. NAS files (extract video ID → build URL)
        2. Existing entry data
        3. API/cache search results
    
    Returns: list of lines
    """
    # ── Resolve YouTube info ──
    yt_url = None
    yt_title = entry["yt_title"]
    
    if nas["yt_video"]:
        vid = extract_video_id_from_filename(nas["yt_video"])
        if vid:
            yt_url = build_stream_url("youtube", vid)
    if not yt_url and entry["yt_url"]:
        yt_url = entry["yt_url"]
    if not yt_url and yt_found:
        yt_url = yt_found["url"]
        if not yt_title:
            yt_title = yt_found.get("title")
    
    # ── Resolve Twitch info ──
    tw_url = None
    tw_title = entry["tw_title"]
    
    if nas["tw_video"]:
        vid = extract_video_id_from_filename(nas["tw_video"])
        if vid:
            tw_url = build_stream_url("twitch", vid)
    if not tw_url and entry["tw_url"]:
        tw_url = entry["tw_url"]
    if not tw_url and tw_found:
        tw_url = tw_found["url"]
        if not tw_title:
            tw_title = tw_found.get("title")
    
    # Fall back to deriving titles from filenames
    if not yt_title and nas["yt_video"]:
        yt_title = _title_from_filename(nas["yt_video"])
    if not tw_title and nas["tw_video"]:
        tw_title = _title_from_filename(nas["tw_video"])
    
    # ── Assemble lines ──
    date_str = entry["date_str"] or "UNKNOWN"
    tz_str = entry["tz_str"]
    
    lines = []
    lines.append(f"- {entry['checkbox']} **{index}** : {date_str} {tz_str}  \n")
    lines.append(build_platform_line("YT", yt_url, yt_title, nas["yt_video"], nas["yt_chat"]))
    lines.append(build_platform_line("TW", tw_url, tw_title, nas["tw_video"], nas["tw_chat"]))
    
    # Preserve sub-checkboxes / notes
    if entry["notes"]:
        lines.extend(entry["notes"])
    
    return lines

def _title_from_filename(filename):
    """Extract clean title from NAS filename (strip index, video ID, timestamp, extension)."""
    name = os.path.splitext(filename)[0]
    # Remove index prefix
    name = re.sub(r'^\d+_', '', name)
    # Remove [video_id] @ timestamp suffix
    name = re.sub(r'\s*\[[^\]]+\]\s*@\s*\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$', '', name)
    return name

# ── Download Integration ──────────────────────────────────────────────────────

def find_ls_download():
    """Find ls-download.py in the same directory as this script."""
    path = os.path.join(SCRIPT_DIR, "ls-download.py")
    return path if os.path.exists(path) else None

def identify_missing(nas, yt_url, tw_url):
    """
    Figure out what needs downloading.
    Returns list of dicts: [{platform, type, url, label}, ...]
    """
    missing = []
    
    if yt_url:
        if not nas["yt_video"]:
            missing.append({"platform": "youtube", "type": "video", "url": yt_url, "label": "YT video"})
        if not nas["yt_chat"]:
            missing.append({"platform": "youtube", "type": "chat", "url": yt_url, "label": "YT chat"})
    
    if tw_url:
        if not nas["tw_video"]:
            missing.append({"platform": "twitch", "type": "video", "url": tw_url, "label": "TW video"})
        if not nas["tw_chat"]:
            missing.append({"platform": "twitch", "type": "chat", "url": tw_url, "label": "TW chat"})
    
    return missing

def offer_downloads(missing, index):
    """Offer to download missing files via ls-download.py. Returns True if anything was downloaded."""
    ls_download = find_ls_download()
    if not ls_download:
        print("  ⚠ ls-download.py not found alongside this script, can't auto-download.")
        return False
    
    print()
    print("  Missing files:")
    for m in missing:
        print(f"    ↓ {m['label']}: {m['url']}")
    
    confirm = input("\n  Download missing files? (y/n): ").strip().lower()
    if confirm != 'y':
        print("  Skipped downloads.")
        return False
    
    # Group by URL so we don't call ls-download twice for same stream
    by_url = {}
    for m in missing:
        url = m["url"]
        if url not in by_url:
            by_url[url] = {"url": url, "platform": m["platform"], "types": set()}
        by_url[url]["types"].add(m["type"])
    
    any_success = False
    for url, group in by_url.items():
        types = group["types"]
        dl_type = "both" if ("video" in types and "chat" in types) else ("video" if "video" in types else "chat")
        plat = group["platform"].upper()
        
        print(f"\n  Downloading {plat} ({dl_type})...")
        cmd = [
            sys.executable, ls_download,
            "--url", url,
            "--prefix", str(index),
            "--type", dl_type,
            "--output", CONFIG["nas_path"],
        ]
        
        try:
            result = subprocess.run(cmd)
            if result.returncode == 0:
                print(f"  ✔ {plat} download complete.")
                any_success = True
            else:
                print(f"  ✗ {plat} download failed (exit {result.returncode}).")
        except Exception as e:
            print(f"  ✗ {plat} download error: {e}")
    
    return any_success

# ── Write-back ────────────────────────────────────────────────────────────────

def apply_fix(fixed_lines, line_start, line_end):
    """Replace the entry in the Obsidian file."""
    with open(CONFIG["obsidian"], 'r', encoding='utf-8') as f:
        all_lines = f.readlines()
    
    # Preserve trailing --- if present
    has_sep = (line_end > 0 and line_end <= len(all_lines)
               and all_lines[line_end - 1].strip() == '---')
    
    replacement = fixed_lines[:]
    if has_sep and not any(l.strip() == '---' for l in replacement):
        replacement.append('---\n')
    
    new_lines = all_lines[:line_start] + replacement + all_lines[line_end:]
    
    with open(CONFIG["obsidian"], 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    
    print(f"  ✔ Entry #{line_start} updated in Obsidian file.")

# ── Audit Flow ────────────────────────────────────────────────────────────────

def audit(index, auto_fix=False):
    """
    Run the full audit for entry #index.
    
    Steps:
        1. Find + parse the entry
        2. Scan NAS for files
        3. Search for missing URLs (YouTube / Twitch, via cache)
        4. Report status
        5. Offer to download missing files
        6. Re-scan NAS if anything downloaded
        7. Show fixed entry, offer to apply
    """
    
    print(f"\n{'='*60}")
    print(f"  AUDIT: Entry #{index}")
    print(f"{'='*60}\n")
    
    # ── 1. Parse entry ──
    block, line_start, line_end = find_entry(index)
    if block is None:
        print(f"  ✗ Entry #{index} not found in Obsidian file.")
        return
    
    entry = parse_entry(block, index)
    
    print(f"  Date     : {entry['date_str'] or 'MISSING'} {entry['tz_str']}")
    print(f"  Checkbox : {entry['checkbox']}")
    print(f"  Format   : {'✔ OK' if entry['format_ok'] else '✗ needs rewrite'}")
    print()
    
    # Show current URLs
    print(f"  YT URL   : {entry['yt_url'] or '(none)'}")
    print(f"  TW URL   : {entry['tw_url'] or '(none)'}")
    print()
    
    # ── 2. Scan NAS ──
    print("  Scanning NAS...")
    nas = scan_nas(index)
    
    labels = {"yt_video": "YT video", "yt_chat": "YT chat", "tw_video": "TW video", "tw_chat": "TW chat"}
    for key, label in labels.items():
        if nas[key]:
            print(f"    ✔ {label}: {nas[key]}")
        else:
            print(f"    ✗ {label}: not found")
    print()
    
    # ── 3. Search for missing URLs ──
    cache = load_cache()
    yt_found = None
    tw_found = None
    
    has_yt = bool(entry["yt_url"]) or bool(nas["yt_video"])
    has_tw = bool(entry["tw_url"]) or bool(nas["tw_video"])
    
    if not has_yt and entry["date_obj"]:
        print("  YouTube link missing, searching cache...")
        yt_found = find_youtube_by_date(cache, entry["date_obj"])
        if yt_found:
            note = yt_found.get("note", "")
            print(f"    ✔ Found: {yt_found['title']}")
            print(f"      {yt_found['url']} {note}")
        else:
            print(f"    ✗ No YouTube stream found for {entry['date_str']}")
        print()
    
    if not has_tw and entry["date_obj"]:
        print("  Twitch link missing, searching cache...")
        tw_found = find_twitch_by_date(cache, entry["date_obj"])
        if tw_found:
            note = tw_found.get("note", "")
            print(f"    ✔ Found: {tw_found['title']}")
            print(f"      {tw_found['url']} {note}")
        else:
            print(f"    ✗ No Twitch VOD found for {entry['date_str']}")
        print()
    
    # Resolve final URLs for download identification
    yt_url = entry["yt_url"] or (yt_found["url"] if yt_found else None)
    tw_url = entry["tw_url"] or (tw_found["url"] if tw_found else None)
    
    # If NAS has a file, we can derive the URL from it even if entry didn't have one
    if not yt_url and nas["yt_video"]:
        vid = extract_video_id_from_filename(nas["yt_video"])
        if vid:
            yt_url = build_stream_url("youtube", vid)
    if not tw_url and nas["tw_video"]:
        vid = extract_video_id_from_filename(nas["tw_video"])
        if vid:
            tw_url = build_stream_url("twitch", vid)
    
    if not yt_url and not tw_url:
        print("  ⚠ No stream URLs found for either platform. Can't proceed with downloads.")
        print("    If this stream exists, add a URL manually and re-run.\n")
    
    # ── 4. Check for missing files + offer download ──
    missing = identify_missing(nas, yt_url, tw_url)
    
    downloaded = False
    if missing:
        downloaded = offer_downloads(missing, index)
    else:
        print("  ✔ All files present on NAS.")
    
    # ── 5. Re-scan NAS if we downloaded anything ──
    if downloaded:
        print("\n  Re-scanning NAS after downloads...")
        nas = scan_nas(index)
        for key, label in labels.items():
            if nas[key]:
                print(f"    ✔ {label}: {nas[key]}")
            else:
                print(f"    ✗ {label}: still missing")
        print()
    
    # ── 6. Build fixed entry + show preview ──
    fixed = build_fixed_entry(index, entry, nas, yt_found, tw_found)
    
    # Decide if rewrite is needed
    needs_rewrite = not entry["format_ok"] or downloaded or yt_found or tw_found
    
    # Always show remaining issues
    still_missing = identify_missing(nas, yt_url, tw_url)
    if still_missing:
        print("  Still missing:")
        for m in still_missing:
            print(f"    ✗ {m['label']}")
        print()
    
    if needs_rewrite or auto_fix:
        print("  Preview of fixed entry:")
        print("  " + "-" * 50)
        for line in fixed:
            print(f"  | {line.rstrip()}")
        print("  " + "-" * 50)
        print()
        
        confirm = input("  Apply this fix to Obsidian? (y/n): ").strip().lower()
        if confirm == 'y':
            apply_fix(fixed, line_start, line_end)
        else:
            print("  Skipped.")
    else:
        print("  ✔ Entry looks good, no rewrite needed.")
    
    print()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        return
    
    # Handle --refresh
    if sys.argv[1] == "--refresh":
        cache = load_cache()
        target = sys.argv[2] if len(sys.argv) > 2 else "all"
        if target in ("all", "youtube"):
            refresh_youtube(cache, full=True)
        if target in ("all", "twitch"):
            refresh_twitch(cache, full=True)
        print("\n  Cache refreshed.")
        return
    
    # Normal audit
    try:
        index = int(sys.argv[1])
    except ValueError:
        print(f"  Error: '{sys.argv[1]}' is not a valid index number.")
        return
    
    auto_fix = "--fix" in sys.argv
    
    if not os.path.exists(CONFIG["obsidian"]):
        print(f"  Error: Obsidian file not found: {CONFIG['obsidian']}")
        return
    
    audit(index, auto_fix=auto_fix)

if __name__ == "__main__":
    main()
