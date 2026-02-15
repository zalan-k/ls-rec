#!/usr/bin/env python3
"""
ls-audit - Reconstruct Obsidian livestream entries from authoritative sources.

Philosophy:
    The existing entry is only used for: date, checkbox state, and sub-notes.
    Everything else (titles, URLs, file links) is rebuilt fresh from
    yt-dlp / Twitch API / NAS filesystem every time.

Usage:
    ls-audit <index>            Reconstruct entry #index
    ls-audit --refresh          Force-refresh the stream cache
    ls-audit --refresh youtube  Force-refresh YouTube cache only
    ls-audit --refresh twitch   Force-refresh Twitch cache only
"""

import os, re, sys, json, glob, subprocess, shutil, datetime, urllib.parse, urllib.request

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
    if vid.lstrip('v').isdigit():
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

def copy_to_clipboard(text):
    """Try to copy text to system clipboard. Returns True on success."""
    # Try xclip first, then xsel, then wl-copy (Wayland)
    for cmd in [["xclip", "-selection", "clipboard"],
                ["xsel", "--clipboard", "--input"],
                ["wl-copy"]]:
        if shutil.which(cmd[0]):
            try:
                subprocess.run(cmd, input=text.encode(), check=True, timeout=5)
                return True
            except (subprocess.SubprocessError, OSError):
                continue
    return False

# ── Stream Cache ──────────────────────────────────────────────────────────────

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {"youtube": {"streams": []}, "twitch": {"vods": []}}

def save_cache(cache):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)

def _yt_newest_date(cache):
    streams = cache.get("youtube", {}).get("streams", [])
    return max((s["upload_date"] for s in streams), default=None)

def _tw_newest_date(cache):
    vods = cache.get("twitch", {}).get("vods", [])
    return max((v["created_at"][:10] for v in vods), default=None)

# ── YouTube: yt-dlp ──────────────────────────────────────────────────────────

def refresh_youtube(cache, full=False):
    count = 100 if full else 10
    print(f"  ⌛ Refreshing YouTube cache ({'full' if full else 'incremental'}, last {count})...")

    cmd = [
        "yt-dlp", "--dump-json",
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
            title = data.get("title", "Unknown")
            upload_date = data.get("upload_date") or data.get("release_date") or ""
            if vid_id and upload_date:
                new_streams.append({
                    "id": vid_id,
                    "title": title,
                    "upload_date": upload_date,
                    "duration": data.get("duration")
                })
        except json.JSONDecodeError:
            continue

    if not new_streams:
        print(f"  ⚠ No streams parsed from yt-dlp output")
        return False

    existing = {s["id"]: s for s in cache.get("youtube", {}).get("streams", [])}
    for s in new_streams:
        existing[s["id"]] = s

    merged = sorted(existing.values(), key=lambda s: s["upload_date"], reverse=True)
    cache["youtube"] = {"streams": merged}
    save_cache(cache)

    print(f"  ✔ YouTube cache: {len(merged)} streams (newest: {merged[0]['upload_date']})")
    return True

# ── Twitch: Helix API ────────────────────────────────────────────────────────

def _twitch_get_token():
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

    existing = {v["id"]: v for v in cache.get("twitch", {}).get("vods", [])}
    for v in all_vods:
        existing[v["id"]] = v

    merged = sorted(existing.values(), key=lambda v: v["created_at"], reverse=True)
    cache["twitch"] = {"vods": merged}
    save_cache(cache)

    print(f"  ✔ Twitch cache: {len(merged)} VODs (newest: {merged[0]['created_at'][:10]})")
    return True

# ── Date-based stream lookup ─────────────────────────────────────────────────

def find_youtube_by_date(cache, target_date):
    """Search cache for YouTube stream on target_date. Auto-refreshes if needed."""
    target_str = target_date.strftime("%Y%m%d")
    newest = _yt_newest_date(cache)

    if newest is None:
        refresh_youtube(cache, full=True)
    elif target_str > newest:
        refresh_youtube(cache, full=False)

    streams = cache.get("youtube", {}).get("streams", [])

    # Exact match
    for s in streams:
        if s["upload_date"] == target_str:
            return {"id": s["id"], "title": s["title"]}

    # ±1 day (timezone edge cases)
    for delta in [-1, 1]:
        alt_str = (target_date + datetime.timedelta(days=delta)).strftime("%Y%m%d")
        for s in streams:
            if s["upload_date"] == alt_str:
                return {"id": s["id"], "title": s["title"], "note": f"±1d: {alt_str}"}

    return None

def find_twitch_by_date(cache, target_date):
    """Search cache for Twitch VOD on target_date. Auto-refreshes if needed."""
    target_str = target_date.strftime("%Y-%m-%d")
    newest = _tw_newest_date(cache)

    if newest is None:
        refresh_twitch(cache, full=True)
    elif target_str > newest:
        refresh_twitch(cache, full=False)

    vods = cache.get("twitch", {}).get("vods", [])

    for v in vods:
        if v["created_at"][:10] == target_str:
            return {"id": v["id"], "title": v["title"]}

    for delta in [-1, 1]:
        alt_str = (target_date + datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
        for v in vods:
            if v["created_at"][:10] == alt_str:
                return {"id": v["id"], "title": v["title"], "note": f"±1d: {alt_str}"}

    return None

# ── Obsidian: minimal entry reader ───────────────────────────────────────────
#
# We only extract: date, checkbox, and sub-notes.
# Everything else is reconstructed from API + NAS.

def read_entry(index):
    """
    Find entry #index in the Obsidian file.
    Returns: {
        "found": bool,
        "date_str": "2026.02.08 04:15" or None,
        "date_obj": datetime or None,
        "tz_str": "(GMT-6)" or None,
        "checkbox": "[ ]" or "[x]",
        "notes": [lines...],        # sub-notes/sub-checkboxes below the entry
    }
    """
    result = {
        "found": False,
        "date_str": None, "date_obj": None, "tz_str": "(GMT-6)",
        "checkbox": "[ ]", "notes": [],
    }

    if not os.path.exists(CONFIG["obsidian"]):
        print(f"  ✗ Obsidian file not found: {CONFIG['obsidian']}")
        return result

    with open(CONFIG["obsidian"], 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Find the header line for this index
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

    # Date + timezone
    dm = re.search(r'(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2})', header)
    if dm:
        result["date_str"] = dm.group(1)
        try:
            result["date_obj"] = datetime.datetime.strptime(dm.group(1), "%Y.%m.%d %H:%M")
        except ValueError:
            pass

    tz = re.search(r'(\(GMT[^\)]*\))', header)
    if tz:
        result["tz_str"] = tz.group(1)

    # Find end of entry, collect sub-notes
    # Sub-notes are lines after the first 3 lines (header + YT + TW) that aren't
    # the separator or next entry. But since we don't know if YT/TW lines exist
    # in the current entry (it might be malformed), we skip all tab-indented
    # platform lines and collect everything else until the next entry or ---.
    i = start + 1
    # Skip known platform sublines (tab-indented lines starting with `YT` or `TW`)
    while i < len(lines):
        stripped = lines[i].strip()
        if stripped == '---' or re.match(r'^-\s*\[.\]\s*\*\*\d+\*\*', lines[i]):
            break
        m_plat = re.match(r'^\t`(YT|TW)`\s*(✗|✘)', lines[i])
        if m_plat:
            result.setdefault("no_platform", set()).add(m_plat.group(1))
            i += 1
            continue
        if re.match(r'^\t`(YT|TW)`', lines[i]):
            i += 1
            continue
        # This is a sub-note line
        result["notes"].append(lines[i])
        i += 1

    return result

def write_entry(index, new_lines):
    """Replace entry #index in the Obsidian file with new_lines."""
    if not os.path.exists(CONFIG["obsidian"]):
        print(f"  ✗ Obsidian file not found: {CONFIG['obsidian']}")
        return False

    with open(CONFIG["obsidian"], 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Find start of this entry
    start = None
    for i, line in enumerate(lines):
        if re.search(rf'\*\*{index}\*\*\s*:', line):
            start = i
            break

    if start is None:
        print(f"  ✗ Entry #{index} not found for writing.")
        return False

    # Find end of entry (next entry or --- separator)
    end = start + 1
    while end < len(lines):
        stripped = lines[end].strip()
        if stripped == '---' or re.match(r'^-\s*\[.\]\s*\*\*\d+\*\*', lines[end]):
            break
        end += 1

    # Replace
    replacement = [line + "\n" for line in new_lines]
    lines[start:end] = replacement

    with open(CONFIG["obsidian"], 'w', encoding='utf-8') as f:
        f.writelines(lines)

    return True

# ── NAS Scanner ──────────────────────────────────────────────────────────────

def scan_nas(index):
    found = {"yt_video": None, "yt_chat": None, "tw_video": None, "tw_chat": None}
    nas = CONFIG["nas_path"]
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

# ── Entry Builder ────────────────────────────────────────────────────────────

def build_platform_line(tag, url, title, video_file, chat_file):
    vid_link = f"[📁]({build_shell_cmd(video_file)})" if video_file else "[📁]()"
    chat_link = f"[📄]({build_shell_cmd(chat_file)})" if chat_file else "[📄]()"

    display = title or "untitled"
    if url:
        title_link = f"[ {display} ]({url})"
    else:
        title_link = f"[ {display} ]()"

    return f"\t`{tag}` {vid_link} {chat_link} {title_link}"

def build_entry(index, entry, nas, yt_info, tw_info):
    # ── Resolve YouTube ──
    yt_url, yt_title = None, None
    no_plat = entry.get("no_platform", set())
    
    # Priority 1: NAS file
    if nas["yt_video"]:
        vid = extract_video_id_from_filename(nas["yt_video"])
        if vid:
            yt_url = build_stream_url("youtube", vid)
        yt_title = _title_from_filename(nas["yt_video"])

    # Priority 2: API/cache
    if yt_info:
        if not yt_url:
            yt_url = build_stream_url("youtube", yt_info["id"])
        if not yt_title:
            yt_title = yt_info.get("title")

    # ── Resolve Twitch ──
    tw_url, tw_title = None, None

    if nas["tw_video"]:
        vid = extract_video_id_from_filename(nas["tw_video"])
        if vid:
            tw_url = build_stream_url("twitch", vid)
        tw_title = _title_from_filename(nas["tw_video"])

    if tw_info:
        if not tw_url:
            tw_url = build_stream_url("twitch", tw_info["id"])
        if not tw_title:
            tw_title = tw_info.get("title")

    # ── Assemble ──
    date_str = entry["date_str"] or "UNKNOWN"
    tz_str = entry["tz_str"] or "(GMT-6)"

    lines = []
    lines.append(f"- {entry['checkbox']} **{index}** : {date_str} {tz_str}  ")
    if "YT" in no_plat:
        lines.append("\t`YT` ✗")
    else:
        lines.append(build_platform_line("YT", yt_url, yt_title, nas["yt_video"], nas["yt_chat"]))

    if "TW" in no_plat:
        lines.append("\t`TW` ✗")
    else:
        lines.append(build_platform_line("TW", tw_url, tw_title, nas["tw_video"], nas["tw_chat"]))

    # Append preserved sub-notes
    for note in entry.get("notes", []):
        lines.append(note.rstrip())

    return lines

def _title_from_filename(filename):
    """Extract clean title from NAS filename."""
    name = os.path.splitext(filename)[0]
    name = re.sub(r'^\d+_', '', name)
    name = re.sub(r'\s*\[[^\]]+\]\s*@\s*\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$', '', name)
    return name

# ── Download Integration ─────────────────────────────────────────────────────

def find_ls_download():
    path = os.path.join(SCRIPT_DIR, "ls-download.py")
    return path if os.path.exists(path) else None

def identify_missing(nas, yt_url, tw_url):
    """Figure out what needs downloading."""
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
    """Offer to download missing files via ls-download.py. Returns True if anything downloaded."""
    ls_download = find_ls_download()
    if not ls_download:
        print("  ⚠ ls-download.py not found, can't auto-download.")
        return False

    print("\n  Missing files:")
    for m in missing:
        print(f"    ↓ {m['label']}: {m['url']}")

    confirm = input("\n  Download missing files? (y/n): ").strip().lower()
    if confirm != 'y':
        print("  Skipped.")
        return False

    # Group by URL
    by_url = {}
    for m in missing:
        url = m["url"]
        if url not in by_url:
            by_url[url] = {"url": url, "platform": m["platform"], "types": set()}
        by_url[url]["types"].add(m["type"])

    any_success = False
    for url, group in by_url.items():
        types = group["types"]
        dl_type = "both" if len(types) > 1 else next(iter(types))
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

# ── Main Audit Flow ──────────────────────────────────────────────────────────

def audit(index):
    """
    Reconstruct entry #index from scratch.

    Flow:
        1. Read Obsidian → date + checkbox + notes (nothing else)
        2. Load cache → find YouTube + Twitch streams by date
        3. Scan NAS → find existing files
        4. Build entry block from API data + NAS files
        5. Print + copy to clipboard
        6. If files missing → offer download → re-scan → rebuild
    """

    print(f"\n{'='*60}")
    print(f"  Reconstructing entry #{index}")
    print(f"{'='*60}\n")

    # ── 1. Read Obsidian (date + checkbox only) ──
    entry = read_entry(index)
    if not entry["found"]:
        print(f"  ✗ Entry #{index} not found in Obsidian file.")
        return

    if not entry["date_obj"]:
        print(f"  ✗ Could not parse date from entry #{index}.")
        print(f"    Raw date_str: {entry['date_str']}")
        return

    print(f"  Date     : {entry['date_str']} {entry['tz_str']}")
    print(f"  Checkbox : {entry['checkbox']}")
    print()

    no_plat = entry.get("no_platform", set())

    # ── 2. Search cache for streams ──
    cache = load_cache()

    if "YT" not in no_plat:
        print("  Searching YouTube...")
        yt_info = find_youtube_by_date(cache, entry["date_obj"])
        if yt_info:
            note = yt_info.get("note", "")
            print(f"    ✔ {yt_info['title'][:60]}  {note}")
        else:
            print(f"    ✗ No YouTube stream found for {entry['date_str']}")
    else:
        print("  Skipping YouTube...")
        yt_info = None

    if "TW" not in no_plat:
        print("  Searching Twitch...")
        tw_info = find_twitch_by_date(cache, entry["date_obj"])
        if tw_info:
            note = tw_info.get("note", "")
            print(f"    ✔ {tw_info['title'][:60]}  {note}")
        else:
            print(f"    ✗ No Twitch VOD found for {entry['date_str']}")
    else:
        print("  Skipping Twitch...")
        tw_info = None
    print()

    # ── 3. Scan NAS ──
    print("  Scanning NAS...")
    nas = scan_nas(index)

    labels = {"yt_video": "YT video", "yt_chat": "YT chat",
              "tw_video": "TW video", "tw_chat": "TW chat"}
    for key, label in labels.items():
        status = f"✔ {nas[key]}" if nas[key] else "✗ not found"
        print(f"    {status[:2]} {label}: {status[2:]}")
    print()

    # ── 4. Build entry ──
    block = build_entry(index, entry, nas, yt_info, tw_info)
    output_text = "\n".join(block)

    # ── 5. Write to Obsidian ──
    print("  ┌─ Entry ────────────────────────────────────────────")
    for line in block:
        print(f"  │ {line}")
    print("  └──────────────────────────────────────────────────────")
    print()

    confirm = input("  Write to Obsidian? (y/n): ").strip().lower()
    if confirm == 'y':
        if write_entry(index, block):
            print("  ✔ Written to Obsidian file.")
        else:
            print("  ✗ Failed to write to Obsidian file.")
    else:
        print("  Skipped.")
    print()

    # ── 6. Check for missing files → offer download ──
    yt_url = build_stream_url("youtube", yt_info["id"]) if yt_info else None
    tw_url = build_stream_url("twitch", tw_info["id"]) if tw_info else None

    # Also derive URLs from NAS if cache didn't have them
    if not yt_url and nas["yt_video"]:
        vid = extract_video_id_from_filename(nas["yt_video"])
        if vid:
            yt_url = build_stream_url("youtube", vid)
    if not tw_url and nas["tw_video"]:
        vid = extract_video_id_from_filename(nas["tw_video"])
        if vid:
            tw_url = build_stream_url("twitch", vid)

    missing = identify_missing(nas, yt_url, tw_url)

    if not missing:
        print("  ✔ All files present on NAS.\n")
        return

    downloaded = offer_downloads(missing, index)

    if downloaded:
        # Re-scan, rebuild, re-output
        print("\n  Re-scanning NAS...")
        nas = scan_nas(index)
        for key, label in labels.items():
            status = f"✔ {nas[key]}" if nas[key] else "✗ still missing"
            print(f"    {status[:2]} {label}: {status[2:]}")
        print()

        block = build_entry(index, entry, nas, yt_info, tw_info)
        output_text = "\n".join(block)

        print("  ┌─ Updated Entry ────────────────────────────────────")
        for line in block:
            print(f"  │ {line}")
        print("  └──────────────────────────────────────────────────────")
        print()

        confirm = input("  Write to Obsidian? (y/n): ").strip().lower()
        if confirm == 'y':
            if write_entry(index, block):
                print("  ✔ Written to Obsidian file.")
            else:
                print("  ✗ Failed to write to Obsidian file.")
        else:
            print("  Skipped.")
        print()

# ── CLI ──────────────────────────────────────────────────────────────────────

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

    # Normal: reconstruct entry
    try:
        index = int(sys.argv[1])
    except ValueError:
        print(f"  Error: '{sys.argv[1]}' is not a valid index number.")
        return

    audit(index)

if __name__ == "__main__":
    main()