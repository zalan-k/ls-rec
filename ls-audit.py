#!/usr/bin/env python3
"""
ls-audit - Audit and fix Obsidian livestream log entries.

Usage:
    ls-audit <index>          Audit entry #index
    ls-audit <index> --fix    Audit and offer to rewrite entry
"""

import os, re, sys, glob, json, subprocess, datetime, urllib.parse, urllib.request

# ── Config (match livestream-recorder.py) ─────────────────────────────────────
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

# ── Helpers ───────────────────────────────────────────────────────────────────

def classify_video_id(vid):
    """YouTube IDs are 11 alphanumeric chars; Twitch VOD IDs are long numeric."""
    if vid.isdigit() and len(vid) > 11:
        return "twitch"
    return "youtube"

def extract_video_id_from_filename(filename):
    """Extract [video_id] from filename like 'title [ID] @ timestamp.ext'"""
    m = re.search(r'\[([^\]]+)\]\s*@\s*\d{4}-\d{2}-\d{2}', filename)
    return m.group(1) if m else None

def build_shell_cmd(filename):
    """Build obsidian shell-command URI for a file."""
    encoded = urllib.parse.quote(filename, safe='')
    return (f"obsidian://shell-commands/?vault={CONFIG['obsidian_vault']}"
            f"&execute={CONFIG['shellcmd_id']}&_arg0=raws/{encoded}")

def build_stream_url(platform, video_id):
    if platform == "youtube":
        return f"https://www.youtube.com/watch?v={video_id}"
    else:
        return f"https://www.twitch.tv/{CONFIG['twitch_user']}/video/{video_id}"

# ── Entry Parser ──────────────────────────────────────────────────────────────

def find_entry(index):
    """Find and return the raw text block for entry #index, plus its line range."""
    with open(CONFIG["obsidian"], 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Find the start line
    start = None
    for i, line in enumerate(lines):
        # New format: **519** or **519 (missing closing **)
        if re.search(rf'\*\*{index}\*?\*?\s*:', line):
            start = i
            break
        # Old format: [481_title](url) at line start
        if re.match(rf'^-\s*\[.\]\s*\[{index}_', line):
            start = i
            break
    
    if start is None:
        return None, None, None
    
    # Find the end (next --- or next entry header)
    end = len(lines)
    for i in range(start + 1, len(lines)):
        stripped = lines[i].strip()
        if stripped == '---':
            end = i + 1  # Include the ---
            break
        # Next entry header
        if re.match(r'^-\s*\[.\]\s*\*\*\d{3}\*\*', lines[i]):
            end = i
            break
        if re.match(r'^-\s*\[.\]\s*\[\d{3}_', lines[i]):
            end = i
            break
    
    block = lines[start:end]
    return block, start, end

def parse_entry(block, index):
    """Parse an entry block into structured data."""
    result = {
        "checkbox": "[ ]",
        "date_str": None,
        "date_obj": None,
        "yt_title": None,
        "yt_url": None,
        "tw_title": None,
        "tw_url": None,
        "header_file_links": [],  # Old-style [📁]/[🖿] on header
        "notes": [],              # Everything after platform lines
        "raw": block,
    }
    
    if not block:
        return result
    
    header = block[0]
    
    # Extract checkbox state
    cb = re.search(r'\[([ x])\]', header)
    if cb:
        result["checkbox"] = f"[{cb.group(1)}]"
    
    # Extract date
    dm = re.search(r'(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2})', ''.join(block[:5]))
    if dm:
        result["date_str"] = dm.group(1)
        try:
            result["date_obj"] = datetime.datetime.strptime(dm.group(1), "%Y.%m.%d %H:%M")
        except ValueError:
            pass
    
    # Parse platform lines and notes
    found_platform = False
    notes_start = 1
    
    for i, line in enumerate(block):
        if i == 0:
            continue
        
        stripped = line.strip()
        
        # Check for YT line
        if stripped.startswith('`YT`'):
            found_platform = True
            notes_start = i + 1
            url_match = re.search(r'\]\((https?://[^\s\)]+)\)?', stripped)
            title_match = re.search(r'\[([^\]]*\d{3}_[^\]]*)\]', stripped)
            if not title_match:
                # Try format: [ title ](url)
                title_match = re.search(r'\[\s*([^\]]+?)\s*\]\(https?://', stripped)
            if url_match:
                result["yt_url"] = url_match.group(1)
            if title_match:
                result["yt_title"] = title_match.group(1).strip()
            continue
        
        # Check for TW line
        if stripped.startswith('`TW`'):
            found_platform = True
            notes_start = i + 1
            url_match = re.search(r'\]\((https?://[^\s\)]+)\)?', stripped)
            title_match = re.search(r'\[([^\]]*\d{3}_[^\]]*)\]', stripped)
            if not title_match:
                title_match = re.search(r'\[\s*([^\]]+?)\s*\]\(https?://', stripped)
            if url_match:
                result["tw_url"] = url_match.group(1)
            if title_match:
                result["tw_title"] = title_match.group(1).strip()
            continue
    
    # For old format entries (no `YT`/`TW` lines), extract URLs from wherever
    if not found_platform:
        notes_start = 1
        for i, line in enumerate(block):
            for url_match in re.finditer(r'https://www\.youtube\.com/(?:watch\?v=|live/)([^\s\)]+)', line):
                if not result["yt_url"]:
                    result["yt_url"] = url_match.group(0).rstrip(')')
            for url_match in re.finditer(r'https://www\.twitch\.tv/tenma/video/(\d+)', line):
                if not result["tw_url"]:
                    result["tw_url"] = url_match.group(0).rstrip(')')
    
    # Detect Twitch URLs mislabeled as YouTube
    if result["yt_url"] and "twitch.tv" in result["yt_url"]:
        if not result["tw_url"]:
            result["tw_url"] = result["yt_url"]
            result["tw_title"] = result["yt_title"]
        result["yt_url"] = None
        result["yt_title"] = None
    
    # Detect YouTube URLs mislabeled as Twitch
    if result["tw_url"] and "youtube.com" in result["tw_url"]:
        if result["tw_url"] == result["yt_url"]:
            # Duplicate - just clear the TW line, it was copy-pasted
            result["tw_url"] = None
            result["tw_title"] = None
        elif not result["yt_url"]:
            result["yt_url"] = result["tw_url"]
            result["yt_title"] = result["tw_title"]
            result["tw_url"] = None
            result["tw_title"] = None
    
    # Collect notes (skip --- line and blank lines right after platform)
    for i in range(notes_start, len(block)):
        stripped = block[i].strip()
        if stripped == '---' or stripped == '':
            continue
        # Skip date lines that are part of old format headers
        if re.match(r'^\d{4}\.\d{2}\.\d{2}', stripped):
            continue
        result["notes"].append(block[i])
    
    return result

# ── NAS Scanner ───────────────────────────────────────────────────────────────

def scan_nas(index, entry):
    """Scan NAS for files belonging to this entry. Returns dict of findings."""
    nas = CONFIG["nas_path"]
    findings = {"yt_video": None, "yt_chat": None, "tw_video": None, "tw_chat": None}
    
    if not os.path.exists(nas):
        print("  ⚠ NAS not mounted, skipping file scan")
        return findings
    
    # Strategy 1: glob for index prefix
    pattern = os.path.join(nas, f"{index}_*")
    files = glob.glob(pattern)
    
    # Strategy 2: check filenames referenced in existing entry
    if not files:
        raw_text = ''.join(entry["raw"])
        # Decode URL-encoded filenames from [📁] and [🖿] links
        for encoded_match in re.finditer(r'raws/([^)\s]+\.(?:mp4|json))', raw_text):
            decoded = urllib.parse.unquote(encoded_match.group(1))
            full_path = os.path.join(nas, decoded)
            if os.path.exists(full_path) and full_path not in files:
                files.append(full_path)
    
    # Classify each file
    for filepath in files:
        filename = os.path.basename(filepath)
        vid = extract_video_id_from_filename(filename)
        if not vid:
            continue
        
        platform = classify_video_id(vid)
        ext = os.path.splitext(filename)[1].lower()
        
        key = f"{platform}_{'video' if ext == '.mp4' else 'chat'}"
        findings[key] = {"path": filepath, "filename": filename, "video_id": vid}
    
    return findings

# ── Twitch API ────────────────────────────────────────────────────────────────

def twitch_get_token():
    """Get OAuth token using client credentials."""
    data = urllib.parse.urlencode({
        "client_id": CONFIG["twitch_client_id"],
        "client_secret": CONFIG["twitch_client_secret"],
        "grant_type": "client_credentials"
    }).encode()
    
    req = urllib.request.Request("https://id.twitch.tv/oauth2/token", data=data, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())["access_token"]
    except Exception as e:
        print(f"  ⚠ Twitch auth failed: {e}")
        return None

def twitch_find_vod(target_date):
    """Find a Twitch VOD matching the target date."""
    token = twitch_get_token()
    if not token:
        return None
    
    headers = {
        "Client-ID": CONFIG["twitch_client_id"],
        "Authorization": f"Bearer {token}"
    }
    
    url = (f"https://api.twitch.tv/helix/videos"
           f"?user_id={CONFIG['twitch_user_id']}&type=archive&first=100")
    
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        
        target_str = target_date.strftime("%Y-%m-%d")
        
        for video in data.get("data", []):
            created = video["created_at"][:10]  # "2026-02-08T..."
            if created == target_str:
                return {
                    "id": video["id"],
                    "title": video["title"],
                    "url": f"https://www.twitch.tv/{CONFIG['twitch_user']}/video/{video['id']}",
                    "created_at": video["created_at"]
                }
        
        # Try day before/after (timezone edge cases)
        for delta in [-1, 1]:
            alt_str = (target_date + datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
            for video in data.get("data", []):
                created = video["created_at"][:10]
                if created == alt_str:
                    return {
                        "id": video["id"],
                        "title": video["title"],
                        "url": f"https://www.twitch.tv/{CONFIG['twitch_user']}/video/{video['id']}",
                        "created_at": video["created_at"],
                        "note": f"(date matched ±1 day: {alt_str})"
                    }
        
        return None
    except Exception as e:
        print(f"  ⚠ Twitch API error: {e}")
        return None

# ── YouTube Search ────────────────────────────────────────────────────────────

def youtube_find_stream(target_date):
    """Search for a YouTube stream on the target date using yt-dlp."""
    target_str = target_date.strftime("%Y%m%d")
    
    print(f"  Searching YouTube streams for {target_date.strftime('%Y-%m-%d')}...")
    
    try:
        cmd = [
            "yt-dlp", "--flat-playlist", "--dump-json",
            "--cookies-from-browser", "firefox",
            "--playlist-items", "1:80",
            f"https://www.youtube.com/{CONFIG['youtube_handle']}/streams"
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode != 0:
            print(f"  ⚠ yt-dlp streams search failed")
            return None
        
        for line in result.stdout.strip().split('\n'):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                upload_date = entry.get("upload_date", "")
                
                if upload_date == target_str:
                    vid_id = entry.get("id", "")
                    title = entry.get("title", "Unknown")
                    return {
                        "id": vid_id,
                        "title": title,
                        "url": f"https://www.youtube.com/watch?v={vid_id}"
                    }
            except json.JSONDecodeError:
                continue
        
        # Try ±1 day
        for delta in [-1, 1]:
            alt_str = (target_date + datetime.timedelta(days=delta)).strftime("%Y%m%d")
            for line in result.stdout.strip().split('\n'):
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                    if entry.get("upload_date", "") == alt_str:
                        vid_id = entry.get("id", "")
                        title = entry.get("title", "Unknown")
                        return {
                            "id": vid_id,
                            "title": title,
                            "url": f"https://www.youtube.com/watch?v={vid_id}",
                            "note": f"(date matched ±1 day: {alt_str})"
                        }
                except json.JSONDecodeError:
                    continue
        
        return None
        
    except subprocess.TimeoutExpired:
        print("  ⚠ YouTube search timed out")
        return None
    except Exception as e:
        print(f"  ⚠ YouTube search error: {e}")
        return None

# ── Report ────────────────────────────────────────────────────────────────────

def audit(index):
    """Run full audit for entry #index."""
    
    print(f"\n{'='*60}")
    print(f"  AUDIT: Entry #{index}")
    print(f"{'='*60}\n")
    
    # 1. Parse entry
    block, line_start, line_end = find_entry(index)
    if block is None:
        print(f"  ✗ Entry #{index} not found in Obsidian file.")
        return None
    
    entry = parse_entry(block, index)
    
    print(f"  Date: {entry['date_str'] or 'MISSING'}")
    print(f"  Status: {entry['checkbox']}")
    print()
    
    # Show current state
    print("  Current entry:")
    print(f"    YT: {entry['yt_url'] or '(none)'}")
    if entry['yt_title']:
        print(f"        {entry['yt_title']}")
    print(f"    TW: {entry['tw_url'] or '(none)'}")
    if entry['tw_title']:
        print(f"        {entry['tw_title']}")
    print()
    
    # 2. Scan NAS
    print("  Scanning NAS...")
    nas = scan_nas(index, entry)
    
    for key in ["yt_video", "yt_chat", "tw_video", "tw_chat"]:
        plat, ftype = key.split("_")
        label = f"{'YT' if plat == 'youtube' else 'TW'} {ftype}"
        if nas[key]:
            print(f"    ✔ {label}: {nas[key]['filename']}")
        else:
            print(f"    ✗ {label}: not found")
    print()
    
    # 3. Search for missing links
    yt_found = None
    tw_found = None
    
    # Determine what we already have
    has_yt = bool(entry["yt_url"]) or bool(nas["yt_video"])
    has_tw = bool(entry["tw_url"]) or bool(nas["tw_video"])
    
    if not has_yt and entry["date_obj"]:
        print("  YouTube link missing, searching...")
        yt_found = youtube_find_stream(entry["date_obj"])
        if yt_found:
            note = yt_found.get("note", "")
            print(f"    ✔ Found: {yt_found['title']}")
            print(f"      URL: {yt_found['url']} {note}")
        else:
            print(f"    ✗ No YouTube stream found for this date")
        print()
    
    if not has_tw and entry["date_obj"]:
        print("  Twitch link missing, searching API...")
        tw_found = twitch_find_vod(entry["date_obj"])
        if tw_found:
            note = tw_found.get("note", "")
            print(f"    ✔ Found: {tw_found['title']}")
            print(f"      URL: {tw_found['url']} {note}")
        else:
            print(f"    ✗ No Twitch VOD found for this date")
        print()
    
    # 4. Build consolidated info
    info = build_consolidated_info(index, entry, nas, yt_found, tw_found)
    
    # 5. Show what the fixed entry would look like
    issues = info["issues"]
    if issues:
        print(f"  Issues found ({len(issues)}):")
        for issue in issues:
            print(f"    • {issue}")
        print()
    else:
        print("  ✔ No issues found.\n")
    
    # 6. Show files that need downloading
    if info["needs_download"]:
        print("  Missing files (need download):")
        for dl in info["needs_download"]:
            print(f"    ↓ {dl['label']}")
        print()
    
    return {
        "index": index,
        "entry": entry,
        "nas": nas,
        "yt_found": yt_found,
        "tw_found": tw_found,
        "info": info,
        "line_start": line_start,
        "line_end": line_end
    }

def build_consolidated_info(index, entry, nas, yt_found, tw_found):
    """Consolidate all sources into final info for the entry."""
    issues = []
    needs_download = []
    
    # YouTube info - priority: NAS file > existing entry > API search
    yt_url = None
    yt_title = None
    yt_video_file = None
    yt_chat_file = None
    
    if nas["yt_video"]:
        vid = nas["yt_video"]["video_id"]
        yt_url = build_stream_url("youtube", vid)
        yt_video_file = nas["yt_video"]["filename"]
    if nas["yt_chat"]:
        yt_chat_file = nas["yt_chat"]["filename"]
    
    if entry["yt_url"] and not yt_url:
        yt_url = entry["yt_url"]
    if entry["yt_title"]:
        yt_title = entry["yt_title"]
    
    if yt_found and not yt_url:
        yt_url = yt_found["url"]
        yt_title = yt_found.get("title")
    
    # Twitch info
    tw_url = None
    tw_title = None
    tw_video_file = None
    tw_chat_file = None
    
    if nas["tw_video"]:
        vid = nas["tw_video"]["video_id"]
        tw_url = build_stream_url("twitch", vid)
        tw_video_file = nas["tw_video"]["filename"]
    if nas["tw_chat"]:
        tw_chat_file = nas["tw_chat"]["filename"]
    
    if entry["tw_url"] and not tw_url:
        tw_url = entry["tw_url"]
    if entry["tw_title"]:
        tw_title = entry["tw_title"]
    
    if tw_found and not tw_url:
        tw_url = tw_found["url"]
        tw_title = tw_found.get("title")
    
    # Generate titles from filenames if missing
    if not yt_title and yt_video_file:
        yt_title = _title_from_filename(yt_video_file)
    if not tw_title and tw_video_file:
        tw_title = _title_from_filename(tw_video_file)
    
    # Identify issues
    if entry["yt_url"] and "twitch.tv" in entry["yt_url"]:
        issues.append("Twitch URL was on the YT line (swapped)")
    
    raw_header = entry["raw"][0] if entry["raw"] else ""
    if '[📁]' in raw_header or '[🖿]' in raw_header:
        issues.append("Old format: file links on header line instead of platform lines")
    if '🖿' in ''.join(entry["raw"]):
        issues.append("Old icon: 🖿 should be 📄")
    
    if yt_url and not yt_video_file:
        needs_download.append({"platform": "youtube", "type": "video", "url": yt_url, "label": f"YT video: {yt_url}"})
    if yt_url and not yt_chat_file:
        needs_download.append({"platform": "youtube", "type": "chat", "url": yt_url, "label": f"YT chat: {yt_url}"})
    if tw_url and not tw_video_file:
        needs_download.append({"platform": "twitch", "type": "video", "url": tw_url, "label": f"TW video: {tw_url}"})
    if tw_url and not tw_chat_file:
        needs_download.append({"platform": "twitch", "type": "chat", "url": tw_url, "label": f"TW chat: {tw_url}"})
    
    if not yt_url and not tw_url:
        issues.append("No stream URLs found for either platform")
    
    return {
        "yt_url": yt_url, "yt_title": yt_title,
        "yt_video_file": yt_video_file, "yt_chat_file": yt_chat_file,
        "tw_url": tw_url, "tw_title": tw_title,
        "tw_video_file": tw_video_file, "tw_chat_file": tw_chat_file,
        "issues": issues, "needs_download": needs_download
    }

def _title_from_filename(filename):
    """Extract a clean title from a NAS filename."""
    # Remove extension
    name = os.path.splitext(filename)[0]
    # Remove [video_id] @ timestamp suffix
    name = re.sub(r'\s*\[[^\]]+\]\s*@\s*\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$', '', name)
    return name

# ── Entry Rewriter ────────────────────────────────────────────────────────────

def generate_fixed_entry(audit_result):
    """Generate the corrected entry text (header + platform lines only)."""
    info = audit_result["info"]
    entry = audit_result["entry"]
    index = audit_result["index"]
    
    # Header line
    date_str = entry["date_str"] or "UNKNOWN DATE"
    # Reconstruct timezone from original
    tz_match = re.search(r'\(GMT[^\)]+\)', ''.join(entry["raw"][:3]))
    tz_str = tz_match.group(0) if tz_match else "(GMT-6)"
    
    header = f"- {entry['checkbox']} **{index}** : {date_str} {tz_str}  \n"
    
    # YT line
    yt_line = _build_platform_line("YT", info["yt_title"], info["yt_url"],
                                    info["yt_video_file"], info["yt_chat_file"])
    
    # TW line
    tw_line = _build_platform_line("TW", info["tw_title"], info["tw_url"],
                                    info["tw_video_file"], info["tw_chat_file"])
    
    # Combine header + platform lines + original notes
    lines = [header, yt_line, tw_line]
    
    if entry["notes"]:
        lines.extend(entry["notes"])
    
    return lines

def _build_platform_line(tag, title, url, video_file, chat_file):
    """Build a single platform line in the new format."""
    if not url and not title:
        return f"\t`{tag}` \n"
    
    # File links
    if video_file:
        video_link = f"[📁]({build_shell_cmd(video_file)})"
    else:
        video_link = "[📁]()"
    
    if chat_file:
        chat_link = f"[📄]({build_shell_cmd(chat_file)})"
    else:
        chat_link = "[📄]()"
    
    # Title and URL
    display_title = title or "untitled"
    if url:
        title_part = f"[ {display_title} ]({url})"
    else:
        title_part = f"[ {display_title} ]()"
    
    return f"\t`{tag}` {video_link} {chat_link} {title_part}\n"

def apply_fix(audit_result):
    """Write the fixed entry back to the Obsidian file."""
    fixed_lines = generate_fixed_entry(audit_result)
    line_start = audit_result["line_start"]
    line_end = audit_result["line_end"]
    
    with open(CONFIG["obsidian"], 'r', encoding='utf-8') as f:
        all_lines = f.readlines()
    
    # Check if there's a --- at the end that should be preserved
    has_separator = False
    if line_end > 0 and line_end <= len(all_lines):
        if all_lines[line_end - 1].strip() == '---':
            has_separator = True
    
    # Build replacement
    replacement = fixed_lines[:]
    if has_separator and not any(l.strip() == '---' for l in replacement):
        replacement.append('---\n')
    
    # Replace
    new_lines = all_lines[:line_start] + replacement + all_lines[line_end:]
    
    with open(CONFIG["obsidian"], 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    
    print(f"  ✔ Entry #{audit_result['index']} updated.")

# ── Download Integration ──────────────────────────────────────────────────────

def find_ls_download():
    """Find ls-download.py in the same directory as this script."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, "ls-download.py")
    if os.path.exists(path):
        return path
    return None

def offer_downloads(audit_result):
    """Offer to download missing files using ls-download.py."""
    info = audit_result["info"]
    index = audit_result["index"]
    downloads = info["needs_download"]
    
    if not downloads:
        return
    
    ls_download = find_ls_download()
    if not ls_download:
        print("  ⚠ ls-download.py not found in script directory, can't auto-download.")
        return
    
    confirm = input("  Download missing files? (y/n): ").strip().lower()
    if confirm != 'y':
        print("  Skipped downloads.")
        return
    
    # Group downloads by URL to avoid redundant calls
    # If both video and chat are missing for same URL, download "both"
    by_url = {}
    for dl in downloads:
        url = dl["url"]
        if url not in by_url:
            by_url[url] = {"url": url, "platform": dl["platform"], "types": set()}
        by_url[url]["types"].add(dl["type"])
    
    for url, group in by_url.items():
        types = group["types"]
        if "video" in types and "chat" in types:
            dl_type = "both"
        elif "video" in types:
            dl_type = "video"
        else:
            dl_type = "chat"
        
        plat = group["platform"].upper()
        print(f"\n  Downloading {plat} ({dl_type})...")
        
        cmd = [
            sys.executable, ls_download,
            "--url", url,
            "--prefix", str(index),
            "--type", dl_type,
            "--output", CONFIG["nas_path"]
        ]
        
        try:
            result = subprocess.run(cmd)
            if result.returncode == 0:
                print(f"  ✔ {plat} download complete.")
            else:
                print(f"  ✗ {plat} download failed (exit {result.returncode}).")
        except Exception as e:
            print(f"  ✗ {plat} download error: {e}")
    
    print()
    print("  Tip: Re-run ls-audit to verify files and update Obsidian entry.")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        return
    
    try:
        index = int(sys.argv[1])
    except ValueError:
        print(f"Error: '{sys.argv[1]}' is not a valid index number.")
        return
    
    auto_fix = "--fix" in sys.argv
    
    # Check file exists
    if not os.path.exists(CONFIG["obsidian"]):
        print(f"Error: Obsidian file not found: {CONFIG['obsidian']}")
        return
    
    # Run audit
    result = audit(index)
    if result is None:
        return
    
    info = result["info"]
    
    # Show preview and offer to fix
    if info["issues"] or auto_fix:
        print("  Preview of fixed entry:")
        print("  " + "-" * 40)
        fixed = generate_fixed_entry(result)
        for line in fixed:
            print(f"  | {line.rstrip()}")
        print("  " + "-" * 40)
        print()
        
        if auto_fix:
            confirm = input("  Apply this fix? (y/n): ").strip().lower()
        else:
            confirm = input("  Issues detected. Apply fix? (y/n): ").strip().lower()
        
        if confirm == 'y':
            apply_fix(result)
        else:
            print("  Skipped.")
    
    # Offer to download missing files
    if info["needs_download"]:
        offer_downloads(result)
    
    print()

if __name__ == "__main__":
    main()
