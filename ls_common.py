"""
ls_common - Shared infrastructure for the ls-rec toolset.

Config loading, yt-dlp command building, Twitch Helix API, VOD cache,
Obsidian entry helpers, Twitch IRC chat recorder, and utilities.
"""

import datetime, glob, json, os, re, socket, subprocess, time
import urllib.parse, urllib.request
from typing import Any, Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_PATH = os.path.join(SCRIPT_DIR, ".vod_cache.json")

VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".ts", ".flv", ".mov")


# ═══════════════════════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════════════════════

_DEFAULTS: dict[str, Any] = {
    "check_interval":    60,
    "cooldown_duration": 30,
    "dual_stream_cycle": 10,
    "cookies_browser":   "firefox",
}

_REQUIRED = (
    "obsidian", "obsidian_vault", "shellcmd_id",
    "nas_path", "output",
    "youtube_handle", "twitch_user",
)


def load_config(path: str | None = None) -> dict[str, Any]:
    """Load config.json, apply defaults, validate required keys."""
    path = path or os.path.join(SCRIPT_DIR, "config.json")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"config.json not found at {path}. "
            "Copy config.example.json and fill in the values."
        )
    with open(path) as f:
        raw = json.load(f)
    cfg = dict(_DEFAULTS)
    cfg.update(raw)
    missing = [k for k in _REQUIRED if not cfg.get(k)]
    if missing:
        raise ValueError(f"config.json missing required keys: {missing}")
    return cfg


# ═══════════════════════════════════════════════════════════════════════════
#  YT-DLP
# ═══════════════════════════════════════════════════════════════════════════
#
#  Functions that build yt-dlp command lines. The binary path and cookie
#  args are resolved from config so callers never construct their own argv.

def _ytdlp_base(config: dict) -> list[str]:
    venv = config.get("venv")
    binary = os.path.join(venv, "bin", "yt-dlp") if venv else "yt-dlp"
    return [binary, "--cookies-from-browser", config.get("cookies_browser", "firefox")]


def ytdlp_probe(config: dict, url: str, *,
                playlist_items: str | None = None,
                timeout: int = 30) -> dict | None:
    """Probe URL for metadata. Returns parsed JSON dict or None."""
    cmd = _ytdlp_base(config) + ["--dump-json"]
    if playlist_items:
        cmd += ["--playlist-items", playlist_items]
    cmd.append(url)
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return None
    if r.returncode != 0 or not r.stdout.strip():
        return None
    try:
        return json.loads(r.stdout.strip().split("\n", 1)[0])
    except json.JSONDecodeError:
        return None


def ytdlp_dump_playlist(config: dict, url: str, playlist_items: str, *,
                        timeout: int = 300) -> list[dict]:
    """Dump multiple playlist entries as parsed dicts."""
    cmd = _ytdlp_base(config) + [
        "--dump-json", "--playlist-items", playlist_items, url,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return []
    entries = []
    for line in (r.stdout or "").strip().split("\n"):
        if not line.strip():
            continue
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return entries


def ytdlp_live_cmd(config: dict, url: str, platform: str,
                   output_template: str) -> list[str]:
    """Build command for live stream recording.

    YouTube uses plain 'best' — constraining to mp4/avc1 caused yt-dlp to
    drop long streams after ~4h. Container may end up as webm/mkv.
    Twitch keeps mp4/avc1 since HLS VODs segment cleanly.
    """
    fmt = ("best" if platform == "youtube"
           else "bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best[ext=mp4]/best")
    cmd = _ytdlp_base(config) + [
        "--format",           fmt,
        "-o",                 output_template,
        "--no-part",
        "--retries",          "10",
        "--fragment-retries", "3",
        "--retry-sleep",      "exp=1::10",
        "--retry-sleep",      "fragment:exp=2::15",
        "--socket-timeout",   "15",
    ]
    if platform == "twitch":
        cmd += ["--concurrent-fragments", "4"]
    cmd.append(url)
    return cmd


def ytdlp_vod_cmd(config: dict, url: str, output_template: str) -> list[str]:
    """Build command for post-hoc VOD download."""
    cmd = _ytdlp_base(config) + [
        "--format",
        "bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "-o", output_template,
        "--no-part", "--no-mtime",
        "--concurrent-fragments", "16",
    ]
    if "twitch.tv" in url:
        cmd += ["--remux-video", "mp4"]
    cmd.append(url)
    return cmd


def ytdlp_chat_cmd(config: dict, url: str, output_template: str) -> list[str]:
    """Build command for chat download via yt-dlp live_chat subs."""
    return _ytdlp_base(config) + [
        "--skip-download", "--write-subs",
        "--sub-langs", "live_chat",
        "-o", output_template, url,
    ]


# ═══════════════════════════════════════════════════════════════════════════
#  TWITCH HELIX API
# ═══════════════════════════════════════════════════════════════════════════

def twitch_get_token(config: dict) -> str | None:
    cid = config.get("twitch_client_id")
    secret = config.get("twitch_client_secret")
    if not (cid and secret):
        return None
    data = urllib.parse.urlencode({
        "client_id": cid, "client_secret": secret,
        "grant_type": "client_credentials",
    }).encode()
    req = urllib.request.Request(
        "https://id.twitch.tv/oauth2/token", data=data, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())["access_token"]
    except Exception:
        return None


def twitch_list_vods(config: dict, limit: int = 100) -> list[dict]:
    """Fetch recent VODs via Twitch Helix API."""
    token = twitch_get_token(config)
    if not token:
        return []
    user_id = config.get("twitch_user_id")
    if not user_id:
        return []
    headers = {
        "Client-ID": config["twitch_client_id"],
        "Authorization": f"Bearer {token}",
    }
    vods: list[dict] = []
    cursor = None
    fetched = 0
    while fetched < limit:
        batch = min(100, limit - fetched)
        url = (f"https://api.twitch.tv/helix/videos"
               f"?user_id={user_id}&type=archive&first={batch}")
        if cursor:
            url += f"&after={cursor}"
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
        except Exception:
            break
        videos = data.get("data", [])
        if not videos:
            break
        vods.extend(videos)
        fetched += len(videos)
        cursor = data.get("pagination", {}).get("cursor")
        if not cursor:
            break
    return vods


def parse_twitch_duration(dur_str: str | None) -> int | None:
    """Parse Twitch duration string like '3h24m18s' into seconds."""
    if not dur_str:
        return None
    total = 0
    for m in re.finditer(r"(\d+)([hms])", dur_str):
        val, unit = int(m.group(1)), m.group(2)
        total += val * {"h": 3600, "m": 60, "s": 1}[unit]
    return total or None


# ═══════════════════════════════════════════════════════════════════════════
#  VOD CACHE
# ═══════════════════════════════════════════════════════════════════════════
#
#  Flat list of VOD entries, one per video:
#    { id, platform, title, start_time, channel, duration, obsidian_index? }
#
#  Stored on disk as {"vods": [...]}.
#  Migrates transparently from legacy {"youtube": [...], "twitch": [...]}.

def load_cache(path: str | None = None) -> list[dict]:
    path = path or CACHE_PATH
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            raw = json.load(f)
    except (json.JSONDecodeError, IOError):
        return []

    # Current format
    if isinstance(raw, dict) and "vods" in raw:
        return raw["vods"]

    # Legacy: {"youtube": [...], "twitch": [...]} or nested variants
    if isinstance(raw, dict):
        vods: list[dict] = []
        for platform in ("youtube", "twitch"):
            entries = raw.get(platform, [])
            if isinstance(entries, dict):
                entries = entries.get("streams", entries.get("vods", []))
            if isinstance(entries, list):
                for e in entries:
                    if e.get("id"):
                        e.setdefault("platform", platform)
                        vods.append(e)
        return vods

    if isinstance(raw, list):
        return raw
    return []


def save_cache(cache: list[dict], path: str | None = None) -> None:
    path = path or CACHE_PATH
    with open(path, "w") as f:
        json.dump({"vods": cache}, f, indent=2)


def upsert_vod(cache: list[dict], vod: dict) -> dict:
    """Insert or update by (id, platform). None values are skipped."""
    vid, plat = vod["id"], vod["platform"]
    for existing in cache:
        if existing["id"] == vid and existing.get("platform") == plat:
            existing.update({k: v for k, v in vod.items() if v is not None})
            return existing
    cache.append(vod)
    return vod


def find_vod(cache: list[dict], video_id: str,
             platform: str | None = None) -> dict | None:
    for v in cache:
        if v["id"] == video_id:
            if platform is None or v.get("platform") == platform:
                return v
    return None


def find_vod_by_date(cache: list[dict], platform: str,
                     target_date: datetime.datetime,
                     window_hours: float = 1) -> dict | None:
    """Find the entry closest to target_date within window."""
    window = datetime.timedelta(hours=window_hours)
    best, best_delta = None, None
    for v in cache:
        if v.get("platform") != platform:
            continue
        st = v.get("start_time", "")
        if not st:
            continue
        try:
            vdt = (datetime.datetime.fromisoformat(st.replace("Z", "+00:00"))
                   .replace(tzinfo=None))
        except ValueError:
            continue
        delta = abs(vdt - target_date)
        if delta <= window and (best_delta is None or delta < best_delta):
            best, best_delta = v, delta
    return best


def refresh_youtube_cache(config: dict, cache: list[dict], *,
                          full: bool = False) -> bool:
    """Refresh YouTube VODs in cache via yt-dlp."""
    count = 10 if full else 5
    url = f"https://www.youtube.com/{config['youtube_handle']}/streams"
    entries = ytdlp_dump_playlist(config, url, f"1:{count}")
    if not entries:
        return False
    for data in entries:
        vid = data.get("id")
        if not vid:
            continue
        release_ts = data.get("release_timestamp")
        upload_date = data.get("upload_date", "")
        if release_ts:
            start_time = datetime.datetime.fromtimestamp(release_ts).isoformat()
        elif upload_date:
            start_time = datetime.datetime.strptime(upload_date, "%Y%m%d").isoformat()
        else:
            continue
        upsert_vod(cache, {
            "id":         vid,
            "platform":   "youtube",
            "title":      data.get("title", "Unknown"),
            "start_time": start_time,
            "channel":    data.get("channel") or config["youtube_handle"],
            "duration":   data.get("duration"),
        })
    return True


def refresh_twitch_cache(config: dict, cache: list[dict], *,
                         full: bool = False) -> bool:
    """Refresh Twitch VODs in cache via Helix API."""
    limit = 200 if full else 100
    vods = twitch_list_vods(config, limit=limit)
    if not vods:
        return False
    for v in vods:
        upsert_vod(cache, {
            "id":         v["id"],
            "platform":   "twitch",
            "title":      v["title"],
            "start_time": v["created_at"],
            "channel":    config.get("twitch_user", ""),
            "duration":   parse_twitch_duration(v.get("duration")),
        })
    return True


# ═══════════════════════════════════════════════════════════════════════════
#  OBSIDIAN HELPERS
# ═══════════════════════════════════════════════════════════════════════════
#
#  Read/write helpers for the Obsidian livestream log file. Used by the
#  recorder (creates new entries) and ls-audit (rebuilds them). All regex
#  surface area lives here so there's one place to touch.

def extract_video_id_from_url(url: str) -> tuple[str | None, str | None]:
    """Extract (video_id, platform) from a YouTube or Twitch URL."""
    if not url:
        return None, None
    m = re.search(r"(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})", url)
    if m:
        return m.group(1), "youtube"
    m = re.search(r"twitch\.tv/[^/]+/videos?/(\d+)", url)
    if m:
        return m.group(1), "twitch"
    return None, None


def build_stream_url(config: dict, platform: str, video_id: str) -> str:
    if platform == "youtube":
        return f"https://www.youtube.com/watch?v={video_id}"
    return f"https://www.twitch.tv/{config['twitch_user']}/video/{video_id}"


def build_shell_cmd(config: dict, filename: str) -> str:
    """Build an obsidian://shell-commands URI for opening a file in the vault."""
    encoded = urllib.parse.quote(filename, safe="")
    return (f"obsidian://shell-commands/?vault={config['obsidian_vault']}"
            f"&execute={config['shellcmd_id']}&_arg0=raws/{encoded}")


def obsidian_next_index(config: dict) -> int:
    """Get the next available index from the Obsidian log file."""
    path = config["obsidian"]
    if not os.path.exists(path):
        return 1
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        matches = re.findall(r"\*\*(\d{3})\*\*", content)
        return max(int(m) for m in matches) + 1 if matches else 1
    except Exception:
        return 1


def obsidian_create_entry(config: dict, index: int, platform: str,
                          title: str, url: str) -> bool:
    """Create a new entry prepended to the top of the obsidian file."""
    obs_path = config["obsidian"]
    if not os.path.exists(os.path.dirname(obs_path)):
        return False

    now = datetime.datetime.now()
    offset = now.astimezone().utcoffset()
    tz = f"GMT{int(offset.total_seconds() / 3600):+d}" if offset else "GMT+0"
    date_str = now.strftime(f"%Y.%m.%d %H:%M ({tz})")

    yt = f"[📁]() [📄]() [ {title} ]({url})" if platform == "youtube" else ""
    tw = f"[📁]() [📄]() [ {title} ]({url})" if platform == "twitch" else ""

    try:
        try:
            with open(obs_path, "r", encoding="utf-8") as f:
                content = f.read()
        except FileNotFoundError:
            content = ""

        entry = (
            f"- [ ] **{index:03d}** : {date_str}  #stream\n"
            f"\t`YT` {yt}\n"
            f"\t`TW` {tw}\n"
            f"\t- [ ] \n"
            f"---\n"
        )
        with open(obs_path, "w", encoding="utf-8") as f:
            f.write(entry + content)
        return True
    except Exception:
        return False


def obsidian_update_entry(config: dict, index: int, platform: str, *,
                          title: str | None = None,
                          url: str | None = None,
                          stream_title: str | None = None,
                          duration_seconds: float | None = None,
                          video_ext: str = ".mp4") -> bool:
    """Update platform line, file paths, or duration in an existing entry."""
    obs_path = config["obsidian"]
    if not os.path.exists(obs_path):
        return False
    try:
        tag = "YT" if platform == "youtube" else "TW"
        with open(obs_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Update title/url on platform line
        if title and url:
            pattern = rf"(\t`{tag}` )[^\n]*\n"
            replacement = f"\\1[📁]() [📄]() [ {title} ]({url})\n"
            content = re.sub(pattern, replacement, content, count=1)

        # Update file links (📁 video, 📄 chat)
        if stream_title:
            shell_base = (
                f"obsidian://shell-commands/?vault={config['obsidian_vault']}"
                f"&execute={config['shellcmd_id']}&_arg0=raws/"
            )
            encoded = urllib.parse.quote(stream_title, safe="")
            ext = video_ext if video_ext.startswith(".") else f".{video_ext or 'mp4'}"
            pattern = rf"(\t`{tag}` )\[📁\]\(\) \[📄\]\(\)"
            replacement = (
                f"\\1[📁]({shell_base}{encoded}{ext}) "
                f"[📄]({shell_base}{encoded}.json)"
            )
            content = re.sub(pattern, replacement, content, count=1)

        # Update duration (keep the longer value)
        if duration_seconds is not None:
            idx_str = f"{index:03d}"
            h, rem = divmod(int(duration_seconds), 3600)
            m, s = divmod(rem, 60)
            new_dur = f"[{h:02d}:{m:02d}:{s:02d}]"

            existing = re.search(
                rf"\*\*{idx_str}\*\*.*?\[(\d{{2}}):(\d{{2}}):(\d{{2}})\]", content,
            )
            if existing:
                existing_secs = (int(existing.group(1)) * 3600
                                 + int(existing.group(2)) * 60
                                 + int(existing.group(3)))
                if duration_seconds > existing_secs:
                    content = re.sub(
                        rf"(\*\*{idx_str}\*\*.*?)\[\d{{2}}:\d{{2}}:\d{{2}}\]",
                        rf"\1{new_dur}", content, count=1,
                    )
            else:
                content = re.sub(
                    rf"(\*\*{idx_str}\*\*.*?)\s+#stream",
                    rf"\1 {new_dur}  #stream", content, count=1,
                )

        with open(obs_path, "w", encoding="utf-8") as f:
            f.write(content)
        return True
    except Exception:
        return False


def obsidian_parse_entry(config: dict, index: int) -> dict:
    """Minimal parse of entry #index — extracts only what's worth preserving.

    Returns dict with: found, checkbox, date_str, date_obj, tz_str,
    duration_str, yt_id, tw_id, no_yt, no_tw, notes.
    """
    result: dict[str, Any] = {
        "found": False, "checkbox": "[ ]",
        "date_str": None, "date_obj": None, "tz_str": None,
        "duration_str": None,
        "yt_id": None, "tw_id": None,
        "no_yt": False, "no_tw": False,
        "notes": [],
    }
    obs_path = config["obsidian"]
    if not os.path.exists(obs_path):
        return result

    with open(obs_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    idx_str = f"{int(index):03d}"
    header_re = re.compile(rf"\*\*{idx_str}\*\*\s*:")
    start = None
    for i, line in enumerate(lines):
        if header_re.search(line):
            start = i
            break
    if start is None:
        return result

    result["found"] = True
    header = lines[start]

    cb = re.search(r"\[([ x])\]", header)
    if cb:
        result["checkbox"] = f"[{cb.group(1)}]"

    dm = re.search(r"(\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2})", header)
    if dm:
        result["date_str"] = dm.group(1)
        try:
            result["date_obj"] = datetime.datetime.strptime(
                dm.group(1), "%Y.%m.%d %H:%M",
            )
        except ValueError:
            pass

    tz = re.search(r"(\(GMT[^)]*\))", header)
    if tz:
        result["tz_str"] = tz.group(1)

    dur = re.search(r"\[(\d{2}:\d{2}:\d{2})\]", header)
    if dur:
        result["duration_str"] = dur.group(1)

    i = start + 1
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()
        if stripped == "---" or re.match(r"^-\s*\[.\]\s*\*\*\d+\*\*", line):
            break
        if re.match(r"^\t`YT`", line):
            if re.search(r"[✗✘]", line):
                result["no_yt"] = True
            else:
                for u in re.findall(r"\]\(([^)]+)\)", line):
                    vid, plat = extract_video_id_from_url(u)
                    if vid and plat == "youtube":
                        result["yt_id"] = vid
                        break
        elif re.match(r"^\t`TW`", line):
            if re.search(r"[✗✘]", line):
                result["no_tw"] = True
            else:
                for u in re.findall(r"\]\(([^)]+)\)", line):
                    vid, plat = extract_video_id_from_url(u)
                    if vid and plat == "twitch":
                        result["tw_id"] = vid
                        break
        else:
            result["notes"].append(line)
        i += 1

    return result


def obsidian_write_entry(config: dict, index: int,
                         new_lines: list[str]) -> bool:
    """Replace entry #index in the Obsidian file with new_lines."""
    obs_path = config["obsidian"]
    if not os.path.exists(obs_path):
        return False

    with open(obs_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    idx_str = f"{int(index):03d}"
    header_re = re.compile(rf"\*\*{idx_str}\*\*\s*:")
    start = None
    for i, line in enumerate(lines):
        if header_re.search(line):
            start = i
            break
    if start is None:
        return False

    end = start + 1
    while end < len(lines):
        stripped = lines[end].strip()
        if stripped == "---" or re.match(r"^-\s*\[.\]\s*\*\*\d+\*\*", lines[end]):
            break
        end += 1

    replacement = [(l if l.endswith("\n") else l + "\n") for l in new_lines]
    lines[start:end] = replacement

    with open(obs_path, "w", encoding="utf-8") as f:
        f.writelines(lines)
    return True


# ═══════════════════════════════════════════════════════════════════════════
#  TWITCH IRC CHAT RECORDER
# ═══════════════════════════════════════════════════════════════════════════
#
#  Connects to Twitch anonymous IRC, parses tagged messages, and writes a
#  JSON array to disk. Runs until stop_event is set or the connection drops.

def record_twitch_chat(channel: str, stream_start_ms: int, output_path: str,
                       stop_event, logger=None) -> None:
    """Block and record Twitch chat to a JSON file."""
    sock = socket.socket()
    sock.settimeout(5.0)

    try:
        sock.connect(("irc.chat.twitch.tv", 6667))
        sock.send(b"CAP REQ :twitch.tv/tags twitch.tv/commands\r\n")
        sock.send(f"NICK justinfan{int(time.time()) % 99999}\r\n".encode())
        sock.send(f"JOIN #{channel.lower()}\r\n".encode())
        if logger:
            logger.info(f"Connected to Twitch IRC for #{channel}")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("[\n")
            first = True
            buf = ""

            while not stop_event.is_set():
                try:
                    buf += sock.recv(4096).decode("utf-8", errors="replace")
                    while "\r\n" in buf:
                        line, buf = buf.split("\r\n", 1)
                        if line.startswith("PING"):
                            sock.send(b"PONG :tmi.twitch.tv\r\n")
                            continue
                        msg = _parse_irc_message(line, stream_start_ms)
                        if msg:
                            if not first:
                                f.write(",\n")
                            json.dump(msg, f, ensure_ascii=False)
                            first = False
                except socket.timeout:
                    continue
                except Exception as e:
                    if logger:
                        logger.error(f"IRC recv error: {e}")
                    break

            f.write("\n]")
        if logger:
            logger.info(f"Chat recording finished: {os.path.basename(output_path)}")
    except Exception as e:
        if logger:
            logger.error(f"IRC connection error: {e}")
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _parse_irc_message(line: str, stream_start_ms: int) -> dict | None:
    """Parse a single tagged IRC line into a chat message dict."""
    if not line.startswith("@"):
        return None

    match = re.match(
        r"@(?P<tags>[^ ]+) :(?P<user>[^!]+)![^ ]+ "
        r"(?P<cmd>\w+) #[^ ]+(?: :(?P<msg>.*))?",
        line,
    )
    if not match:
        return None

    # Parse key=value tags
    tags: dict[str, str] = {}
    for tag in match.group("tags").split(";"):
        if "=" in tag:
            k, v = tag.split("=", 1)
            tags[k] = v.replace("\\s", " ").replace("\\:", ";")

    cmd = match.group("cmd")
    username = match.group("user")
    message = match.group("msg") or ""
    ts = int(
        (int(tags.get("tmi-sent-ts", time.time() * 1000)) - stream_start_ms)
        * 1000
    )

    # Badges
    badges = []
    for b in tags.get("badges", "").split(","):
        if "/" in b:
            name, ver = b.split("/", 1)
            badges.append({
                "name": name, "version": ver,
                "title": name.replace("-", " ").title(),
            })

    # Emotes
    emotes = []
    msg_bytes = message.encode("utf-8")
    for e in tags.get("emotes", "").split("/"):
        if ":" not in e:
            continue
        eid, positions = e.split(":", 1)
        locs: list[str] = []
        ename = None
        for pos in positions.split(","):
            if "-" in pos:
                s, end = int(pos.split("-")[0]), int(pos.split("-")[1])
                locs.append(f"{s}-{end}")
                if not ename:
                    try:
                        ename = msg_bytes[s : end + 1].decode("utf-8")
                    except Exception:
                        ename = f"emote_{eid}"
        if ename:
            emotes.append({"id": eid, "name": ename, "locations": locs})

    author = {
        "id": tags.get("user-id", ""),
        "name": username,
        "display_name": tags.get("display-name", username),
        "badges": badges,
    }

    # ── PRIVMSG ───────────────────────────────────────────────────────
    if cmd == "PRIVMSG":
        msg = {
            "message_type": "text_message", "timestamp": ts,
            "message_id": tags.get("id", ""), "author": author,
            "colour": tags.get("color", ""), "message": message,
            "emotes": emotes,
        }
        if tags.get("bits"):
            msg["bits"] = int(tags["bits"])
        return msg

    # ── USERNOTICE (subs, gifts, raids) ───────────────────────────────
    if cmd == "USERNOTICE":
        msg_id = tags.get("msg-id", "")
        base = {
            "timestamp": ts, "message_id": tags.get("id", ""),
            "author": author, "colour": tags.get("color", ""),
            "message": message or None, "emotes": emotes,
        }
        if msg_id == "sub":
            return {**base, "message_type": "subscription",
                    "subscription_type": tags.get("msg-param-sub-plan", "1000")}
        if msg_id == "resub":
            return {**base, "message_type": "resubscription",
                    "subscription_type": tags.get("msg-param-sub-plan", "1000"),
                    "cumulative_months": int(tags.get("msg-param-cumulative-months", 1))}
        if msg_id == "submysterygift":
            return {**base, "message_type": "mystery_subscription_gift",
                    "subscription_type": tags.get("msg-param-sub-plan", "1000"),
                    "mass_gift_count": int(tags.get("msg-param-mass-gift-count", 1)),
                    "origin_id": tags.get("msg-param-origin-id", "")}
        if msg_id == "subgift":
            return {**base, "message_type": "subscription_gift",
                    "subscription_type": tags.get("msg-param-sub-plan", "1000"),
                    "gift_recipient_id": tags.get("msg-param-recipient-id", ""),
                    "gift_recipient_display_name": tags.get("msg-param-recipient-display-name", ""),
                    "origin_id": tags.get("msg-param-origin-id", "")}
        if msg_id == "raid":
            return {**base, "message_type": "raid",
                    "number_of_raiders": int(tags.get("msg-param-viewerCount", 0))}
        return None

    # ── CLEARCHAT / CLEARMSG ──────────────────────────────────────────
    if cmd == "CLEARCHAT" and message:
        return {
            "message_type": "ban_user", "timestamp": ts,
            "author": {"target_id": tags.get("target-user-id", ""), "name": message},
            "ban_duration": int(tags["ban-duration"]) if tags.get("ban-duration") else None,
        }
    if cmd == "CLEARMSG" and tags.get("target-msg-id"):
        return {
            "message_type": "delete_message", "timestamp": ts,
            "target_message_id": tags["target-msg-id"],
        }

    return None


# ═══════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

def extract_video_id_from_filename(filename: str) -> str | None:
    """Extract [video_id] from NAS filename like '516_title [ID] @ 2026-02-08_04-15.ext'"""
    m = re.search(r"\[([^\]]+)\]\s*@\s*\d{4}-\d{2}-\d{2}", filename)
    return m.group(1) if m else None


def classify_video_id(vid: str) -> str:
    """Guess platform from a raw video ID string."""
    return "twitch" if vid.lstrip("v").isdigit() else "youtube"


def probe_duration(filepath: str) -> float | None:
    """Get media duration in seconds via ffprobe."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", filepath],
            capture_output=True, text=True,
        )
        if r.returncode == 0 and r.stdout.strip():
            return float(r.stdout.strip())
    except Exception:
        pass
    return None


def merge_chat_fragments(output_dir: str, stream_title: str) -> bool:
    """Merge yt-dlp live_chat fragment files into a single .json file."""
    base = f"{stream_title}.live_chat.json"
    main_part = os.path.join(output_dir, f"{base}.part")
    frag_pattern = os.path.join(output_dir, f"{base}.part-Frag*.part")
    final_output = os.path.join(output_dir, f"{stream_title}.json")

    try:
        all_lines: list[str] = []
        if os.path.exists(main_part):
            with open(main_part, "r", encoding="utf-8") as f:
                all_lines.extend(f.readlines())
        for frag in sorted(glob.glob(frag_pattern)):
            with open(frag, "r", encoding="utf-8") as f:
                all_lines.extend(f.readlines())

        if not all_lines:
            return False

        with open(final_output, "w", encoding="utf-8") as f:
            f.writelines(all_lines)

        # Cleanup fragments
        if os.path.exists(main_part):
            os.remove(main_part)
        for frag in sorted(glob.glob(frag_pattern)):
            os.remove(frag)
        return True
    except Exception:
        return False
