"""
ls_common - Shared infrastructure for the ls-rec toolset.

Consolidates:
  * Config loading (config.json is the single source of truth)
  * yt-dlp invocation (one wrapper, three modes: probe / live / post-hoc)
  * Twitch Helix API client
  * Stream cache (keyed by internal index, with per-platform pools for discovery)
  * Obsidian entry read/write helpers

All scripts in this repo (livestream-recorder, ls-download, ls-audit) should
import from here rather than rolling their own yt-dlp command lines or
cache logic.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import subprocess
import urllib.parse
import urllib.request
from typing import Any, Optional


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
CACHE_PATH = os.path.join(SCRIPT_DIR, ".stream_cache.json")


# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════════════════

# Defaults for runtime tunables. Paths / handles / API keys must come from
# config.json — there are no sensible defaults for those.
_DEFAULTS: dict[str, Any] = {
    "priority":           "youtube",
    "check_interval":     60,
    "cleanup_hour":       3,
    "cooldown_duration":  30,
    "dual_stream_cycle":  10,
    "cookies_browser":    "firefox",
}

# Keys that must be present in config.json for the tools to function.
_REQUIRED_KEYS = (
    "obsidian",
    "obsidian_vault",
    "shellcmd_id",
    "nas_path",
    "output",
    "youtube_handle",
    "twitch_user",
)


def load_config(path: str = CONFIG_PATH) -> dict[str, Any]:
    """Load config.json, apply defaults, validate required keys."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"config.json not found at {path}. "
            f"Copy config.example.json and fill in the values."
        )
    with open(path) as f:
        raw = json.load(f)

    cfg = dict(_DEFAULTS)
    cfg.update(raw)

    missing = [k for k in _REQUIRED_KEYS if k not in cfg]
    if missing:
        raise ValueError(f"config.json missing required keys: {missing}")

    return cfg


# ══════════════════════════════════════════════════════════════════════════════
#  YT-DLP WRAPPER
# ══════════════════════════════════════════════════════════════════════════════
#
#  One class, three modes:
#    * probe(...)            — dump-json, short timeout, used by pings
#    * build_live_cmd(...)   — long-running live recording command
#    * build_vod_cmd(...)    — post-hoc VOD download
#    * build_chat_cmd(...)   — chat via yt-dlp live_chat subs
#
#  The binary path, cookie args, and format strings all live here so that
#  callers never construct their own yt-dlp argv.

class YtDlp:
    def __init__(self, config: dict[str, Any]):
        venv = config.get("venv")
        self.bin = os.path.join(venv, "bin", "yt-dlp") if venv else "yt-dlp"
        self.cookies = ["--cookies-from-browser", config.get("cookies_browser", "firefox")]

    # ── base ──────────────────────────────────────────────────────────────────

    def _base(self) -> list[str]:
        return [self.bin, *self.cookies]

    # ── probing / metadata ────────────────────────────────────────────────────

    def probe(
        self,
        url: str,
        *,
        playlist_items: Optional[str] = None,
        timeout: int = 30,
    ) -> Optional[dict]:
        """Return parsed JSON metadata for the first entry at `url`, or None."""
        cmd = self._base() + ["--dump-json"]
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
            first = r.stdout.strip().split("\n", 1)[0]
            return json.loads(first)
        except json.JSONDecodeError:
            return None

    def dump_playlist(
        self,
        url: str,
        playlist_items: str,
        *,
        timeout: int = 300,
    ) -> list[dict]:
        """Return parsed JSON entries for a playlist range (e.g. '1:10')."""
        cmd = self._base() + ["--dump-json", "--playlist-items", playlist_items, url]
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return []
        if r.returncode != 0 and not r.stdout.strip():
            return []
        out = []
        for line in r.stdout.strip().split("\n"):
            if not line.strip():
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out

    # ── download command builders ─────────────────────────────────────────────

    def build_live_cmd(self, url: str, platform: str, output_template: str) -> list[str]:
        """Long-running live recording command.

        YouTube: plain "best" — constraining to mp4/avc1 caused yt-dlp to drop
        long streams after ~4h. The container may end up webm/mkv.
        Twitch: mp4/avc1 selection is fine since HLS VODs segment cleanly.
        """
        fmt = (
            "best"
            if platform == "youtube"
            else "bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best[ext=mp4]/best"
        )
        cmd = self._base() + [
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

    def build_vod_cmd(self, url: str, output_template: str) -> list[str]:
        """Post-hoc VOD download (used by ls-download)."""
        cmd = self._base() + [
            "--format", "bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best[ext=mp4]/best",
            "-o", output_template,
            "--no-part",
            "--no-mtime",
            "--concurrent-fragments", "16",
        ]
        if "twitch.tv" in url:
            cmd += ["--remux-video", "mp4"]
        cmd.append(url)
        return cmd

    def build_chat_cmd(self, url: str, output_template: str) -> list[str]:
        """Chat via yt-dlp live_chat subs (YouTube live / VOD)."""
        return self._base() + [
            "--skip-download",
            "--write-subs",
            "--sub-langs", "live_chat",
            "-o", output_template,
            url,
        ]


# ══════════════════════════════════════════════════════════════════════════════
#  TWITCH HELIX API
# ══════════════════════════════════════════════════════════════════════════════

class TwitchApi:
    def __init__(self, config: dict[str, Any]):
        self.client_id = config.get("twitch_client_id")
        self.client_secret = config.get("twitch_client_secret")
        self.user_id = config.get("twitch_user_id")
        self.user = config.get("twitch_user")
        self._token: Optional[str] = None

    def token(self) -> Optional[str]:
        if self._token:
            return self._token
        if not (self.client_id and self.client_secret):
            return None
        data = urllib.parse.urlencode({
            "client_id":     self.client_id,
            "client_secret": self.client_secret,
            "grant_type":    "client_credentials",
        }).encode()
        req = urllib.request.Request(
            "https://id.twitch.tv/oauth2/token", data=data, method="POST"
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                self._token = json.loads(resp.read())["access_token"]
                return self._token
        except Exception as e:
            print(f"  ⚠ Twitch auth failed: {e}")
            return None

    def list_vods(self, limit: int = 100) -> list[dict]:
        tok = self.token()
        if not tok or not self.user_id:
            return []
        headers = {"Client-ID": self.client_id, "Authorization": f"Bearer {tok}"}
        out: list[dict] = []
        cursor = None
        fetched = 0
        while fetched < limit:
            batch = min(100, limit - fetched)
            url = (
                f"https://api.twitch.tv/helix/videos"
                f"?user_id={self.user_id}&type=archive&first={batch}"
            )
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
            out.extend(videos)
            fetched += len(videos)
            cursor = data.get("pagination", {}).get("cursor")
            if not cursor:
                break
        return out


def parse_twitch_duration(dur_str: Optional[str]) -> Optional[int]:
    """Parse '3h24m18s' → seconds."""
    if not dur_str:
        return None
    total = 0
    for m in re.finditer(r"(\d+)([hms])", dur_str):
        val, unit = int(m.group(1)), m.group(2)
        total += val * {"h": 3600, "m": 60, "s": 1}[unit]
    return total or None


# ══════════════════════════════════════════════════════════════════════════════
#  STREAM CACHE
# ══════════════════════════════════════════════════════════════════════════════
#
#  Schema v2:
#    {
#      "version": 2,
#      "entries": {
#        "<index>": {
#          "index": 557,
#          "yt_id": "...", "tw_id": "...",
#          "yt_starttime": "ISO", "tw_starttime": "ISO",
#          "yt_title": "...", "tw_title": "...",
#          "yt_duration": 1234, "tw_duration": 1234,
#          "injected": false
#        }
#      },
#      "yt_pool": [ ...recent fetched YT stream metadata... ],
#      "tw_pool": [ ...recent fetched TW VOD metadata... ]
#    }
#
#  - `entries` is authoritative per internal index. Live recorder, ls-download
#    and ls-audit all upsert into this.
#  - `yt_pool` / `tw_pool` are raw platform results used by ls-audit to resolve
#    a video ID from an entry date. Pools are fully replaced on refresh.
#
#  Migration: an older flat-list schema ({"youtube": [...], "twitch": [...]})
#  is rewritten on load — the flat data is moved into the pools and any entries
#  marked `injected` are kept there too (ls-audit will opportunistically link
#  them back to Obsidian indices on the next audit).

_PLATFORMS = ("youtube", "twitch")
_TAG = {"youtube": "yt", "twitch": "tw"}


class StreamCache:
    def __init__(self, path: str = CACHE_PATH):
        self.path = path
        self.data: dict[str, Any] = self._load()

    # ── persistence ───────────────────────────────────────────────────────────

    def _load(self) -> dict[str, Any]:
        if not os.path.exists(self.path):
            return self._empty()
        try:
            with open(self.path) as f:
                raw = json.load(f)
        except (json.JSONDecodeError, IOError):
            return self._empty()

        if raw.get("version") == 2:
            # Ensure all expected keys exist.
            out = self._empty()
            out["entries"] = raw.get("entries", {}) or {}
            out["yt_pool"] = raw.get("yt_pool", []) or []
            out["tw_pool"] = raw.get("tw_pool", []) or []
            return out

        # Legacy migration: flat {"youtube": [...], "twitch": [...]} → pools.
        out = self._empty()
        for plat in _PLATFORMS:
            data = raw.get(plat, [])
            if isinstance(data, dict):
                data = data.get("streams", data.get("vods", []))
            if isinstance(data, list):
                out[f"{_TAG[plat]}_pool"] = data
        return out

    @staticmethod
    def _empty() -> dict[str, Any]:
        return {"version": 2, "entries": {}, "yt_pool": [], "tw_pool": []}

    def save(self) -> None:
        with open(self.path, "w") as f:
            json.dump(self.data, f, indent=2)

    # ── entries ───────────────────────────────────────────────────────────────

    def get(self, index: int) -> Optional[dict]:
        return self.data["entries"].get(str(int(index)))

    def upsert(self, index: int, **fields: Any) -> dict:
        """Merge `fields` into entry #index. Creates the entry if absent."""
        key = str(int(index))
        entry = self.data["entries"].get(key) or {"index": int(index), "injected": False}
        for k, v in fields.items():
            if v is None:
                continue
            entry[k] = v
        self.data["entries"][key] = entry
        return entry

    def find_entry_by_video_id(self, platform: str, video_id: str) -> Optional[dict]:
        tag = _TAG[platform]
        for entry in self.data["entries"].values():
            if entry.get(f"{tag}_id") == video_id:
                return entry
        return None

    # ── pool (refresh + discovery) ────────────────────────────────────────────

    def _pool(self, platform: str) -> list[dict]:
        return self.data[f"{_TAG[platform]}_pool"]

    def newest_pool_date(self, platform: str) -> Optional[str]:
        dates = []
        for s in self._pool(platform):
            st = (s.get("start_time") or "")[:10]
            if st:
                dates.append(st)
            ud = s.get("upload_date") or ""
            if ud:
                dates.append(f"{ud[:4]}-{ud[4:6]}-{ud[6:8]}")
        return max(dates) if dates else None

    def find_in_pool(
        self,
        platform: str,
        target_date: datetime.datetime,
        *,
        window: datetime.timedelta = datetime.timedelta(hours=1),
    ) -> Optional[dict]:
        """Find the pool entry whose start_time is closest to target_date within window."""
        best, best_delta = None, None
        for s in self._pool(platform):
            st = s.get("start_time") or ""
            if not st:
                continue
            try:
                sdt = datetime.datetime.fromisoformat(st.replace("Z", "+00:00")).replace(tzinfo=None)
            except ValueError:
                continue
            delta = abs(sdt - target_date)
            if delta <= window and (best_delta is None or delta < best_delta):
                best, best_delta = s, delta
        if best and best_delta and best_delta > datetime.timedelta(minutes=5):
            best = dict(best)
            best["_fuzzy"] = f"~{int(best_delta.total_seconds() // 60)}min off"
        return best

    # ── refresh ───────────────────────────────────────────────────────────────

    def refresh_youtube(
        self,
        ytdlp: YtDlp,
        config: dict[str, Any],
        *,
        full: bool = False,
    ) -> bool:
        count = 100 if full else 10
        print(f"  ⌛ Refreshing YouTube pool ({'full' if full else 'incremental'}, last {count})...")
        entries = ytdlp.dump_playlist(
            f"https://www.youtube.com/{config['youtube_handle']}/streams",
            f"1:{count}",
        )
        if not entries:
            print("  ⚠ No streams parsed")
            return False

        parsed: list[dict] = []
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
            parsed.append({
                "id":          vid,
                "platform":    "youtube",
                "title":       data.get("title", "Unknown"),
                "start_time":  start_time,
                "upload_date": upload_date,
                "duration":    data.get("duration"),
                "channel":     data.get("channel") or config["youtube_handle"],
            })

        # Merge by ID — fresh data wins, previously-seen entries are retained.
        by_id = {s["id"]: s for s in self._pool("youtube")}
        for s in parsed:
            by_id[s["id"]] = s
        self.data["yt_pool"] = sorted(
            by_id.values(), key=lambda s: s.get("start_time", ""), reverse=True
        )
        self.save()
        print(f"  ✔ YouTube pool: {len(self.data['yt_pool'])} streams")
        return True

    def refresh_twitch(
        self,
        twitch: TwitchApi,
        *,
        full: bool = False,
    ) -> bool:
        limit = 200 if full else 100
        print(f"  ⌛ Refreshing Twitch pool ({'full' if full else 'incremental'}, last {limit})...")
        vods = twitch.list_vods(limit=limit)
        if not vods:
            print("  ⚠ No VODs returned")
            return False

        parsed: list[dict] = []
        for v in vods:
            parsed.append({
                "id":         v["id"],
                "platform":   "twitch",
                "title":      v["title"],
                "start_time": v["created_at"],
                "duration":   parse_twitch_duration(v.get("duration")),
                "channel":    twitch.user or "",
            })

        by_id = {s["id"]: s for s in self._pool("twitch")}
        for s in parsed:
            by_id[s["id"]] = s
        self.data["tw_pool"] = sorted(
            by_id.values(), key=lambda s: s.get("start_time", ""), reverse=True
        )
        self.save()
        print(f"  ✔ Twitch pool: {len(self.data['tw_pool'])} VODs")
        return True


# ══════════════════════════════════════════════════════════════════════════════
#  OBSIDIAN
# ══════════════════════════════════════════════════════════════════════════════
#
#  Shared read/write helpers for the Obsidian livestream log. Used by both
#  the live recorder (which creates new entries) and ls-audit (which rebuilds
#  them). Keep the regex surface area here so there's one place to touch.

def extract_video_id_from_url(url: str) -> tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None
    m = re.search(r"(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})", url)
    if m:
        return m.group(1), "youtube"
    m = re.search(r"twitch\.tv/[^/]+/videos?/(\d+)", url)
    if m:
        return m.group(1), "twitch"
    return None, None


def build_stream_url(config: dict[str, Any], platform: str, video_id: str) -> str:
    if platform == "youtube":
        return f"https://www.youtube.com/watch?v={video_id}"
    return f"https://www.twitch.tv/{config['twitch_user']}/video/{video_id}"


def build_shell_cmd(config: dict[str, Any], filename: str) -> str:
    encoded = urllib.parse.quote(filename, safe="")
    return (
        f"obsidian://shell-commands/?vault={config['obsidian_vault']}"
        f"&execute={config['shellcmd_id']}&_arg0=raws/{encoded}"
    )


class Obsidian:
    def __init__(self, config: dict[str, Any]):
        self.config = config
        self.path = config["obsidian"]

    # ── read ──────────────────────────────────────────────────────────────────

    def parse_entry(self, index: int) -> dict:
        """Minimal parse of entry #index — used by ls-audit.

        Returns: found, checkbox, date_str, date_obj, tz_str, duration_str,
        yt_id, tw_id, no_yt, no_tw, notes.

        Only the fields worth preserving across a rebuild are extracted;
        titles, URLs and file links are discarded and regenerated from IDs.
        """
        result: dict[str, Any] = {
            "found": False,
            "checkbox": "[ ]",
            "date_str": None, "date_obj": None, "tz_str": None,
            "duration_str": None,
            "yt_id": None, "tw_id": None,
            "no_yt": False, "no_tw": False,
            "notes": [],
        }

        if not os.path.exists(self.path):
            return result

        with open(self.path, "r", encoding="utf-8") as f:
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
                result["date_obj"] = datetime.datetime.strptime(dm.group(1), "%Y.%m.%d %H:%M")
            except ValueError:
                pass

        tz = re.search(r"(\(GMT[^)]*\))", header)
        if tz:
            result["tz_str"] = tz.group(1)

        dur_m = re.search(r"\[(\d{2}:\d{2}:\d{2})\]", header)
        if dur_m:
            result["duration_str"] = dur_m.group(1)

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
                    for url in re.findall(r"\]\(([^)]+)\)", line):
                        vid, plat = extract_video_id_from_url(url)
                        if vid and plat == "youtube":
                            result["yt_id"] = vid
                            break
            elif re.match(r"^\t`TW`", line):
                if re.search(r"[✗✘]", line):
                    result["no_tw"] = True
                else:
                    for url in re.findall(r"\]\(([^)]+)\)", line):
                        vid, plat = extract_video_id_from_url(url)
                        if vid and plat == "twitch":
                            result["tw_id"] = vid
                            break
            else:
                result["notes"].append(line)
            i += 1

        return result

    def write_entry(self, index: int, new_lines: list[str]) -> bool:
        """Replace entry #index with `new_lines` (a list of raw lines, no trailing \\n required)."""
        if not os.path.exists(self.path):
            return False

        with open(self.path, "r", encoding="utf-8") as f:
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

        with open(self.path, "w", encoding="utf-8") as f:
            f.writelines(lines)
        return True

    def next_index(self) -> int:
        if not os.path.exists(self.path):
            return 1
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                content = f.read()
            matches = re.findall(r"\*\*(\d{3})\*\*", content)
            return max(int(m) for m in matches) + 1 if matches else 1
        except Exception:
            return 1

    # ── write (live recorder) ─────────────────────────────────────────────────

    def create_entry(self, index: int, platform: str, title: str, url: str) -> bool:
        if not os.path.exists(os.path.dirname(self.path)):
            return False

        now = datetime.datetime.now()
        utc_offset = now.astimezone().utcoffset()
        hours_offset = int(utc_offset.total_seconds() / 3600) if utc_offset else 0
        tz_str = f"GMT{hours_offset:+d}"
        today = now.strftime(f"%Y.%m.%d %H:%M ({tz_str})")

        yt_line = f"[📁]() [📄]() [ {title} ]({url})" if platform == "youtube" else ""
        tw_line = f"[📁]() [📄]() [ {title} ]({url})" if platform == "twitch" else ""

        try:
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    content = f.read()
            except FileNotFoundError:
                content = ""

            entry = (
                f"- [ ] **{index:03d}** : {today}  #stream\n"
                f"\t`YT` {yt_line}\n"
                f"\t`TW` {tw_line}\n"
                f"\t- [ ] \n"
                f"---\n"
            )
            with open(self.path, "w", encoding="utf-8") as f:
                f.write(entry + content)
            return True
        except Exception:
            return False

    def update_entry(
        self,
        index: int,
        platform: str,
        *,
        title: Optional[str] = None,
        url: Optional[str] = None,
        stream_title: Optional[str] = None,
        duration_seconds: Optional[float] = None,
        video_ext: str = ".mp4",
    ) -> bool:
        if not os.path.exists(self.path):
            return False

        try:
            tag = "YT" if platform == "youtube" else "TW"
            with open(self.path, "r", encoding="utf-8") as f:
                content = f.read()

            if title and url:
                pattern = rf"(\t`{tag}` )[^\n]*\n"
                replacement = f"\\1[📁]() [📄]() [ {title} ]({url})\n"
                content = re.sub(pattern, replacement, content, count=1)

            if stream_title:
                shell_base = (
                    f"obsidian://shell-commands/?vault={self.config['obsidian_vault']}"
                    f"&execute={self.config['shellcmd_id']}&_arg0=raws/"
                )
                encoded = urllib.parse.quote(stream_title, safe="")
                ext = video_ext if video_ext and video_ext.startswith(".") else f".{video_ext or 'mp4'}"
                pattern = rf"(\t`{tag}` )\[📁\]\(\) \[📄\]\(\)"
                replacement = (
                    f"\\1[📁]({shell_base}{encoded}{ext}) "
                    f"[📄]({shell_base}{encoded}.json)"
                )
                content = re.sub(pattern, replacement, content, count=1)

            if duration_seconds is not None:
                idx_str = str(index).zfill(3)
                h, rem = divmod(int(duration_seconds), 3600)
                m, s = divmod(rem, 60)
                new_dur = f"[{h:02d}:{m:02d}:{s:02d}]"
                existing = re.search(
                    rf"\*\*{idx_str}\*\*.*?\[(\d{{2}}):(\d{{2}}):(\d{{2}})\]",
                    content,
                )
                if existing:
                    existing_secs = (
                        int(existing.group(1)) * 3600
                        + int(existing.group(2)) * 60
                        + int(existing.group(3))
                    )
                    if duration_seconds > existing_secs:
                        content = re.sub(
                            rf"(\*\*{idx_str}\*\*.*?)\[\d{{2}}:\d{{2}}:\d{{2}}\]",
                            rf"\1{new_dur}",
                            content,
                            count=1,
                        )
                else:
                    content = re.sub(
                        rf"(\*\*{idx_str}\*\*.*?)\s+#stream",
                        rf"\1 {new_dur}  #stream",
                        content,
                        count=1,
                    )

            with open(self.path, "w", encoding="utf-8") as f:
                f.write(content)
            return True
        except Exception:
            return False
