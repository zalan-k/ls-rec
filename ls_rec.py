#!/usr/bin/env python3
"""
ls-rec - Livestream recorder daemon and CLI.

Usage:
    ls-rec                       Start daemon (monitor + record)
    ls-rec run                   Same as above

    ls-rec status                Compact summary + per-stream health line
    ls-rec tail [YT|TW]          Live-tail current recording's log (Ctrl+C to exit)
    ls-rec check [youtube|twitch]    Force-probe for live streams
    ls-rec record <url>          Record live stream / watch if scheduled
    ls-rec watch <url>           Add URL to watch list
    ls-rec unwatch [url|N]       Remove from watch list

    ls-rec mando <url> [--index N] [--type video|chat|both]
                                 Download VOD directly to NAS

YouTube recording uses yt-dlp's --live-from-start, pulling from the
broadcast start via DVR. One process per stream, no rotation. A watchdog
thread samples file size every 10s and restarts yt-dlp if it stalls.
"""

import os, re, glob, time, logging, subprocess, datetime, sys, signal, threading, socket, argparse, ls_common
from collections import deque
from pathlib import Path
from yt_dlp.utils import sanitize_filename

SOCKET_PATH = "/tmp/livestream-recorder.sock"

# ── Watchdog / sampling constants ─────────────────────────────────────────
SAMPLE_INTERVAL_S    = 10     # file-size sample period
SAMPLE_WINDOW        = 12     # ~2 min of samples kept per stream
WATCHDOG_STALL_S     = 300    # kill yt-dlp if file hasn't grown this long
STALL_DISPLAY_S      = 30     # status shows STALLED after this long
BITRATE_PROBE_MIN_MB = 30     # ffprobe once file reaches this size
RESTART_MAX          = 10     # bounded restart attempts per stream
RESTART_DELAY_S      = 15     # backoff between restart attempts


# ═══════════════════════════════════════════════════════════════════════════
#  LOGGING (daemon only — configured lazily so CLI commands stay clean)
# ═══════════════════════════════════════════════════════════════════════════

logger = logging.getLogger("ls-rec")
def _setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("livestream_recorder.log", encoding="utf-8"),
            logging.StreamHandler(
                stream=open(
                    sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1,
                )
            ),
        ],
    )

# ═══════════════════════════════════════════════════════════════════════════
#  COMMAND SERVER  (unix socket, runs inside daemon)
# ═══════════════════════════════════════════════════════════════════════════

class CommandServer:
    def __init__(self, recorder):
        self.recorder = recorder
        self.running = False
        self.server_socket = None

    def start(self):
        if os.path.exists(SOCKET_PATH):
            os.unlink(SOCKET_PATH)
        self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server_socket.bind(SOCKET_PATH)
        os.chmod(SOCKET_PATH, 0o666)
        self.server_socket.listen(1)
        self.server_socket.settimeout(1.0)
        self.running = True
        threading.Thread(target=self._serve, daemon=True).start()
        logger.info(f"  > Command server on {SOCKET_PATH}")

    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        if os.path.exists(SOCKET_PATH):
            os.unlink(SOCKET_PATH)

    def _serve(self):
        while self.running:
            try:
                conn, _ = self.server_socket.accept()
                try:
                    data = conn.recv(4096).decode("utf-8").strip()
                    if data:
                        response = self.recorder.handle_command(data)
                        conn.sendall(response.encode("utf-8"))
                finally:
                    conn.close()
            except socket.timeout:
                continue
            except OSError:
                break


# ═══════════════════════════════════════════════════════════════════════════
#  SOCKET CLIENT  (CLI side — sends command, prints response, exits)
# ═══════════════════════════════════════════════════════════════════════════

def _connect_socket(timeout: int = 35) -> socket.socket:
    if not os.path.exists(SOCKET_PATH):
        print("ERROR: ls-rec daemon is not running.")
        print("  Start with: ls-rec run")
        sys.exit(1)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(SOCKET_PATH)
    except ConnectionRefusedError:
        print("ERROR: Could not connect. Daemon may have crashed.")
        sys.exit(1)
    return sock


def send_command(command: str) -> str:
    """Send a command, return the response string."""
    sock = _connect_socket()
    try:
        sock.sendall(command.encode("utf-8"))
        chunks = []
        while True:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                chunks.append(data)
            except socket.timeout:
                break
        return b"".join(chunks).decode("utf-8")
    except socket.timeout:
        print("ERROR: Command timed out.")
        sys.exit(1)
    finally:
        sock.close()


def send_command_and_print(command: str):
    print(send_command(command))


def do_tail(target: str | None):
    """Ask daemon for log path, then exec tail -F on it.

    Ctrl+C in tail terminates only this CLI process; the daemon and
    recording are untouched. tail -F follows the file by name and handles
    re-creation, so a yt-dlp restart mid-tail just keeps working.
    """
    cmd = "tail " + (target or "")
    response = send_command(cmd.strip()).strip()
    if response.startswith("PATH:"):
        log_path = response[5:].strip()
        if not os.path.exists(log_path):
            print(f"Log file not found yet: {log_path}")
            sys.exit(1)
        try:
            os.execvp("tail", ["tail", "-F", "-n", "100", log_path])
        except FileNotFoundError:
            print("'tail' not found on PATH.")
            sys.exit(1)
    else:
        print(response or "No response.")
        sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════
#  RECORDER DAEMON
# ═══════════════════════════════════════════════════════════════════════════

class LivestreamRecorder:
    def __init__(self):
        self.config = ls_common.load_config()
        self.active_streams: dict[str, dict] = {}
        self.watch_list: dict[str, dict] = {}       # ephemeral

        # State
        self.was_streaming = False
        self.monitoring_cooldown_until = None
        self.manual_termination_in_progress = False
        self.last_check_time: dict[str, datetime.datetime | None] = {
            "youtube": None, "twitch": None,
        }

        # Filesystem
        Path(self.config["output"]).mkdir(parents=True, exist_ok=True)
        self._log_disk_space()

        # Signals
        self._orig_sigint = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self._handle_sigint)

        # Socket server
        self.command_server = CommandServer(self)

        # Background sampler/watchdog
        self._monitor_stop = threading.Event()
        threading.Thread(target=self._monitor_loop, daemon=True).start()

    # ── SIGINT ────────────────────────────────────────────────────────────

    def _handle_sigint(self, sig, frame):
        if self.manual_termination_in_progress:
            print("\nForce terminating...")
            signal.signal(signal.SIGINT, self._orig_sigint)
            os.kill(os.getpid(), signal.SIGINT)
            return

        if self.active_streams:
            print("\nCtrl+C — terminating yt-dlp gracefully...")
            self.manual_termination_in_progress = True
            for s in self.active_streams.values():
                # SIGTERM to video — yt-dlp will write its resume file and exit
                vp = s.get("video_process")
                if vp and vp.poll() is None:
                    try:
                        vp.terminate()
                    except Exception:
                        pass
                # Stop chat
                if s.get("chat_stop_event"):
                    s["chat_stop_event"].set()
            self.monitoring_cooldown_until = (
                datetime.datetime.now()
                + datetime.timedelta(seconds=self.config["cooldown_duration"])
            )
            print("Press Ctrl+C again to force exit.")
            return

        print("\nShutting down...")
        signal.signal(signal.SIGINT, self._orig_sigint)
        os.kill(os.getpid(), signal.SIGINT)

    def _is_monitoring_allowed(self) -> bool:
        if self.monitoring_cooldown_until is None:
            return True
        if datetime.datetime.now() >= self.monitoring_cooldown_until:
            self.monitoring_cooldown_until = None
            logger.info("Cooldown ended. Resuming monitoring.")
            return True
        return False

    def _mark_termination_finished_if_idle(self):
        """If we were in manual termination and no streams remain, clear the flag."""
        if not self.manual_termination_in_progress:
            return
        active = [
            s for s in self.active_streams.values()
            if s.get("video_process") and s["video_process"].poll() is None
        ]
        if not active:
            print("All streams finished. Cooldown active.")
            self.manual_termination_in_progress = False

    # ── command dispatch ──────────────────────────────────────────────────

    def handle_command(self, command: str) -> str:
        parts = command.split()
        if not parts:
            return ""
        cmd = parts[0].lower()
        if cmd == "status":
            return self._cmd_status()
        if cmd == "tail":
            return self._cmd_tail(parts[1] if len(parts) > 1 else None)
        if cmd == "check":
            p = parts[1] if len(parts) > 1 and parts[1] in ("youtube", "twitch") else None
            return self._cmd_check(p)
        if cmd == "record":
            return self._cmd_record(parts[1] if len(parts) > 1 else None)
        if cmd == "watch":
            return self._cmd_watch(parts[1] if len(parts) > 1 else None)
        if cmd == "unwatch":
            return self._cmd_unwatch(parts[1] if len(parts) > 1 else None)
        return ("Commands: status | tail [YT|TW] | check [youtube|twitch] | "
                "record <url> | watch <url> | unwatch [url|N]")

    # ── status ────────────────────────────────────────────────────────────

    def _stream_health(self, stream: dict) -> str:
        """Compute the right-hand-side health string: speed | rate | size."""
        samples = stream.get("_samples")
        if not samples:
            return "starting..."

        now = time.time()
        last_size = samples[-1][1]

        # Stall indicator wins over everything else
        last_growth = stream.get("_last_growth_ts", now)
        stalled_for = now - last_growth
        if stalled_for >= STALL_DISPLAY_S:
            return f"STALLED {int(stalled_for)}s"

        # Size
        size_mb = last_size / (1024 * 1024)
        size_str = (f"{size_mb:.0f}MB" if size_mb < 1024
                    else f"{size_mb / 1024:.2f}GB")

        # Rate over last ~60s
        rate_str = "—"
        speed_str = "—"
        cutoff = now - 60
        recent = [s for s in samples if s[0] >= cutoff]
        if len(recent) >= 2:
            d_bytes = recent[-1][1] - recent[0][1]
            d_time = recent[-1][0] - recent[0][0]
            if d_time > 0:
                bps = d_bytes / d_time
                mbpm = bps * 60 / (1024 * 1024)
                rate_str = f"{mbpm:.0f}MB/min"
                bitrate = stream.get("_bitrate_bps")
                if bitrate:
                    speed = (bps * 8) / bitrate
                    speed_str = f"{speed:.1f}x"

        return f"{speed_str:>5}  {rate_str:>10}  {size_str:>8}"

    def _stream_status_line(self, stream: dict) -> str:
        plat = "YT" if stream["platform"] == "youtube" else "TW"
        idx = stream.get("obsidian_index", 0)
        title = stream.get("obsidian_title", "Unknown")
        if len(title) > 32:
            title = title[:29] + "..."
        elapsed = str(
            datetime.datetime.now() - stream["start_time"]
        ).split(".")[0]
        health = self._stream_health(stream)
        return f"  [{plat} {idx:03d}] {title:<32}  {elapsed:>8}   {health}"

    def _cmd_status(self) -> str:
        lines = []

        # ── Monitored ─────────────────────────────────────────────
        lines.append("")
        lines.append(" ─── Monitored ─────────────────────────────────────────────────────")
        t_w = 30
        defaults = [
            ("YT", self.config["youtube_handle"], f"{self.config['check_interval']}s"),
            ("TW", self.config["twitch_user"],    f"{self.config['check_interval']}s"),
        ]
        watched = []
        for url, info in self.watch_list.items():
            plat = "TW" if "twitch.tv" in url else "YT"
            title = info.get("title", "Unknown")
            if len(title) > t_w - 3:
                title = title[:t_w - 3] + "..."
            start_ts = info.get("start_time")
            if start_ts:
                until = start_ts - time.time()
                if until > 0:
                    h, m = divmod(int(until) // 60, 60)
                    eta = f"~{h}h{m:02d}m"
                else:
                    eta = "should be live"
            else:
                eta = "unknown"
            watched.append((plat, title, eta))

        lines.append(f"  {'Platform'} │ {'Title':<{t_w}} │ Interval")
        lines.append(f"  ─────────┼─{'─' * t_w}─┼─────────────────────")
        for plat, name, interval in defaults:
            lines.append(f"  {plat:<8} │ {name:<{t_w}} │ {interval}")
        if watched:
            lines.append(f"  ─────────┼─{'─' * t_w}─┼─────────────────────")
            for plat, title, eta in watched:
                lines.append(f"  {plat:<8} │ {title:<{t_w}} │ {eta}")

        # ── Recording ─────────────────────────────────────────────
        lines.append("")
        lines.append(" ─── Recording ─────────────────────────────────────────────────────")
        if self.active_streams:
            lines.append(f"           Title                              Elapsed   Speed         Rate      Size")
            lines.append("  " + "─" * 78)
            for stream in self.active_streams.values():
                lines.append(self._stream_status_line(stream))
        else:
            lines.append("  (none)")

        # ── Last Checked ──────────────────────────────────────────
        lines.append("")
        lines.append(" ─── Last Checked ──────────────────────────────────────────────────")
        for plat in ("youtube", "twitch"):
            t = self.last_check_time.get(plat)
            ts = t.strftime("%H:%M:%S") if t else "never"
            lines.append(f"  {plat.capitalize():<7} │ {ts}")

        lines.append("")
        return "\n".join(lines)

    def _cmd_tail(self, target: str | None) -> str:
        """Resolve the log path for an active stream. Returns PATH:<path> or error."""
        if not self.active_streams:
            return "No active recordings."

        if target is None or target == "":
            if len(self.active_streams) == 1:
                stream = next(iter(self.active_streams.values()))
            else:
                return ("Multiple streams active. Use 'ls-rec tail YT' "
                        "or 'ls-rec tail TW'.")
        else:
            target_upper = target.upper()
            if target_upper not in ("YT", "TW"):
                return f"Unknown target: {target}. Use YT or TW."
            platform = "youtube" if target_upper == "YT" else "twitch"
            candidates = [s for s in self.active_streams.values()
                          if s["platform"] == platform]
            if not candidates:
                return f"No active {target_upper} recording."
            if len(candidates) > 1:
                return f"Multiple {target_upper} recordings active (unexpected)."
            stream = candidates[0]

        log_path = os.path.join(
            self.config["output"], f"{stream['stream_title']}.ytdlp.log",
        )
        return f"PATH:{log_path}"

    def _cmd_check(self, platform: str | None) -> str:
        platforms = [platform] if platform else ["youtube", "twitch"]
        lines = []
        for plat in platforms:
            result = self._probe_platform(plat)
            if result:
                tag = "(recording)" if result["stream_key"] in self.active_streams else "(not recording)"
                lines.append(f"  ✔ {plat.upper()}: LIVE — {result['obsidian_title']} {tag}")
            else:
                lines.append(f"  ✗ {plat.upper()}: offline")
        return "\n".join(lines)

    def _cmd_record(self, target: str | None) -> str:
        if not target:
            return "Usage: record <youtube|twitch|url>"

        # Platform shorthand
        if target in ("youtube", "twitch"):
            result = self._probe_platform(target)
            if not result:
                return f"No live stream on {target}."
            if result["stream_key"] in self.active_streams:
                return f"Already recording: {result['obsidian_title']}"
            idx, dual = self._get_stream_index(target, datetime.datetime.now())
            self._start_recording(result, idx, dual)
            return f"✔ Recording {target.upper()}: {result['obsidian_title']} (#{idx:03d})"

        # Direct URL
        url = target
        data = ls_common.ytdlp_probe(self.config, url, playlist_items="1")
        if not data:
            return f"✗ Could not fetch: {url}"

        platform = "twitch" if "twitch.tv" in url else "youtube"
        title = data.get("fulltitle") or data.get("title") or "Unknown"
        video_id = data.get("id", "unknown")
        stream_key = f"{platform}_{video_id}"

        if stream_key in self.active_streams:
            return f"Already recording: {title}"

        if data.get("is_live", False):
            result = self._make_stream_info(platform, video_id, title, url)
            idx, dual = self._get_stream_index(platform, datetime.datetime.now())
            self._start_recording(result, idx, dual)
            return f"✔ LIVE — recording: {title} (#{idx:03d})"

        # Not live → add to watch list
        entry: dict = {"title": title, "last_check": time.time()}
        release_ts = data.get("release_timestamp")
        if release_ts:
            entry["start_time"] = release_ts
            until = release_ts - time.time()
            h, m = divmod(int(until) // 60, 60)
            self.watch_list[url] = entry
            return f"✔ Watching: {title} (starts in ~{h}h{m:02d}m)"
        self.watch_list[url] = entry
        return f"✔ Watching: {title}"

    def _cmd_watch(self, url: str | None) -> str:
        if not url:
            return "Usage: watch <url>"
        entry: dict = {"title": "Unknown", "last_check": time.time()}
        data = ls_common.ytdlp_probe(self.config, url, playlist_items="1")
        if data:
            entry["title"] = data.get("fulltitle") or data.get("title") or "Unknown"
            release_ts = data.get("release_timestamp")
            if release_ts:
                entry["start_time"] = release_ts
        self.watch_list[url] = entry
        return f"✔ Watching: {entry['title']}"

    def _cmd_unwatch(self, target: str | None) -> str:
        if not target:
            if not self.watch_list:
                return "Watch list is empty."
            lines = ["Watch list:"]
            for i, (url, info) in enumerate(self.watch_list.items(), 1):
                lines.append(f"  {i}) {info['title']}")
                lines.append(f"     {url}")
            return "\n".join(lines)
        if target in self.watch_list:
            removed = self.watch_list.pop(target)
            return f"✔ Removed: {removed['title']}"
        try:
            idx = int(target) - 1
            key = list(self.watch_list.keys())[idx]
            removed = self.watch_list.pop(key)
            return f"✔ Removed: {removed['title']}"
        except (ValueError, IndexError):
            return "✗ Not found in watch list."

    # ── probing ───────────────────────────────────────────────────────────

    def _probe_platform(self, platform: str) -> dict | None:
        """Probe configured channel for a live stream."""
        services = {
            "youtube": {
                "url": f"https://www.youtube.com/{self.config['youtube_handle']}/live",
                "playlist_items": "1",
            },
            "twitch": {
                "url": f"https://www.twitch.tv/{self.config['twitch_user']}",
                "playlist_items": None,
            },
        }
        svc = services[platform]
        data = ls_common.ytdlp_probe(
            self.config, svc["url"], playlist_items=svc["playlist_items"],
        )
        self.last_check_time[platform] = datetime.datetime.now()

        if not data or not data.get("is_live", False):
            return None

        video_id = data.get("id")
        if platform == "youtube":
            title = data.get("fulltitle")
            stream_url = f"https://www.youtube.com/watch?v={video_id}"
            obsidian_url = stream_url
        else:
            title = data.get("description")
            stream_url = svc["url"]
            obsidian_url = f"{svc['url']}/videos/{video_id.lstrip('v')}"

        return self._make_stream_info(
            platform, video_id, title, stream_url, obsidian_url,
        )

    def _make_stream_info(self, platform, video_id, title, stream_url,
                          obsidian_url=None):
        """Build the info dict consumed by _start_recording."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        stream_title = sanitize_filename(f"{title} [{video_id}] @ {timestamp}")
        if obsidian_url is None:
            obsidian_url = (f"https://www.youtube.com/watch?v={video_id}"
                            if platform == "youtube" else stream_url)
        return {
            "platform":       platform,
            "video_id":       video_id,
            "stream_url":     stream_url,
            "stream_title":   stream_title,
            "obsidian_title": title,
            "obsidian_url":   obsidian_url,
            "stream_key":     f"{platform}_{video_id}",
        }

    def _check_streams(self):
        if not self._is_monitoring_allowed():
            return
        for platform in ("youtube", "twitch"):
            result = self._probe_platform(platform)
            if result and result["stream_key"] not in self.active_streams:
                logger.info(f"Live: {result['stream_title']}")
                idx, dual = self._get_stream_index(
                    platform, datetime.datetime.now(),
                )
                if dual:
                    logger.info(f"Dual-stream, sharing index {idx:03d}")
                self._start_recording(result, idx, dual)

    def _probe_watchlist(self):
        """Check watched URLs; start recording when they go live."""
        now = time.time()
        for url in list(self.watch_list.keys()):
            entry = self.watch_list[url]

            # Adaptive polling interval
            start_ts = entry.get("start_time")
            if start_ts:
                until = start_ts - now
                interval = (3600 if until > 4 * 3600
                            else 300 if until > 900
                            else 60)
            else:
                interval = 120

            if now - entry.get("last_check", 0) < interval:
                continue

            entry["last_check"] = now
            data = ls_common.ytdlp_probe(self.config, url)
            if not data:
                continue

            if not data.get("is_live", False):
                release_ts = data.get("release_timestamp")
                if release_ts:
                    entry["start_time"] = release_ts
                continue

            # Stream went live
            platform = "twitch" if "twitch.tv" in url else "youtube"
            title = data.get("fulltitle") or data.get("title") or "Unknown"
            video_id = data.get("id", "unknown")
            if f"{platform}_{video_id}" in self.active_streams:
                continue

            result = self._make_stream_info(platform, video_id, title, url)
            idx, dual = self._get_stream_index(platform, datetime.datetime.now())
            logger.info(f"Watched stream live: {title}")
            self._start_recording(result, idx, dual)
            del self.watch_list[url]

    # ── recording ─────────────────────────────────────────────────────────

    def _get_stream_index(self, platform: str,
                          start_time: datetime.datetime) -> tuple[int, bool]:
        """Get obsidian index, detecting dual-stream to share an index."""
        window = self.config["dual_stream_cycle"] * self.config["check_interval"]
        other = "twitch" if platform == "youtube" else "youtube"
        for s in self.active_streams.values():
            if s["platform"] == other:
                diff = abs((start_time - s["start_time"]).total_seconds())
                if diff <= window:
                    return s["obsidian_index"], True
        return ls_common.obsidian_next_index(self.config), False

    def _start_recording(self, info: dict, obsidian_index: int, is_dual: bool):
        """Create obsidian + cache entries and spawn video + chat threads."""
        platform = info["platform"]
        video_id = info["video_id"]
        obsidian_title = info["obsidian_title"]
        obsidian_url = info["obsidian_url"]

        # Obsidian
        if is_dual:
            ls_common.obsidian_update_entry(
                self.config, obsidian_index, platform,
                title=obsidian_title, url=obsidian_url,
            )
        else:
            ls_common.obsidian_create_entry(
                self.config, obsidian_index, platform,
                obsidian_title, obsidian_url,
            )

        # Cache
        cache = ls_common.load_cache()
        channel = (self.config["youtube_handle"] if platform == "youtube"
                   else self.config["twitch_user"])
        ls_common.upsert_vod(cache, {
            "id":             video_id,
            "platform":       platform,
            "title":          obsidian_title,
            "start_time":     datetime.datetime.now().isoformat(),
            "channel":        channel,
            "obsidian_index": obsidian_index,
        })
        ls_common.save_cache(cache)

        stream_title = f"{obsidian_index:03d}_{info['stream_title']}"
        stream_key = f"{platform}_{video_id}"

        self.active_streams[stream_key] = {
            "url":             info["stream_url"],
            "platform":        platform,
            "identifier":      video_id,
            "stream_title":    stream_title,
            "obsidian_title":  obsidian_title,
            "start_time":      datetime.datetime.now(),
            "video_process":   None,
            "chat_thread":     None,
            "chat_stop_event": None,
            "obsidian_index":  obsidian_index,
            # ── health & watchdog state ──
            "_samples":            deque(maxlen=SAMPLE_WINDOW),
            "_last_size":          0,
            "_last_growth_ts":     time.time(),
            "_bitrate_bps":        None,
            "_watchdog_triggered": False,
            "_restart_count":      0,
        }
        self._record_video(stream_key)
        self._record_chat(stream_key)

    def _record_video(self, stream_key: str):
        """Spawn yt-dlp video process and a monitor thread that watches its exit.

        On crash (non-zero exit), restarts up to RESTART_MAX times — with
        --live-from-start, yt-dlp's resume file lets it pick up where it
        left off. Clean exit (rc=0) means stream ended and any DVR catchup
        is complete.
        """
        stream = self.active_streams.get(stream_key)
        if stream is None:
            return
        url      = stream["url"]
        platform = stream["platform"]
        title    = stream["stream_title"]

        try:
            cmd = ls_common.ytdlp_live_cmd(
                self.config, url, platform, f"{title}.%(ext)s",
            )
            log_path = os.path.join(self.config["output"], f"{title}.ytdlp.log")
            log_fh = open(log_path, "ab", buffering=0)
            process = subprocess.Popen(
                cmd, cwd=self.config["output"],
                stdout=log_fh, stderr=subprocess.STDOUT,
            )
            stream["video_process"] = process
            stream["_video_log_fh"] = log_fh
            stream["_last_growth_ts"] = time.time()    # reset watchdog
            stream["_watchdog_triggered"] = False
            logger.info(
                f"Video recording started: {title} (PID {process.pid}, "
                f"log: {os.path.basename(log_path)})"
            )

            def monitor():
                process.wait()
                rc = process.returncode
                try:
                    log_fh.close()
                except Exception:
                    pass

                # Manual termination wins regardless of exit code
                if self.manual_termination_in_progress:
                    logger.info(f"Video stopped (manual termination): {title}")
                    self._handle_completion(stream_key)
                    self._mark_termination_finished_if_idle()
                    return

                # Clean exit → done (for --live-from-start this means full catchup)
                if rc == 0:
                    logger.info(f"Video complete (rc=0): {title}")
                    self._handle_completion(stream_key)
                    return

                # Non-zero → restart if budget allows
                restart_count = stream.get("_restart_count", 0)
                if restart_count >= RESTART_MAX:
                    logger.error(
                        f"Video failed permanently after {RESTART_MAX} restarts "
                        f"(rc={rc}): {title}"
                    )
                    self._handle_completion(stream_key)
                    return

                stream["_restart_count"] = restart_count + 1
                logger.warning(
                    f"yt-dlp exited rc={rc}, restart "
                    f"{restart_count + 1}/{RESTART_MAX} in {RESTART_DELAY_S}s: {title}"
                )
                time.sleep(RESTART_DELAY_S)

                # Re-check world state after sleep
                if self.manual_termination_in_progress:
                    self._handle_completion(stream_key)
                    self._mark_termination_finished_if_idle()
                    return
                if stream_key not in self.active_streams:
                    return

                # --live-from-start resumes from the .frag.json file
                self._record_video(stream_key)

            threading.Thread(target=monitor, daemon=True).start()

        except Exception as e:
            logger.error(f"Video start error: {e}")
            self.active_streams.pop(stream_key, None)

    def _record_chat(self, stream_key: str):
        """Spawn a chat recording thread (IRC for Twitch, yt-dlp for YouTube)."""
        stream = self.active_streams[stream_key]
        platform = stream["platform"]
        title = stream["stream_title"]
        stop_event = threading.Event()
        stream["chat_stop_event"] = stop_event

        if platform == "twitch":
            channel = self.config["twitch_user"]
            start_ms = int(stream["start_time"].timestamp() * 1000)
            output = os.path.join(self.config["output"], f"{title}.json")

            def run():
                ls_common.record_twitch_chat(
                    channel, start_ms, output, stop_event, logger,
                )
        else:
            # YouTube: yt-dlp live_chat (captures pre-stream chat via replay)
            def run():
                max_retries = 10
                retry_delay = 30
                attempt = 0
                while not stop_event.is_set() and attempt < max_retries:
                    try:
                        cmd = ls_common.ytdlp_chat_cmd(
                            self.config, stream["url"], f"{title}.%(ext)s",
                        )
                        proc = subprocess.Popen(
                            cmd, cwd=self.config["output"],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True,
                        )
                        while proc.poll() is None:
                            if stop_event.is_set():
                                proc.terminate()
                                try:
                                    proc.wait(timeout=5)
                                except subprocess.TimeoutExpired:
                                    proc.kill()
                                break
                            time.sleep(0.5)

                        if stop_event.is_set():
                            break
                        rc = proc.returncode
                        if rc == 0:
                            break  # clean exit — stream ended
                        attempt += 1
                        logger.warning(
                            f"Chat exited rc={rc}, retry "
                            f"{attempt}/{max_retries} in {retry_delay}s"
                        )
                        time.sleep(retry_delay)
                    except Exception as e:
                        attempt += 1
                        logger.error(f"Chat error (attempt {attempt}): {e}")
                        if not stop_event.is_set():
                            time.sleep(retry_delay)

                ls_common.merge_chat_fragments(self.config["output"], title)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        stream["chat_thread"] = thread
        logger.info(f"Chat recording started: {title}")

    # ── monitor / watchdog ────────────────────────────────────────────────

    def _monitor_loop(self):
        """Background sampler: file growth + watchdog, every SAMPLE_INTERVAL_S."""
        while not self._monitor_stop.is_set():
            if self._monitor_stop.wait(SAMPLE_INTERVAL_S):
                return
            try:
                if self.manual_termination_in_progress:
                    continue
                now = time.time()
                for stream_key, stream in list(self.active_streams.items()):
                    try:
                        self._sample_stream(stream, now)
                        self._watchdog_check(stream_key, stream, now)
                    except Exception as e:
                        logger.error(f"Monitor error on {stream_key}: {e}")
            except Exception as e:
                logger.error(f"Monitor loop error: {e}")

    def _sample_stream(self, stream: dict, now: float):
        """Add a (timestamp, size) sample. Probe bitrate when enough data exists."""
        title = stream["stream_title"]
        file_path = self._find_video(title)
        if not file_path or not os.path.exists(file_path):
            return

        try:
            size = os.path.getsize(file_path)
        except OSError:
            return

        samples = stream["_samples"]
        last_size = stream.get("_last_size", 0)
        samples.append((now, size))
        if size > last_size:
            stream["_last_growth_ts"] = now
            stream["_watchdog_triggered"] = False  # growth resets watchdog state
        stream["_last_size"] = size

        if (stream.get("_bitrate_bps") is None
                and size > BITRATE_PROBE_MIN_MB * 1024 * 1024):
            bitrate = ls_common.probe_bitrate(file_path)
            if bitrate:
                stream["_bitrate_bps"] = bitrate
                logger.info(
                    f"Bitrate probed for {title}: "
                    f"{bitrate / 1_000_000:.2f} Mbps"
                )

    def _watchdog_check(self, stream_key: str, stream: dict, now: float):
        """Kill yt-dlp if file hasn't grown in WATCHDOG_STALL_S seconds."""
        if stream.get("_watchdog_triggered"):
            return
        vp = stream.get("video_process")
        if not vp or vp.poll() is not None:
            return  # process gone; monitor() handles
        last_growth = stream.get("_last_growth_ts")
        if last_growth is None or (now - last_growth) < WATCHDOG_STALL_S:
            return
        stream["_watchdog_triggered"] = True
        logger.warning(
            f"Watchdog: no file growth for {WATCHDOG_STALL_S}s in "
            f"{stream['stream_title']}, killing yt-dlp (PID {vp.pid})"
        )
        try:
            vp.terminate()
            # Don't wait here — let the monitor thread handle the exit and restart
        except Exception as e:
            logger.error(f"Watchdog terminate failed: {e}")

    # ── completion & upload ───────────────────────────────────────────────

    def _handle_completion(self, stream_key: str, upload: bool = True):
        if stream_key not in self.active_streams:
            return
        stream = self.active_streams[stream_key]
        title = stream["stream_title"]
        platform = stream["platform"]
        obs_idx = stream.get("obsidian_index")
        logger.info(f"Completing: {title}")

        try:
            # Stop video if still running (e.g. shutdown path)
            vp = stream.get("video_process")
            if vp and vp.poll() is None:
                try:
                    vp.terminate()
                    vp.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    vp.kill()

            # Stop chat
            if stream.get("chat_stop_event"):
                stream["chat_stop_event"].set()
                if stream.get("chat_thread"):
                    stream["chat_thread"].join(timeout=15)

            if not upload or not os.path.exists(self.config["nas_path"]):
                return

            # ── Chat ──
            chat_file = os.path.join(self.config["output"], f"{title}.json")
            if os.path.exists(chat_file) and os.path.getsize(chat_file) > 100:
                chat_dst = os.path.join(self.config["nas_path"], f"{title}.json")
                self._upload(chat_file, chat_dst)

            # ── Video ──
            video_file = self._find_video(title)
            uploaded_ext = None
            duration = None
            if video_file:
                ext = os.path.splitext(video_file)[1]
                dst = os.path.join(self.config["nas_path"], f"{title}{ext}")
                ok, dur = self._upload(video_file, dst)
                if ok:
                    uploaded_ext = ext
                    duration = dur

            # ── Obsidian + cache ──
            if uploaded_ext and obs_idx:
                ls_common.obsidian_update_entry(
                    self.config, obs_idx, platform,
                    stream_title=title, duration_seconds=duration,
                    video_ext=uploaded_ext,
                )
                if duration:
                    cache = ls_common.load_cache()
                    vod = ls_common.find_vod(
                        cache, stream["identifier"], platform,
                    )
                    if vod:
                        vod["duration"] = int(duration)
                        ls_common.save_cache(cache)
                logger.info(f"Uploaded and logged: {title}")

        except Exception as e:
            logger.error(f"Completion error for {title}: {e}")
        finally:
            self.active_streams.pop(stream_key, None)
            logger.info(f"Cleanup done: {title}")

    def _find_video(self, stream_title: str) -> str | None:
        """Locate the recorded video file (yt-dlp picks the extension)."""
        output_dir = self.config["output"]
        for ext in ls_common.VIDEO_EXTS:
            path = os.path.join(output_dir, f"{stream_title}{ext}")
            if os.path.exists(path):
                return path
        # Fallback: glob, skip fragments and non-video
        for path in sorted(glob.glob(os.path.join(output_dir, f"{stream_title}.*"))):
            base = os.path.basename(path)
            if re.search(r"\.f\d+\.\w+$", base):
                continue
            if base.endswith(".json") or base.endswith(".part"):
                continue
            if base.endswith(".ytdlp.log") or base.endswith(".frag.json"):
                continue
            if os.path.splitext(base)[1].lower() in ls_common.VIDEO_EXTS:
                return path
        return None

    def _upload(self, src: str, dst: str) -> tuple[bool, float | None]:
        """Move file to NAS. Returns (success, duration_seconds)."""
        try:
            if os.path.exists(dst):
                logger.info(f"Already on NAS: {os.path.basename(src)}")
                os.remove(src)
                return True, None

            ext = os.path.splitext(src)[1].lower()
            is_video = ext in ls_common.VIDEO_EXTS
            duration = ls_common.probe_duration(src) if is_video else None

            if ext == ".mp4":
                # Remux with faststart for Premiere compatibility
                r = subprocess.run(
                    ["ffmpeg", "-i", src, "-c", "copy",
                     "-movflags", "+faststart", dst],
                    capture_output=True, text=True,
                )
                if r.returncode == 0:
                    os.remove(src)
                    logger.info(f"Uploaded (faststart): {os.path.basename(src)}")
                    return True, duration
                logger.error(f"ffmpeg failed: {r.stderr[-200:]}")
                return False, None

            # Non-mp4 or non-video: rsync to NAS
            subprocess.run(
                ["rsync", "-av", "--remove-source-files", src, dst], check=True,
            )
            logger.info(f"Uploaded (rsync): {os.path.basename(src)}")
            return True, duration

        except Exception as e:
            logger.error(f"Upload failed: {os.path.basename(src)}: {e}")
            return False, None

    # ── main loop ─────────────────────────────────────────────────────────

    def run(self):
        logger.info("=" * 60)
        logger.info("ls-rec starting")
        logger.info("=" * 60)
        logger.info(f"  > Check interval: {self.config['check_interval']}s")
        logger.info(f"  > Cooldown: {self.config['cooldown_duration']}s")
        logger.info(f"  > Watchdog stall threshold: {WATCHDOG_STALL_S}s")
        self.command_server.start()
        print("-" * 80)

        try:
            while True:
                # Cooldown after manual termination
                if not self._is_monitoring_allowed():
                    now = datetime.datetime.now()
                    remain = max(
                        0,
                        (self.monitoring_cooldown_until - now).total_seconds(),
                    )
                    pct = int(
                        20 * (1 - remain / self.config["cooldown_duration"])
                    )
                    ts = now.strftime("%H:%M:%S")
                    print(
                        f"[{ts}] Cooldown: "
                        f"[{'#' * pct}{'.' * (20 - pct)}] {remain:.0f}s"
                    )
                    time.sleep(self.config["check_interval"])
                    continue

                # Always probe, even while already recording
                self._check_streams()

                if self.active_streams:
                    if not self.was_streaming:
                        logger.info(f"Active: {len(self.active_streams)}")
                        self.was_streaming = True
                else:
                    if self.was_streaming:
                        logger.info("All streams ended, resuming monitoring")
                        self.was_streaming = False
                    now = datetime.datetime.now()
                    nxt = now + datetime.timedelta(
                        seconds=self.config["check_interval"],
                    )
                    print(
                        f"[{now.strftime('%H:%M:%S')}] "
                        f"No streams. Next: {nxt.strftime('%H:%M:%S')}"
                    )

                # Watch list
                if self.watch_list:
                    self._probe_watchlist()

                time.sleep(self.config["check_interval"])

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt")
            self._shutdown()

    def _log_disk_space(self):
        try:
            stat = os.statvfs(self.config["output"])
            free = (stat.f_bavail * stat.f_frsize) / (1024 ** 3)
            logger.info(f"  > Disk: {free:.1f} GB free")
            if free < 10:
                logger.warning(f"Low disk space: {free:.1f} GB")
        except Exception:
            pass

    def _shutdown(self):
        logger.info("Shutting down...")
        self._monitor_stop.set()
        self.command_server.stop()
        for key in list(self.active_streams):
            self._handle_completion(key, upload=False)
        logger.info("Shutdown complete.")


# ═══════════════════════════════════════════════════════════════════════════
#  MANDO  (direct VOD download — runs in your terminal, no daemon)
# ═══════════════════════════════════════════════════════════════════════════

def cmd_mando(args):
    config = ls_common.load_config()
    url = args.url
    dl_type = args.type or "both"
    prefix = args.index

    print("  ⌛ Fetching metadata...")
    data = ls_common.ytdlp_probe(config, url)
    if not data:
        print(f"  ✗ Could not fetch: {url}")
        sys.exit(1)

    title = data.get("title") or "Unknown"
    video_id = data.get("id", "unknown")
    platform = "twitch" if "twitch.tv" in url else "youtube"

    release_ts = data.get("release_timestamp")
    upload_date = data.get("upload_date", "")
    if release_ts:
        ts_str = datetime.datetime.fromtimestamp(release_ts).strftime("%Y-%m-%d_%H-%M")
        start_iso = datetime.datetime.fromtimestamp(release_ts).isoformat()
    elif upload_date:
        ts_str = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}_00-00"
        start_iso = datetime.datetime.strptime(upload_date, "%Y%m%d").isoformat()
    else:
        ts_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        start_iso = datetime.datetime.now().isoformat()

    safe_title = sanitize_filename(f"{title} [{video_id}] @ {ts_str}")
    if prefix is not None:
        safe_title = f"{int(prefix):03d}_{safe_title}"

    nas_path = config["nas_path"]
    os.makedirs(nas_path, exist_ok=True)

    print(f"  Title    : {title}")
    print(f"  ID       : {video_id}")
    print(f"  Platform : {platform}")
    print(f"  Saving as: {safe_title}")
    print(f"  Output   : {nas_path}")
    print("  " + "-" * 50)

    if dl_type in ("video", "both"):
        print("\n  ↓ Downloading video...")
        cmd = ls_common.ytdlp_vod_cmd(config, url, f"{safe_title}.%(ext)s")
        subprocess.run(cmd, cwd=nas_path)

    if dl_type in ("chat", "both"):
        print("\n  ↓ Downloading chat...")
        tdl = config.get("twitch_downloader_cli")
        if platform == "twitch" and tdl and os.path.exists(tdl):
            vod_id = url.rstrip("/").split("/")[-1]
            chat_out = os.path.join(nas_path, f"{safe_title}.json")
            subprocess.run([tdl, "chatdownload", "--id", vod_id, "-o", chat_out])
        else:
            cmd = ls_common.ytdlp_chat_cmd(
                config, url, f"{safe_title}.%(ext)s",
            )
            subprocess.run(cmd, cwd=nas_path)
            lc = os.path.join(nas_path, f"{safe_title}.live_chat.json")
            final = os.path.join(nas_path, f"{safe_title}.json")
            if os.path.exists(lc):
                os.rename(lc, final)

    # Update cache
    cache = ls_common.load_cache()
    vod: dict = {
        "id":         video_id,
        "platform":   platform,
        "title":      title,
        "start_time": start_iso,
        "channel":    (data.get("channel") or data.get("uploader")
                       or config.get("youtube_handle", "")),
        "duration":   data.get("duration"),
    }
    if prefix is not None:
        vod["obsidian_index"] = int(prefix)
    ls_common.upsert_vod(cache, vod)
    ls_common.save_cache(cache)

    print("\n  ✔ Done. Cache updated.")


# ═══════════════════════════════════════════════════════════════════════════
#  CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) < 2 or sys.argv[1] == "run":
        _setup_logging()
        recorder = LivestreamRecorder()
        recorder.run()
        return

    cmd = sys.argv[1]

    # Mando: direct download, no daemon
    if cmd == "mando":
        parser = argparse.ArgumentParser(prog="ls-rec mando")
        parser.add_argument("url", help="Stream/VOD URL")
        parser.add_argument("--index", type=int, help="Index prefix (e.g. 557)")
        parser.add_argument(
            "--type", choices=["video", "chat", "both"], default="both",
        )
        args = parser.parse_args(sys.argv[2:])
        cmd_mando(args)
        return

    # Tail: socket roundtrip, then exec tail -F
    if cmd == "tail":
        target = sys.argv[2] if len(sys.argv) > 2 else None
        do_tail(target)
        return

    # Everything else → daemon over socket
    send_command_and_print(" ".join(sys.argv[1:]))


if __name__ == "__main__":
    main()
