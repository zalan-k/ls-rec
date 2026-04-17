#!/usr/bin/env python3
"""
ls-rec - Livestream recorder daemon and CLI.

Usage:
    ls-rec                      Start daemon (monitor + record)
    ls-rec run                  Same as above

    ls-rec status               Show active recordings, watch list, last checks
    ls-rec check [youtube|twitch]   Force-probe for live streams
    ls-rec record <url>         Record live stream / watch if scheduled
    ls-rec watch <url>          Add URL to watch list
    ls-rec unwatch [url|N]      Remove from watch list

    ls-rec mando <url> [--index N] [--type video|chat|both]
                                Download VOD directly to NAS
"""

import os, re, glob, time, json, logging, subprocess, datetime, sys
import signal, threading, socket, argparse
from pathlib import Path
from yt_dlp.utils import sanitize_filename

import ls_common

SOCKET_PATH = "/tmp/livestream-recorder.sock"
YT_ROTATION_SECONDS = 3.5 * 3600

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

def send_command(command: str):
    if not os.path.exists(SOCKET_PATH):
        print("ERROR: ls-rec daemon is not running.")
        print("  Start with: ls-rec run")
        sys.exit(1)
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(35)
        sock.connect(SOCKET_PATH)
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
        sock.close()
        print(b"".join(chunks).decode("utf-8"))
    except ConnectionRefusedError:
        print("ERROR: Could not connect. Daemon may have crashed.")
        sys.exit(1)
    except socket.timeout:
        print("ERROR: Command timed out.")
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

    # ── SIGINT ────────────────────────────────────────────────────────────

    def _handle_sigint(self, sig, frame):
        if self.manual_termination_in_progress:
            print("\nForce terminating...")
            signal.signal(signal.SIGINT, self._orig_sigint)
            os.kill(os.getpid(), signal.SIGINT)
            return

        if self.active_streams:
            print("\nCtrl+C — letting yt-dlp terminate naturally...")
            self.manual_termination_in_progress = True
            for s in self.active_streams.values():
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

    # ── command dispatch ──────────────────────────────────────────────────

    def handle_command(self, command: str) -> str:
        parts = command.split()
        cmd = parts[0].lower()
        if cmd == "status":
            return self._cmd_status()
        if cmd == "check":
            p = parts[1] if len(parts) > 1 and parts[1] in ("youtube", "twitch") else None
            return self._cmd_check(p)
        if cmd == "record":
            return self._cmd_record(parts[1] if len(parts) > 1 else None)
        if cmd == "watch":
            return self._cmd_watch(parts[1] if len(parts) > 1 else None)
        if cmd == "unwatch":
            return self._cmd_unwatch(parts[1] if len(parts) > 1 else None)
        return ("Commands: status | check [youtube|twitch] | "
                "record <url> | watch <url> | unwatch [url|N]")

    def _cmd_status(self) -> str:
        lines = []
 
        # ── Monitored ────────────────────────────────────────────
        lines.append("")
        lines.append(" ─── Monitored ──────────────────────────────────────────────────────")
        t_w = 30  # title width

        # Default channels (always pinged)
        defaults = [
            ("YT", self.config["youtube_handle"], f"{self.config['check_interval']}s"),
            ("TW", self.config["twitch_user"], f"{self.config['check_interval']}s"),
        ]
        # Watch list entries
        watched = []
        for url, info in self.watch_list.items():
            plat = "TW" if "twitch.tv" in url else "YT"
            title = info.get("title", "Unknown")
            if len(title) > t_w - 3:
                title = title[: t_w - 3] + "..."
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
 
        # ── Recording ────────────────────────────────────────────
        lines.append("")
        lines.append(" ─── Recording ─────────────────────────────────────────────────────")
        if self.active_streams:
            t_w = 30
            lines.append(f"  {'Platform'} │ {'Title':<{t_w}} │ {'Elapsed':<8} │ URL")
            lines.append(f"  ─────────┼─{'─' * t_w}─┼──────────┼─────────────────────")
            for _, s in self.active_streams.items():
                plat = s["platform"][:2].upper()
                title = s.get("obsidian_title", "Unknown")
                if len(title) > t_w - 3:
                    title = title[: t_w - 3] + "..."
                elapsed = str(datetime.datetime.now() - s["start_time"]).split(".")[0]
                url = s.get("url", "")
                # Shorten URL for display
                url_short = url.replace("https://www.", "").replace("https://", "")
                lines.append(f"  {plat:<8} │ {title:<{t_w}} │ {elapsed:<8} │ {url_short}")
        else:
            lines.append("  (none)")
 
        # ── Last Checked ─────────────────────────────────────────
        lines.append("")
        lines.append(" ─── Last Checked ──────────────────────────────────────────────────")
        for plat in ("youtube", "twitch"):
            t = self.last_check_time.get(plat)
            ts = t.strftime("%H:%M:%S") if t else "never"
            lines.append(f"  {plat.capitalize():<7} │ {ts}")
 
        lines.append("")
        return "\n".join(lines)

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
                # Update scheduled start if available
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
        """Create logs, cache entry, and spawn video + chat threads."""
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

        # Filename with index prefix
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
        }

        threading.Thread(
            target=self._record_video, args=(stream_key,), daemon=True,
        ).start()
        threading.Thread(
            target=self._record_chat, args=(stream_key,), daemon=True,
        ).start()

    def _merge_video_parts(self, parts_info: list[dict], output_path: str) -> bool:
        """Merge multiple video segments, trimming DVR overlap."""
        paths = [p["path"] for p in parts_info]

        if len(paths) == 1:
            os.rename(paths[0], output_path)
            return True

        concat_list = output_path + ".concat.txt"
        trimmed_files = []
        try:
            for i, entry in enumerate(parts_info):
                p = entry["path"]

                if i == 0:
                    trimmed_files.append(p)
                    continue

                # Compute overlap: how much longer the previous file is
                # than the wall-clock gap between segment starts
                prev = parts_info[i - 1]
                wall_clock = entry["started_at"] - prev["started_at"]
                prev_dur = ls_common.probe_duration(prev["path"])

                # Only trim if previous segment is >30s longer than wall clock
                # (genuine DVR overlap, not just timestamp jitter)
                if prev_dur and prev_dur > wall_clock + 30:
                    overlap = prev_dur - wall_clock
                    trimmed = p + ".trimmed" + os.path.splitext(p)[1]
                    r = subprocess.run(
                        ["ffmpeg", "-y", "-ss", str(overlap),
                        "-i", p, "-c", "copy", trimmed],
                        capture_output=True, text=True,
                    )
                    if r.returncode == 0 and os.path.exists(trimmed):
                        trimmed_files.append(trimmed)
                        logger.info(
                            f"Trimmed {overlap:.0f}s overlap from "
                            f"{os.path.basename(p)}"
                        )
                    else:
                        logger.warning(
                            f"Trim failed for {os.path.basename(p)}, "
                            f"using untrimmed"
                        )
                        trimmed_files.append(p)
                else:
                    trimmed_files.append(p)

            with open(concat_list, "w") as f:
                for p in trimmed_files:
                    escaped = p.replace("'", "'\\''")
                    f.write(f"file '{escaped}'\n")

            logger.info(
                f"Merging {len(paths)} segments → "
                f"{os.path.basename(output_path)}"
            )
            r = subprocess.run(
                ["ffmpeg", "-y", "-f", "concat", "-safe", "0",
                "-i", concat_list, "-c", "copy", output_path],
                capture_output=True, text=True,
            )
            if r.returncode != 0:
                logger.error(f"Merge failed: {r.stderr[-300:]}")
                return False

            if not os.path.exists(output_path):
                return False

            # Clean up parts and trimmed intermediates
            for p in paths:
                if os.path.exists(p):
                    os.remove(p)
            for p in trimmed_files:
                if p not in paths and os.path.exists(p):
                    os.remove(p)

            logger.info(f"Merged {len(paths)} parts successfully")
            return True

        except Exception as e:
            logger.error(f"Merge error: {e}")
            return False
        finally:
            if os.path.exists(concat_list):
                os.remove(concat_list)

    def _record_video(self, stream_key: str):
        stream = self.active_streams[stream_key]
        url = stream["url"]
        platform = stream["platform"]
        base_title = stream["stream_title"]
 
        # Track segment number and file list
        part = stream.get("_video_part", 1)
        stream["_video_part"] = part
        if "_video_parts" not in stream:
            stream["_video_parts"] = []
 
        # Part suffix only on segment 2+
        title = f"{base_title}_part{part}" if part > 1 else base_title
 
        try:
            cmd = ls_common.ytdlp_live_cmd(
                self.config, url, platform, f"{title}.%(ext)s",
            )
            process = subprocess.Popen(cmd, cwd=self.config["output"])
            stream["video_process"] = process
            if part == 1:
                stream["_segment_start"] = time.time()
            logger.info(f"Video started: {title}")
 
            def monitor():
                rotate = (platform == "youtube"
                          and not self.manual_termination_in_progress)
 
                if rotate:
                    start = time.time()
                    while process.poll() is None:
                        elapsed = time.time() - start
                        if elapsed >= YT_ROTATION_SECONDS:
                            logger.info("3.5h rotation — stopping current segment")

                            # Kill old process, let it finalize properly
                            process.terminate()
                            try:
                                process.wait(timeout=180)
                            except subprocess.TimeoutExpired:
                                process.kill()

                            # Register this part with its wall-clock start
                            part_file = self._find_video(title)
                            if part_file:
                                stream["_video_parts"].append({
                                    "path": part_file,
                                    "started_at": stream.get("_segment_start", start),
                                })
                                logger.info(f"Segment saved: {part_file}")

                            # Rotate chat, then start new video
                            stream["_video_part"] = part + 1
                            stream["_segment_start"] = time.time()
                            self._rotate_chat(stream_key, part + 1)
                            self._record_video(stream_key)
                            return
 
                        time.sleep(1)
                else:
                    process.wait()
 
                # Process exited (stream ended, crash, or manual termination)
                rc = process.returncode
 
                # Register this (final) part file
                part_file = self._find_video(title)
                if part_file:
                    stream["_video_parts"].append({
                        "path": part_file,
                        "started_at": stream.get("_segment_start", time.time()),
                    })
 
                # YouTube: check if stream is actually still live
                if (platform == "youtube"
                        and rc in (0, 1)
                        and not self.manual_termination_in_progress):
                    time.sleep(15)
                    still_live = self._probe_platform("youtube")
                    if still_live and still_live["video_id"] == stream["identifier"]:
                        logger.warning("yt-dlp exited but stream still live — restarting")

                        stream["_video_part"] = part + 1
                        stream["_segment_start"] = time.time()
                        self._record_video(stream_key)
                        self._rotate_chat(stream_key, part + 1)
                        return
 
                if rc in (0, 1):
                    logger.info(f"Video complete: {title}")
                else:
                    logger.error(f"Video failed ({rc}): {title}")
 
                self._handle_completion(stream_key)
 
                if self.manual_termination_in_progress:
                    active = [
                        s for s in self.active_streams.values()
                        if s.get("video_process")
                        and s["video_process"].poll() is None
                    ]
                    if not active:
                        print("All streams finished. Cooldown active.")
                        self.manual_termination_in_progress = False
 
            threading.Thread(target=monitor, daemon=True).start()
 
        except Exception as e:
            logger.error(f"Video start error: {e}")
            self.active_streams.pop(stream_key, None)

    def _record_chat(self, stream_key: str, part: int = 1):
        stream = self.active_streams[stream_key]
        platform = stream["platform"]
        base_title = stream["stream_title"]
        title = f"{base_title}_part{part}" if part > 1 else base_title
        stream["_chat_title"] = title
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
            # YouTube: yt-dlp live_chat
            def run():
                try:
                    cmd = ls_common.ytdlp_chat_cmd(
                        self.config, stream["url"], f"{title}.%(ext)s",
                    )
                    proc = subprocess.Popen(
                        cmd, cwd=self.config["output"],
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        text=True,
                    )
                    stream["chat_process"] = proc
                    while proc.poll() is None:
                        if stop_event.is_set():
                            proc.terminate()
                            try:
                                proc.wait(timeout=5)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                            break
                        time.sleep(0.5)
                    ls_common.merge_chat_fragments(self.config["output"], title)
                except Exception as e:
                    logger.error(f"Chat error: {e}")

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        stream["chat_thread"] = thread
        logger.info(f"Chat started: {title}")

    def _rotate_chat(self, stream_key: str, new_part: int):
        """Stop current chat recorder, wait for its fragment merge, start a new one."""
        stream = self.active_streams[stream_key]

        old_stop   = stream.get("chat_stop_event")
        old_thread = stream.get("chat_thread")
        old_title  = stream.get("_chat_title")

        if old_stop:
            old_stop.set()
        if old_thread:
            old_thread.join(timeout=15)   # lets merge_chat_fragments finish

        if old_title:
            chat_file = os.path.join(self.config["output"], f"{old_title}.json")
            if os.path.exists(chat_file):
                stream.setdefault("_chat_parts", []).append(chat_file)
                logger.info(f"Chat segment saved: {old_title}.json")

        self._record_chat(stream_key, part=new_part)

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
            # Stop video if still running
            vp = stream.get("video_process")
            if vp and vp.poll() is None:
                vp.terminate()

            # Stop chat
            if stream.get("chat_stop_event"):
                stream["chat_stop_event"].set()
                if stream.get("chat_thread"):
                    stream["chat_thread"].join(timeout=15)   # was 5
            
            if not upload or not os.path.exists(self.config["nas_path"]):
                return
            
            # Collect all chat parts: prior rotations + the final segment
            chat_parts = list(stream.get("_chat_parts", []))
            final_title = stream.get("_chat_title") or title
            final_chat = os.path.join(self.config["output"], f"{final_title}.json")
            if os.path.exists(final_chat) and final_chat not in chat_parts:
                chat_parts.append(final_chat)
            chat_parts = [p for p in chat_parts
                        if os.path.exists(p) and os.path.getsize(p) > 100]

            if len(chat_parts) == 1:
                chat_dst = os.path.join(self.config["nas_path"], f"{title}.json")
                self._upload(chat_parts[0], chat_dst)
            elif len(chat_parts) > 1 and platform == "youtube":
                # YouTube live_chat is JSONL — concat line-wise
                merged_chat = os.path.join(
                    self.config["output"], f"{title}.merged_chat.json",
                )
                try:
                    with open(merged_chat, "w", encoding="utf-8") as out:
                        for p in chat_parts:
                            with open(p, "r", encoding="utf-8") as f:
                                content = f.read()
                            out.write(content)
                            if content and not content.endswith("\n"):
                                out.write("\n")
                    chat_dst = os.path.join(self.config["nas_path"], f"{title}.json")
                    ok, _ = self._upload(merged_chat, chat_dst)
                    if ok:
                        for p in chat_parts:
                            try:
                                os.remove(p)
                            except Exception:
                                pass
                except Exception as e:
                    logger.error(f"Chat merge error: {e}")
                    for p in chat_parts:
                        dst = os.path.join(self.config["nas_path"], os.path.basename(p))
                        self._upload(p, dst)

            # Merge video parts if multiple segments exist
            raw_parts = stream.get("_video_parts", [])
            # Filter to files that still exist and aren't stubs
            parts_info = []
            for entry in raw_parts:
                p = entry["path"]
                if not os.path.exists(p):
                    continue
                dur = ls_common.probe_duration(p)
                if dur is None or dur < 10:
                    logger.warning(
                        f"Dropping bad segment: {os.path.basename(p)} "
                        f"(duration={dur})"
                    )
                    try:
                        os.remove(p)
                    except Exception:
                        pass
                    continue
                parts_info.append(entry)

            duration = None
            uploaded_ext = None

            if parts_info:
                ext = os.path.splitext(parts_info[0]["path"])[1]
                merged_path = os.path.join(
                    self.config["output"], f"{title}.merged{ext}",
                )

                if self._merge_video_parts(parts_info, merged_path):
                    duration = ls_common.probe_duration(merged_path)
                    dst = os.path.join(self.config["nas_path"], f"{title}{ext}")
                    ok, dur = self._upload(merged_path, dst)
                    if ok:
                        uploaded_ext = ext
                        duration = dur or duration
                else:
                    logger.warning("Merge failed, uploading parts separately")
                    for entry in parts_info:
                        p = entry["path"]
                        ext = os.path.splitext(p)[1]
                        dst = os.path.join(
                            self.config["nas_path"], os.path.basename(p),
                        )
                        self._upload(p, dst)
                    uploaded_ext = os.path.splitext(parts_info[0]["path"])[1]
                    duration = ls_common.probe_duration(parts_info[-1]["path"])
            else:
                video_file = self._find_video(title)
                if video_file:
                    ext = os.path.splitext(video_file)[1]
                    dst = os.path.join(self.config["nas_path"], f"{title}{ext}")
                    ok, dur = self._upload(video_file, dst)
                    if ok:
                        uploaded_ext = ext
                        duration = dur

            # Update obsidian + cache with file paths and duration
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

    # Probe metadata
    print("  ⌛ Fetching metadata...")
    data = ls_common.ytdlp_probe(config, url)
    if not data:
        print(f"  ✗ Could not fetch: {url}")
        sys.exit(1)

    title = data.get("title") or "Unknown"
    video_id = data.get("id", "unknown")
    platform = "twitch" if "twitch.tv" in url else "youtube"

    # Build filename
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

    # Download video
    if dl_type in ("video", "both"):
        print("\n  ↓ Downloading video...")
        cmd = ls_common.ytdlp_vod_cmd(config, url, f"{safe_title}.%(ext)s")
        subprocess.run(cmd, cwd=nas_path)

    # Download chat
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
            # Rename yt-dlp's .live_chat.json → .json
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

    # Mando: direct download, no daemon involved
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

    # Everything else → send to daemon via socket
    send_command(" ".join(sys.argv[1:]))


if __name__ == "__main__":
    main()
