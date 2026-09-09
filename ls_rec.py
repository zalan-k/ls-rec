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

    ls-rec mark "what happened"  Write a !note into the stream's Obsidian entry
    ls-rec clip <when> [length]  Cut a clip from the recording already on disk
    ls-rec clip --all [length]   Cut every !note on the entry, clearing each !

    ls-rec mando <url> [--index N] [--type video|chat|both]
                                 Download VOD directly to NAS

YouTube recording uses yt-dlp's --live-from-start, pulling from the
broadcast start via DVR. One process per stream, no rotation. A watchdog
thread samples file size every 10s and restarts yt-dlp if it stalls.

`clip` cuts from the file the recorder is already writing -- no network, no
second download, no re-encode. It only ever reads an ACTIVE recording; a
finished broadcast is a normal file on the NAS and wants a normal editor.

`mark` writes nothing but a line of text: it appends

    - [ ] !Tenma ate an orange (00:42:13 / r00:41:09)

to the stream's Obsidian entry, where the first stamp is broadcast time (what
a player shows) and the second is the offset into the captured file. The `!`
is a queue marker for a later pass that cuts the clip and clears it.
"""

import os, re, glob, time, shlex, logging, subprocess, datetime, sys, signal, threading, socket, argparse, ls_common, ls_archive
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

# ── Clipping constants ────────────────────────────────────────────────────
CLIP_TAIL_GUARD_S    = 5      # never cut closer than this to the live edge
CLIP_TIMEOUT_S       = 900    # ffmpeg wall-clock ceiling for one cut

# ═══════════════════════════════════════════════════════════════════════════
#  CLIP TIME PARSING
# ═══════════════════════════════════════════════════════════════════════════
#
#  Every accepted spelling of "when" resolves to one absolute epoch before
#  anything touches a file. Wall-clock is the only timebase that survives a
#  Twitch part rotation: the gap between part N dying and part N+1 spawning
#  exists in wall-clock but not on disk, so an offset measured from the start
#  of the recording drifts by the sum of every gap before it. Each part
#  instead carries the wall time it was spawned, and the epoch picks the part.
#
#  The SHAPE of the token picks the timebase, so there is no flag to remember
#  and no way for one string to mean two things:
#
#      -12m / -90s / -1h20m     that long ago
#      21:42 / 21:42:30         today, local wall clock
#      @1:23:45                 that far into the broadcast
#      2026.08.20 21:42         absolute (quote it, or 2026.08.20T21:42)
#      now                      right now

_DUR_RE   = re.compile(r"(?:(\d+)h)?(?:(\d+)m)?(?:(\d+(?:\.\d+)?)s)?", re.I)
_CLOCK_RE = re.compile(r"^(\d{1,3}):([0-5]?\d)(?::([0-5]?\d(?:\.\d+)?))?$")
_DATE_RE  = re.compile(r"^(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})$")


def _parse_clock(s: str) -> float | None:
    """HH:MM[:SS] -> seconds. Two fields are HOURS:MINUTES, never MM:SS."""
    m = _CLOCK_RE.match(s.strip())
    if not m:
        return None
    h, mi, sec = m.group(1), m.group(2), m.group(3)
    if sec is None:
        return int(h) * 3600 + int(mi) * 60
    return int(h) * 3600 + int(mi) * 60 + float(sec)


def _parse_duration(s: str, bare_seconds: bool = False) -> float | None:
    """`5` -> 5 minutes (or seconds if bare_seconds). `90s` `5m` `1h` `1h30m`.
    Full `HH:MM:SS` also works.

    Two-field `5:00` is REJECTED rather than guessed. It reads as five
    minutes to a human and as five hours to _parse_clock, and silently
    cutting a five-hour clip is a worse outcome than an error message.
    """
    s = (s or "").strip().lower()
    if not s:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", s):
        return float(s) * (1 if bare_seconds else 60)
    if s.count(":") == 2:
        return _parse_clock(s)
    if ":" in s:
        return None
    m = _DUR_RE.fullmatch(s)
    if not m or not any(m.groups()):
        return None
    h, mi, sec = m.groups()
    return int(h or 0) * 3600 + int(mi or 0) * 60 + float(sec or 0)


def _fmt_hms(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _fmt_dur(seconds: float) -> str:
    """Compact and human: 90s, 12m, 1h20m, 3m00s."""
    seconds = max(0, int(round(seconds)))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{s:02d}s" if s else f"{m}m"
    return f"{s}s"


def _local_dt(day: datetime.date, seconds_into_day: float) -> datetime.datetime:
    """Combine a local date with an offset into it.

    datetime.combine is used rather than midnight + timedelta because only
    the former asks the platform to resolve a naive local time; adding a
    raw 21h42m to midnight lands an hour off on the two DST days a year.
    """
    total = int(seconds_into_day)
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    base = datetime.datetime.combine(day, datetime.time(h % 24, m, s))
    return base + datetime.timedelta(days=h // 24)


_NOTE_RE = re.compile(r"^!\s*(?P<text>.*?)\s*\((?P<stamp>[^)]*)\)\s*$")


def _parse_note(raw: str) -> dict | None:
    """Parse a `!` note back into a clip request.

    Accepts everything `mark` writes and everything you would plausibly type
    by hand:

        !text (00:16:00 / r00:15:00)      both stamps -- prefer the r one
        !text (r00:15:00)                 broadcast start was unknown
        !text (00:42:13)                  hand-written, broadcast-relative
        !text (00:42:13, 4m)              with an explicit length

    The trailing stamp wins because `mark` writes the record-relative one
    last, and that one needs no conversion and cannot be missing.
    """
    m = _NOTE_RE.match(raw.strip())
    if not m:
        return None
    parts = [p.strip() for p in m.group("stamp").split(",")]
    times = [t.strip() for t in parts[0].split("/") if t.strip()]
    if not times:
        return None
    tok  = times[-1]
    secs = _parse_clock(tok.lstrip("rR"))
    if secs is None:
        return None
    return {
        "text":        m.group("text"),
        "offset":      secs,
        "from_record": tok[:1] in ("r", "R"),
        "length":      _parse_duration(parts[1]) if len(parts) > 1 else None,
    }


def _looks_like_time(tok: str) -> bool:
    """Is this leading-dash token `-12m` rather than a mistyped flag?

    Needed because the most natural way to say when is also the one shape
    that collides with option syntax.
    """
    return tok.startswith("-") and _parse_duration(tok[1:]) is not None


def _parse_when(tokens: list[str], now: float,
                stream_zero: float | None) -> tuple[float, str, list[str]]:
    """Consume the leading token(s) of `tokens` as one instant.

    Returns (epoch, human description, remaining tokens).
    `stream_zero` is the broadcast-start epoch, needed only for `@` offsets.
    """
    if not tokens:
        raise ValueError("missing <when>")
    tok, rest = tokens[0], tokens[1:]

    if tok.lower() == "now":
        return now, "now", rest

    # -12m : that long ago. Bare numbers here are MINUTES, matching <length>.
    if tok.startswith("-"):
        d = _parse_duration(tok[1:])
        if d is None:
            raise ValueError(f"bad relative time {tok!r} (try -12m, -90s, -1h20m)")
        return now - d, f"{_fmt_dur(d)} ago", rest

    # @1:23:45 : offset into the broadcast, the number a VOD player shows.
    if tok.startswith("@"):
        off = _parse_clock(tok[1:])
        if off is None:
            raise ValueError(f"bad stream offset {tok!r} (use @HH:MM:SS)")
        if stream_zero is None:
            raise ValueError("broadcast start unknown for this stream — "
                             "use a wall-clock time or -Nm instead")
        return stream_zero + off, f"broadcast +{_fmt_hms(off)}", rest

    # 2026.08.20 21:42  /  2026.08.20T21:42  /  2026.08.20_21:42
    datepart = timepart = None
    m = _DATE_RE.match(tok)
    if m:
        datepart = m.groups()
        if rest and _CLOCK_RE.match(rest[0]):
            timepart, rest = rest[0], rest[1:]
        else:
            raise ValueError(f"date {tok!r} needs a time after it "
                             f'(quote it: "{tok} 21:42")')
    else:
        for sep in ("T", "t", "_"):
            if sep in tok:
                a, _, b = tok.partition(sep)
                m2 = _DATE_RE.match(a)
                if m2 and _CLOCK_RE.match(b):
                    datepart, timepart = m2.groups(), b
                    break

    if datepart and timepart:
        y, mo, d = (int(x) for x in datepart)
        dt = _local_dt(datetime.date(y, mo, d), _parse_clock(timepart))
        return dt.timestamp(), dt.strftime("%Y-%m-%d %H:%M:%S"), rest

    # 21:42 : today, local.
    secs = _parse_clock(tok)
    if secs is not None:
        dt = _local_dt(datetime.date.fromtimestamp(now), secs)
        ep = dt.timestamp()
        # A clock time in the future means the broadcast crossed midnight and
        # you are naming a moment from last night, not one from this evening.
        if ep > now + 60:
            ep -= 86400
            dt = datetime.datetime.fromtimestamp(ep)
        return ep, dt.strftime("%Y-%m-%d %H:%M:%S"), rest

    raise ValueError(
        f"unrecognised time {tok!r} — use -12m, 21:42, @1:23:45, "
        f'or "2026.08.20 21:42"')


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
        self.recorded_keys: set[str] = set()

        # State
        self.was_streaming = False
        self.monitoring_cooldown_until = None
        self.manual_termination_in_progress = False
        self.last_check_time: dict[str, datetime.datetime | None] = {
            "youtube": None, "twitch": None,
        }

        # Filesystem
        Path(self.config["output"]).mkdir(parents=True, exist_ok=True)
        Path(self._clips_dir()).mkdir(parents=True, exist_ok=True)
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
                        os.killpg(os.getpgid(vp.pid), signal.SIGINT)
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
        # shlex, not split(): a quoted datetime ("2026.08.20 21:42") and a
        # multi-word --name have to survive the trip over the socket intact.
        try:
            parts = shlex.split(command)
        except ValueError:
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
        if cmd == "mark":
            return self._cmd_mark(parts[1:])
        if cmd == "clip":
            return self._cmd_clip(parts[1:])
        return ("Commands: status | tail [YT|TW] | check [youtube|twitch] | "
                "record <url> | watch <url> | unwatch [url|N] | "
                "mark <text> | clip <when> [length]")

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
            ("YT", self.config["youtube_handle"],
             f"{self._platform_interval('youtube')}s"
             + (" +cookies" if ls_common.youtube_anon_blocked() else "")),
            ("TW", self.config["twitch_user"], f"{self._platform_interval('twitch')}s"),
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

        # Log extension depends on which recorder is in use.
        part_num = stream.get("_current_part_num")
        if part_num is None:
            return "No active part for this recording yet."
        log_path = os.path.join(
            self.config["output"],
            f"{stream['stream_title']}.part{part_num:02d}.log",
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
            result = self._make_stream_info(platform, video_id, title, url,
                                            data=data)
            idx, dual = self._get_stream_index(platform, datetime.datetime.now())
            self._start_recording(result, idx, dual)
            return f"✔ LIVE — recording: {title} (#{idx:03d})"

        # Not live → add to watch list
        entry: dict = {"title": title, "last_check": time.time()}
        release_ts = ls_common.stream_start_epoch(data)
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
            release_ts = ls_common.stream_start_epoch(data)
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
            # The channel, and NOT a VOD link — because there is no VOD yet.
            #
            # This probes twitch.tv/<user> while she is live, so yt-dlp's Twitch
            # extractor reports the STREAM id: the broadcast session. Twitch
            # mints the VOD when the broadcast ends, with a different number.
            # This line used to write `.../videos/<stream id>`, which is a URL
            # that has never resolved to anything, and every downstream reader
            # then trusted it as the VOD id — the archive keyed its capture on
            # it, the Obsidian entry carried it, and ls-audit read it back out
            # of that entry in preference to the Helix cache that knew better.
            #
            # The stream id is not lost: it stays in the recording's filename,
            # `[{video_id}]`, which is where the id resolution reads it from and
            # where find_vod_by_stream_id turns it into the real VOD id once the
            # broadcast has ended. ls-audit rewrites this line with the true
            # watch URL on its next pass.
            #
            # The `.lstrip('v')` that used to be here was the tell: only
            # yt-dlp's VOD extractor emits `v123`, so this line was written for
            # a case it is never given.
            obsidian_url = svc["url"]

        return self._make_stream_info(
            platform, video_id, title, stream_url, obsidian_url, data=data,
        )

    def _make_stream_info(self, platform, video_id, title, stream_url,
                          obsidian_url=None, data=None):
        """Build the info dict consumed by _start_recording."""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        stream_title = sanitize_filename(f"{title} [{video_id}] @ {timestamp}")
        if obsidian_url is None:
            obsidian_url = (f"https://www.youtube.com/watch?v={video_id}"
                            if platform == "youtube" else stream_url)
        # Absolute broadcast start, epoch SECONDS, off the live probe. This is
        # the only moment it is available: the probe payload is discarded after
        # this call, and for Twitch it cannot be recovered once the stream ends.
        return {
            "platform":       platform,
            "video_id":       video_id,
            "stream_url":     stream_url,
            "stream_title":   stream_title,
            "obsidian_title": title,
            "obsidian_url":   obsidian_url,
            "stream_key":     f"{platform}_{video_id}",
            "stream_start_epoch": ls_common.stream_start_epoch(data) if data else None,
        }

    def _check_streams(self):
        if not self._is_monitoring_allowed():
            return
        now = datetime.datetime.now()
        for platform in ("youtube", "twitch"):
            last = self.last_check_time.get(platform)
            if last and (now - last).total_seconds() < self._platform_interval(platform):
                continue
            result = self._probe_platform(platform)
            if (result and result["stream_key"] not in self.active_streams and result["stream_key"] not in self.recorded_keys):
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
                release_ts = ls_common.stream_start_epoch(data)
                if release_ts:
                    entry["start_time"] = release_ts
                continue

            # Stream went live
            platform = "twitch" if "twitch.tv" in url else "youtube"
            title = data.get("fulltitle") or data.get("title") or "Unknown"
            video_id = data.get("id", "unknown")
            if f"{platform}_{video_id}" in self.active_streams:
                continue

            result = self._make_stream_info(platform, video_id, title, url,
                                            data=data)
            idx, dual = self._get_stream_index(platform, datetime.datetime.now())
            logger.info(f"Watched stream live: {title}")
            self._start_recording(result, idx, dual)
            del self.watch_list[url]

    # ── recording ─────────────────────────────────────────────────────────

    def _platform_interval(self, platform: str) -> int:
        return int(self.config.get(f"check_interval_{platform}")
                   or self.config.get("check_interval", 60))

    def _dual_window(self) -> int:
        """Must exceed the slowest poll interval, or a lenient YouTube check
        misses the pairing and allocates a second obsidian index."""
        if self.config.get("dual_stream_window"):
            return int(self.config["dual_stream_window"])
        return (self.config["dual_stream_cycle"] * self.config["check_interval"]
                + max(self._platform_interval(p) for p in ("youtube", "twitch")))

    def _get_stream_index(self, platform: str,
                          start_time: datetime.datetime) -> tuple[int, bool]:
        """Get obsidian index, detecting dual-stream to share an index."""
        window = self._dual_window()
        other = "twitch" if platform == "youtube" else "youtube"
        for s in self.active_streams.values():
            if s["platform"] == other:
                diff = abs((start_time - s["start_time"]).total_seconds())
                if diff <= window:
                    return s["obsidian_index"], True
        return ls_common.obsidian_next_index(self.config), False

    def _start_recording(self, info: dict, obsidian_index: int, is_dual: bool):
        """Create obsidian + cache entries, init recording state, spawn video + chat."""
        platform       = info["platform"]
        video_id       = info["video_id"]
        obsidian_title = info["obsidian_title"]
        obsidian_url   = info["obsidian_url"]

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

        # One now() shared by the cache entry and the chat recorder's zero,
        # so record_start_epoch_ms is exactly the chat zero and not a second
        # reading taken a few lines later.
        record_start = datetime.datetime.now()
        stream_start = info.get("stream_start_epoch")

        cache = ls_common.load_cache()
        channel = (self.config["youtube_handle"] if platform == "youtube"
                else self.config["twitch_user"])
        ls_common.upsert_vod(cache, {
            "id":             video_id,
            "platform":       platform,
            "title":          obsidian_title,
            "start_time":     record_start.isoformat(),
            "channel":        channel,
            "obsidian_index": obsidian_index,
            # ── chat sync ────────────────────────────────────────────────
            # start_time is NOT usable for this: it holds the record start
            # here, but --refresh later overwrites it with the broadcast
            # start derived from release_timestamp. These two are unambiguous
            # and upsert_vod skips None, so a failed probe leaves no key
            # rather than a misleading zero.
            "record_start_epoch_ms": int(record_start.timestamp() * 1000),
            "stream_start_epoch_ms": stream_start * 1000 if stream_start else None,
        })
        ls_common.save_cache(cache)

        # The archive gets the same two numbers, and it is the only place they
        # are ever written down permanently. Never fatal: a failure here queues
        # and the recording carries on.
        try:
            ls_archive.post_start(
                self.config,
                platform=platform,
                video_id=video_id,
                title=obsidian_title,
                url=obsidian_url,
                obsidian_index=obsidian_index,
                broadcast_started_at=stream_start,
                record_started_at=int(record_start.timestamp()),
            )
        except Exception as e:
            logger.warning(f"archive start packet failed: {e}")

        stream_title = f"{obsidian_index:03d}_{info['stream_title']}"
        stream_key   = f"{platform}_{video_id}"
        self.recorded_keys.add(stream_key)

        self.active_streams[stream_key] = {
            "url":             info["stream_url"],
            "platform":        platform,
            "identifier":      video_id,
            "stream_title":    stream_title,
            "obsidian_title":  obsidian_title,
            "start_time":      record_start,
            "video_process":   None,
            "chat_thread":     None,
            "chat_stop_event": None,
            "obsidian_index":  obsidian_index,
            # Health & watchdog
            "_samples":            deque(maxlen=SAMPLE_WINDOW),
            "_last_size":          0,
            "_last_growth_ts":     time.time(),
            "_bitrate_bps":        None,
            "_watchdog_triggered": False,
            "_restart_count":      0,
            # Recording lifecycle
            "_from_start":       (platform == "youtube"),
            "_part_num":         0,      # Twitch: incremented per part. from-start: pinned to 1.
            "_current_part_num": None,
            # Clip timebase. record_start is when WE started pulling bytes;
            # stream_start is when the broadcast began. They are not the same
            # number and the difference is exactly what makes a naive
            # "00:08:30 into the stream" point at the wrong frame -- Twitch
            # can be a full check_interval late, YouTube from-start is ~0.
            "_record_start_epoch": record_start.timestamp(),
            "_stream_start_epoch": float(stream_start) if stream_start else None,
            "_part_history":       [],
        }
        self._record_video(stream_key)
        self._record_chat(stream_key)

        # Don't make the badge wait for the next tick. The poll loop would get
        # to it within check_interval anyway, but "she went live" is exactly the
        # moment anyone is looking at the page, and a minute of dark badge while
        # the stream is plainly running is the one lag people notice.
        ls_archive.post_live(self.config, self.active_streams)

    def _record_video(self, stream_key: str):
        """Spawn yt-dlp for this stream.

        Twitch (live-edge): each call writes a fresh `<title>.partNN.<ext>`, so
        failure restarts produce new files that are concatenated at completion.

        YouTube (--live-from-start): always part01. A restart re-invokes yt-dlp
        against the SAME output template so it resumes its own download from
        .ytdl state, rather than re-pulling from the broadcast start.
        """
        stream = self.active_streams.get(stream_key)
        if stream is None:
            return

        url      = stream["url"]
        platform = stream["platform"]
        title    = stream["stream_title"]

        if stream["_from_start"]:
            part_num = 1
            stream["_part_num"] = 1
        elif stream.get("_retry_part"):
            # A failed restart is the same part again, not the next one.
            # Advancing here logged "restart 1/10" beside "Part 03 started"
            # and left a trail of empty parts for the concat to pick up.
            stream["_retry_part"] = False
            part_num = stream["_part_num"]
        else:
            stream["_part_num"] += 1
            part_num = stream["_part_num"]
        stream["_current_part_num"] = part_num
        self._note_part_start(stream, part_num)

        output_template = f"{title}.part{part_num:02d}.%(ext)s"
        log_path = os.path.join(
            self.config["output"], f"{title}.part{part_num:02d}.log",
        )

        try:
            cmd = ls_common.ytdlp_live_cmd(
                self.config, url, platform, output_template, 
                from_start=stream["_from_start"]
            )
            log_fh = open(log_path, "ab", buffering=0)
            process = subprocess.Popen(
                cmd, cwd=self.config["output"],
                stdout=log_fh, stderr=subprocess.STDOUT,
                start_new_session=True
            )
            stream["video_process"]       = process
            stream["_video_log_fh"]       = log_fh
            stream["_last_growth_ts"]     = time.time()
            stream["_watchdog_triggered"] = False
            stream["_part_started_ts"]    = time.time()
            logger.info(
                f"Part {part_num:02d} started: {title} "
                f"(PID {process.pid}, log: {os.path.basename(log_path)})"
            )

            threading.Thread(
                target=self._video_monitor,
                args=(stream_key, process, log_fh, title, part_num),
                daemon=True,
            ).start()
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
            def run():
                acc = os.path.join(self.config["output"], f"{title}.live_chat.json")

                def fold(segbase: str) -> bool:
                    if not ls_common.merge_chat_fragments(self.config["output"], segbase):
                        return False
                    segjson = os.path.join(self.config["output"], f"{segbase}.json")
                    try:
                        with open(acc, "a", encoding="utf-8") as o, \
                             open(segjson, encoding="utf-8") as s:
                            o.write(s.read())
                        os.remove(segjson)
                    except Exception as e:
                        logger.error(f"Chat fold failed ({segbase}): {e}")
                    return True

                seg, idle = 0, 0
                IDLE_CAP = 5
                while not stop_event.is_set() and idle < IDLE_CAP:
                    seg += 1
                    log_path = os.path.join(
                        self.config["output"], f"{title}.chatseg{seg:03d}.log")
                    log_fh = None
                    started = time.time()
                    try:
                        cmd = ls_common.ytdlp_chat_cmd(
                            self.config, stream["url"], f"{title}.chatseg{seg:03d}.%(ext)s",
                        )
                        # A log file, NOT subprocess.PIPE: nothing drains the
                        # pipes, so once the 64KB buffer fills yt-dlp blocks
                        # mid-write. It also threw away the only evidence of
                        # why a segment came back empty.
                        log_fh = open(log_path, "ab", buffering=0)
                        proc = subprocess.Popen(
                            cmd, cwd=self.config["output"],
                            stdout=log_fh, stderr=subprocess.STDOUT,
                        )
                        while proc.poll() is None:
                            if stop_event.is_set():
                                proc.terminate()
                                try: proc.wait(timeout=5)
                                except subprocess.TimeoutExpired: proc.kill()
                                break
                            time.sleep(0.5)
                    except Exception as e:
                        logger.error(f"Chat error (seg {seg:03d}): {e}")
                    finally:
                        if log_fh:
                            try: log_fh.close()
                            except Exception: pass

                    got = fold(f"{title}.chatseg{seg:03d}")
                    idle = 0 if got else idle + 1
                    logger.info(
                        f"Chat seg {seg:03d} ran {time.time() - started:.0f}s, "
                        f"{'produced chat' if got else f'EMPTY ({idle}/{IDLE_CAP})'}: "
                        f"{title}")
                    if not got:
                        # Keep the log of an empty segment; it is the only
                        # record of why. Successful segments' logs are noise.
                        pass
                    elif os.path.exists(log_path):
                        try: os.remove(log_path)
                        except OSError: pass

                    if stop_event.is_set():
                        break
                    if not self._source_still_live(stream):
                        logger.info(f"Chat: {title} no longer live; stopping")
                        break
                    time.sleep(30)

                if idle >= IDLE_CAP:
                    logger.warning(
                        f"Chat gave up after {IDLE_CAP} empty segments while "
                        f"still live: {title}. See .chatseg*.log for why.")

                ls_common.merge_chat_fragments(self.config["output"], title)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()
        stream["chat_thread"] = thread
        logger.info(f"Chat recording started: {title}")

    # ── monitor / watchdog ────────────────────────────────────────────────

    def _source_still_live(self, stream: dict, attempts: int = 3,
                           default: bool = True) -> bool:
        """
        Only believe "ended" when a probe actually says so.

        An offline channel is a definitive answer, not a failed probe. Without
        that distinction a Twitch stream could never be seen to end: yt-dlp
        exits non-zero saying "is offline", the probe returned None, and three
        Nones read as inconclusive -- so the recorder rotated into a broadcast
        that had already finished and burned its whole restart budget.

        `default` is what a genuinely inconclusive result means, and it differs
        by caller: the chat loop keeps capturing (cheap), the video rotate does
        not (a live-edge part against an ended stream re-downloads the VOD).
        """
        for i in range(attempts):
            data, reason = ls_common.ytdlp_probe(
                self.config, stream["url"], playlist_items="1", with_reason=True)
            if data and data.get("id") == stream["identifier"]:
                return bool(data.get("is_live"))
            if reason == "offline":
                return False                    # the platform said so
            if i < attempts - 1:
                time.sleep(20)
        logger.warning(f"Liveness probe inconclusive x{attempts} for "
                       f"{stream['stream_title']}; assuming "
                       f"{'still live' if default else 'ended'}")
        return default

    def _video_monitor(self, stream_key, process, log_fh, title, part_num):
        """Wait for yt-dlp exit, then classify: superseded / natural-end / failure."""
        process.wait()
        rc = process.returncode
        try:
            log_fh.close()
        except Exception:
            pass

        stream = self.active_streams.get(stream_key)
        if stream is None:
            return

        # Stale-monitor guard: a newer process already supersedes this one.
        if stream.get("video_process") is not process:
            logger.info(f"Part {part_num:02d} superseded (rc={rc}): {title}")
            return

        if self.manual_termination_in_progress:
            logger.info(f"Part {part_num:02d} stopped (manual): {title}")
            self._handle_completion(stream_key)
            self._mark_termination_finished_if_idle()
            return

        if rc == 0:
            if not self.manual_termination_in_progress and self._source_still_live(stream):
                elapsed = time.time() - stream.get("_part_started_ts", 0)
                if elapsed < 30:
                    # Pathological: rc=0 within seconds while still live = the
                    # 642 "already downloaded" loop. Charge the budget and back
                    # off so it can't spin; give up after RESTART_MAX.
                    stream["_restart_count"] = stream.get("_restart_count", 0) + 1
                    if stream["_restart_count"] >= RESTART_MAX:
                        logger.error(f"Part {part_num:02d} rc=0 instant-looping {RESTART_MAX}x; stopping: {title}")
                        self._handle_completion(stream_key)
                        return
                    logger.warning(f"Part {part_num:02d} rc=0 after {elapsed:.0f}s, still live; backoff {stream['_restart_count']}/{RESTART_MAX}: {title}")
                    time.sleep(RESTART_DELAY_S)
                else:
                    # Healthy-length part that ended while still live: genuine
                    # resume, reset the budget.
                    stream["_from_start"] = False
                    stream["_restart_count"] = 0
                    logger.warning(f"Part {part_num:02d} ended rc=0 but {title} still live; rotating to live-edge")
                    time.sleep(3)
                if stream_key in self.active_streams and not self.manual_termination_in_progress:
                    self._record_video(stream_key)
                return
            logger.info(f"Part {part_num:02d} complete (rc=0): {title}")
            self._handle_completion(stream_key)
            return
        
        # Non-zero: restart within the same recording session if budget allows
        restart_count = stream.get("_restart_count", 0)
        if restart_count >= RESTART_MAX:
            logger.error(
                f"Recording failed after {RESTART_MAX} restarts "
                f"(rc={rc}, part {part_num:02d}): {title}"
            )
            # The chat is a separate process and may be perfectly healthy.
            # Completing here kills it, which is how a YouTube capture that
            # could not get video threw away a whole stream's chat three
            # minutes in. Video and chat fail independently; let chat run on
            # and finish the capture when the broadcast actually ends.
            chat = stream.get("chat_thread")
            if chat and chat.is_alive() and self._source_still_live(stream):
                logger.warning(
                    f"Video abandoned but {title} is still live; chat keeps "
                    f"recording until the stream ends")
                stream["_video_abandoned"] = True
                threading.Thread(target=self._wait_chat_then_complete,
                                 args=(stream_key,), daemon=True).start()
                return
            self._handle_completion(stream_key)
            return

        stream["_restart_count"] = restart_count + 1
        stream["_retry_part"] = True
        logger.warning(
            f"yt-dlp exited rc={rc}, restart "
            f"{restart_count + 1}/{RESTART_MAX} in {RESTART_DELAY_S}s: "
            f"{title} part {part_num:02d}"
        )
        time.sleep(RESTART_DELAY_S)

        if self.manual_termination_in_progress:
            self._handle_completion(stream_key)
            self._mark_termination_finished_if_idle()
            return
        if stream_key not in self.active_streams:
            return

        self._record_video(stream_key)

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

    def _current_growing_file(self, title: str, part_num: int) -> str | None:
        """Largest non-sidecar file matching this part's prefix.

        Under --live-from-start the file that grows mid-recording is yt-dlp's
        in-progress fragment (`<title>.partNN.f<code>.<ext>`), not the merged
        `<title>.partNN.<ext>` (which only exists after yt-dlp finishes). For
        Twitch live-edge it's the part file itself. Taking the largest match
        works for both without hard-coding yt-dlp's fragment naming.
        """
        pattern = os.path.join(
            self.config["output"], f"{glob.escape(title)}.part{part_num:02d}*",
        )
        best, best_size = None, -1
        for p in glob.glob(pattern):
            base = os.path.basename(p)
            # NB: keep `.part` — under --live-from-start the growing format file
            # is `<title>.partNN.f<code>.<ext>.part` until that format completes.
            if base.endswith((".log", ".ytdl", ".json", ".concat.txt",
                              ".frag.json")):
                continue
            try:
                sz = os.path.getsize(p)
            except OSError:
                continue
            if sz > best_size:
                best, best_size = p, sz
        return best

    def _sample_stream(self, stream: dict, now: float):
        """Sample the growing file's size for the watchdog and bitrate probe."""
        title    = stream["stream_title"]
        part_num = stream.get("_current_part_num")
        if part_num is None:
            return

        file_path = self._current_growing_file(title, part_num)
        if not file_path:
            return

        try:
            size = os.path.getsize(file_path)
        except OSError:
            return

        samples   = stream["_samples"]
        last_size = stream.get("_last_size", 0)
        samples.append((now, size))
        if size > last_size:
            stream["_last_growth_ts"]     = now
            stream["_watchdog_triggered"] = False
        stream["_last_size"] = size

        # Bitrate: probe once across the entire recording, not per part
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
            threading.Thread(target=self._stop_process, args=(vp,), kwargs={"timeout": 10}, daemon=True).start()
        except Exception as e:
            logger.error(f"Watchdog terminate failed: {e}")

    def _find_part_files(self, stream_title: str) -> list[str]:
        output_dir = self.config["output"]
        pattern    = os.path.join(
            output_dir, f"{glob.escape(stream_title)}.part*.*"
        )
        parts: list[str] = []
        for p in sorted(glob.glob(pattern)):
            base = os.path.basename(p)
            if base.endswith((".log", ".part", ".ytdl", ".frag.json", ".temp")):
                continue
            # yt-dlp per-format intermediates: .partNN.f299.mp4, .partNN.f299-dash.mp4, .partNN.f140.m4a
            if re.search(r"\.part\d{2}\.f\d+(-\w+)?\.\w+$", base):
                continue
            if os.path.splitext(p)[1].lower() in ls_common.VIDEO_EXTS:
                parts.append(p)
        return parts

    def _cleanup(self, paths: list[str]):
        for p in paths:
            try:
                os.remove(p)
            except OSError:
                pass

    def _merge_parts(self, parts: list[str], dest_mp4: str) -> tuple[bool, float | None]:
        if not parts:
            return False, None

        # Single part: YouTube from-start is already a merged mp4; Twitch wrote one file.
        if len(parts) == 1:
            src = parts[0]
            if src.lower().endswith(".mp4"):
                if os.path.abspath(src) != os.path.abspath(dest_mp4):
                    os.replace(src, dest_mp4)
            else:  # Twitch .ts → remux to mp4 (stream copy, no re-encode)
                r = subprocess.run(["ffmpeg", "-y", "-i", src, "-c", "copy", dest_mp4],
                                capture_output=True, text=True, timeout=1800)
                if not (os.path.exists(dest_mp4) and ls_common.probe_duration(dest_mp4)):
                    logger.error(f"Remux failed: {r.stderr[-300:]}")
                    return False, None      # leave src in place for recovery
                self._cleanup([src])
            dur = ls_common.probe_duration(dest_mp4)
            return (dur is not None), dur

        # Multi-part (Twitch restart recovery only): concat, stream copy.
        list_file = dest_mp4 + ".concat.txt"
        with open(list_file, "w") as f:
            for p in parts:
                f.write("file '%s'\n" % p.replace("'", r"'\''"))
        r = subprocess.run(["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_file,
                            "-c", "copy", dest_mp4], capture_output=True, text=True, timeout=1800)
        dur = ls_common.probe_duration(dest_mp4)
        if os.path.exists(dest_mp4) and dur:
            self._cleanup(parts + [list_file])
            logger.info(f"Merged {len(parts)} parts → {os.path.basename(dest_mp4)}")
            return True, dur
        logger.error(f"Concat failed (rc={r.returncode}): {r.stderr[-300:]}")
        self._cleanup([list_file])           # leave parts in place for recovery
        return False, None


    # ══════════════════════════════════════════════════════════════════════
    #  CLIPPING
    # ══════════════════════════════════════════════════════════════════════
    #
    #  The bytes are already on local disk, so a clip is one ffmpeg stream
    #  copy: no network, no auth, no second download. Clipping therefore
    #  keeps working on days the extractor does not.
    #
    #  Clips go in their own directory, and that is load-bearing rather than
    #  tidiness. _current_growing_file() globs `<title>.partNN*` and takes
    #  the LARGEST match, so a clip left in the output dir under a matching
    #  prefix becomes the file the watchdog samples -- it would see no growth
    #  and force-restart a healthy recording. _find_part_files() would then
    #  sweep the same clip into the final concat.

    def _clips_dir(self) -> str:
        return (self.config.get("clips_dir")
                or os.path.join(self.config["output"], "clips"))

    # ── part timebase ─────────────────────────────────────────────────────

    def _note_part_start(self, stream: dict, part_num: int):
        """Record the wall time this part's file begins.

        `zero_epoch` is the wall time of media t=0 in the file being written:
        the broadcast start under --live-from-start (the file opens at the
        beginning of the stream), otherwise the moment yt-dlp was spawned
        (the file opens at the live edge).
        """
        hist = stream.setdefault("_part_history", [])
        now  = time.time()
        from_start = bool(stream.get("_from_start"))
        entry = {
            "num":        part_num,
            "started_ts": now,
            "from_start": from_start,
            "zero_epoch": (stream.get("_stream_start_epoch") or now)
                          if from_start else now,
        }
        last = hist[-1] if hist else None
        if last and last["num"] == part_num and last["from_start"] == from_start:
            # Same part twice: --live-from-start resumes its own download from
            # .ytdl state and keeps its timebase; a Twitch retry rewrites the
            # part, so the timebase moves with it.
            if not from_start:
                hist[-1] = entry
            return
        hist.append(entry)

    def _resolve_part(self, stream: dict, epoch: float) -> tuple[dict, float] | None:
        """Absolute epoch -> (part entry, seconds into that part's file)."""
        hist = stream.get("_part_history") or []
        found = None
        for i, part in enumerate(hist):
            if epoch < part["zero_epoch"]:
                continue
            upper = hist[i + 1]["started_ts"] if i + 1 < len(hist) else None
            if upper is None or epoch < upper:
                found = (part, epoch - part["zero_epoch"])
        return found

    # ── source files ──────────────────────────────────────────────────────

    def _classify_media(self, path: str) -> str:
        """'muxed' | 'video' | 'audio' — what streams this file carries."""
        try:
            r = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "stream=codec_type",
                 "-of", "csv=p=0", path],
                capture_output=True, text=True, timeout=30,
            )
            types = {t.strip() for t in r.stdout.split() if t.strip()}
            if "video" in types and "audio" in types:
                return "muxed"
            if "video" in types:
                return "video"
            if "audio" in types:
                return "audio"
        except Exception:
            pass
        # ffprobe can refuse a half-written fragment; fall back to the name.
        base = os.path.basename(path).lower().removesuffix(".part")
        return "audio" if base.endswith((".m4a", ".opus", ".ogg", ".aac", ".mp3")) else "muxed"

    def _part_sources(self, title: str, part_num: int) -> list[tuple[str, str]]:
        """Media file(s) holding this part, as (path, kind).

        Twitch live-edge writes one file. YouTube --live-from-start writes
        video and audio as separate `.fNNN.` streams and only merges them when
        yt-dlp exits, so mid-recording there are two and the clip has to mux
        them back together itself.
        """
        pattern = os.path.join(
            self.config["output"], f"{glob.escape(title)}.part{part_num:02d}*")
        merged, frags = [], []
        for p in glob.glob(pattern):
            base = os.path.basename(p)
            if base.endswith((".log", ".ytdl", ".json", ".concat.txt",
                              ".frag.json", ".temp")):
                continue
            try:
                if os.path.getsize(p) <= 0:
                    continue
            except OSError:
                continue
            stem = base.removesuffix(".part")
            if re.search(r"\.part\d{2}\.f\d+(-\w+)?\.\w+$", stem):
                frags.append(p)
            elif re.fullmatch(rf"{re.escape(title)}\.part{part_num:02d}\.\w+", stem):
                merged.append(p)

        # A finished part beats the fragments it was built from.
        if merged:
            best = max(merged, key=os.path.getsize)
            return [(best, self._classify_media(best))]
        if not frags:
            return []

        picked: dict[str, tuple[str, int]] = {}
        for p in frags:
            kind = self._classify_media(p)
            size = os.path.getsize(p)
            if kind not in picked or size > picked[kind][1]:
                picked[kind] = (p, size)
        if "muxed" in picked:
            return [(picked["muxed"][0], "muxed")]
        return [(picked[k][0], k) for k in ("video", "audio") if k in picked]

    # ── the cut ───────────────────────────────────────────────────────────

    def _cut(self, sources: list[tuple[str, str]], offset: float,
             length: float, out_path: str) -> bool:
        """ffmpeg stream copy, on a worker thread. Input-side -ss to seek cheap.

        -c copy can only start on a keyframe, so the real start snaps back by
        up to one GOP (~2s Twitch, up to ~5s YouTube DASH). That is what the
        lead is for; re-encoding to land the frame exactly would cost minutes
        of CPU on a box already running two captures.
        """
        args = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y"]
        maps: list[str] = []
        for i, (path, kind) in enumerate(sources):
            args += ["-ss", f"{offset:.3f}", "-i", path]
            if kind in ("video", "muxed"):
                maps += ["-map", f"{i}:v:0?"]
            if kind in ("audio", "muxed"):
                maps += ["-map", f"{i}:a:0?"]
        args += maps + [
            "-t", f"{length:.3f}", "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart", out_path,
        ]
        name    = os.path.basename(out_path)
        started = time.time()
        try:
            r = subprocess.run(args, capture_output=True, text=True,
                               timeout=CLIP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            logger.error(f"Clip timed out: {name}")
            return False
        took = time.time() - started
        got  = (ls_common.probe_duration(out_path)
                if os.path.exists(out_path) else None)
        if not got:
            logger.error(f"Clip failed: {name}: "
                         f"{(r.stderr or 'no output').strip()[-300:]}")
            return False
        # A short clip is not a failure: the recording simply does not reach
        # that far yet. Say so rather than pretending it is complete.
        short = f" (short: {_fmt_dur(got)} of {_fmt_dur(length)})" if got < length * 0.8 else ""
        logger.info(f"Clip written: {name} — {_fmt_dur(got)}, {took:.1f}s{short}")
        return True

    def _clip_out_path(self, stream: dict, start_epoch: float,
                       length: float, label: str | None) -> str:
        idx  = stream.get("obsidian_index")
        when = datetime.datetime.fromtimestamp(start_epoch).strftime("%Y-%m-%d_%H-%M-%S")
        bits = [f"{idx:03d}" if idx else stream["platform"], when, _fmt_dur(length)]
        if label:
            clean = sanitize_filename(label, restricted=True).strip("_")
            if clean:
                bits.append(clean[:60])
        base = "_".join(bits)
        path = os.path.join(self._clips_dir(), f"{base}.mp4")
        n = 2
        while os.path.exists(path):
            path = os.path.join(self._clips_dir(), f"{base}_{n}.mp4")
            n += 1
        return path

    # ── command ───────────────────────────────────────────────────────────

    def _pick_stream(self, platform: str | None) -> tuple[dict | None, str]:
        cands = [s for s in self.active_streams.values()
                 if platform is None or s["platform"] == platform]
        if not cands:
            if platform:
                return None, f"No active {platform} recording."
            return None, ("No active recording — `clip` and `mark` both work "
                          "on a live capture only.")
        # Twitch first: it records at the live edge, so its file always holds
        # the moment you just watched. A from-start YouTube capture can be
        # minutes behind and simply not have those frames yet.
        cands.sort(key=lambda s: s["platform"] != "twitch")
        return cands[0], ""

    def _cmd_mark(self, argv: list[str]) -> str:
        """ls-rec mark <text> [--tw|--yt]

        Writes a `!`-prefixed note into the stream's Obsidian entry, stamped
        with both timebases: the broadcast time a player shows, and the offset
        into the file we actually captured. Those differ by however late the
        daemon noticed the stream — up to a full check interval on Twitch —
        so writing only one of them would make the note readable in exactly
        one context and wrong in the other.
        """
        platform, words = None, []
        for a in argv:
            if a in ("--tw", "--twitch"):
                platform = "twitch"
            elif a in ("--yt", "--youtube"):
                platform = "youtube"
            else:
                words.append(a)
        text = " ".join(words).strip()
        if not text:
            return '\n  Usage: ls-rec mark "what happened" [--tw|--yt]\n'
        if text.startswith("["):
            # `![[...]]` is an Obsidian embed, not a note. Refusing beats
            # silently planting a broken transclusion in the vault.
            return "\n  Note cannot start with '[' — it would become an embed.\n"

        stream, err = self._pick_stream(platform)
        if stream is None:
            return f"\n  {err}\n"
        idx = stream.get("obsidian_index")
        if not idx:
            return "\n  That recording has no Obsidian entry to mark.\n"

        now  = time.time()
        rec0 = stream.get("_record_start_epoch")
        brd0 = stream.get("_stream_start_epoch")
        if not rec0:
            return "\n  No recording timebase for that stream.\n"
        rec   = _fmt_hms(now - rec0)
        brd   = _fmt_hms(now - brd0) if brd0 else None
        stamp = f"{brd} / r{rec}" if brd else f"r{rec}"
        note  = f"!{text} ({stamp})"

        if not ls_common.obsidian_append_note(self.config, idx, note):
            return f"\n  Could not write to Obsidian entry {idx:03d}.\n"
        logger.info(f"Mark on {stream['stream_title']}: {note}")

        out = ["", f"  {idx:03d}  {note}"]
        if not brd:
            out.append("  (broadcast start unknown — recording-relative only)")
        out += [f"  cut it with:  ls-rec clip @{brd or rec}", ""]
        return "\n".join(out)

    def _cmd_clip(self, argv: list[str]) -> str:
        """ls-rec clip <when> [length] [--tw|--yt] [--lead S] [--name T]"""
        platform = name = lead = None
        do_all = False
        pos: list[str] = []
        i = 0
        try:
            while i < len(argv):
                a = argv[i]
                if a in ("--tw", "--twitch"):
                    platform = "twitch"
                elif a in ("--yt", "--youtube"):
                    platform = "youtube"
                elif a in ("--all", "-a"):
                    do_all = True
                elif a in ("--lead", "-l"):
                    i += 1
                    lead = _parse_duration(argv[i], bare_seconds=True)
                    if lead is None:
                        return f"\n  Bad --lead value: {argv[i]!r}\n"
                elif a in ("--name", "-n"):
                    i += 1
                    name = argv[i]
                elif a.startswith("-") and not _looks_like_time(a):
                    return f"\n  Unknown option: {a}\n"
                else:
                    pos.append(a)
                i += 1
        except IndexError:
            return f"\n  Bad arguments: {' '.join(argv)}\n"

        if do_all and name:
            return "\n  --name has no meaning with --all (each note names its own).\n"
        if not pos and not do_all:
            return ("\n  Usage: ls-rec clip <when> [length] [--tw|--yt]\n"
                    "         ls-rec clip --all [length]\n"
                    "    -12m            12 minutes ago\n"
                    "    21:42           today, local time\n"
                    "    @1:23:45        that far into the broadcast\n"
                    '    "2026.08.20 21:42"\n')

        stream, err = self._pick_stream(platform)
        if stream is None:
            return f"\n  {err}\n"
        if not stream.get("_part_history"):
            return "\n  That recording has not written anything yet.\n"

        now = time.time()
        if lead is None:
            lead = float(self.config.get("clip_lead_s", 60))

        if do_all:
            all_len = None
            if pos:
                all_len = _parse_duration(pos[0])
                if all_len is None:
                    return f"\n  Bad length {pos[0]!r}.\n"
            return self._clip_all(stream, all_len, lead)

        brd = stream.get("_stream_start_epoch") or stream.get("_record_start_epoch")
        try:
            epoch, desc, pos = _parse_when(pos, now, brd)
        except ValueError as e:
            return f"\n  {e}\n"

        if pos:
            length = _parse_duration(pos[0])
            if length is None:
                return (f"\n  Bad length {pos[0]!r} — use 5 (minutes), 90s, "
                        f"5m, 1h30m or 00:05:00.\n")
        else:
            length = float(self.config.get("clip_length_s", 180))
        if length <= 0:
            return "\n  Length must be positive.\n"

        plan, err = self._plan_clip(stream, epoch, length, lead, name, now)
        if plan is None:
            # _plan_clip phrases reasons lowercase so --all can inline them
            # after an em dash; standing alone they want a capital.
            return f"\n  {err[:1].upper()}{err[1:]}\n"
        threading.Thread(target=self._cut,
                         args=(plan["sources"], plan["offset"],
                               plan["length"], plan["out"]),
                         daemon=True).start()

        lines = [
            "",
            "  Clip queued",
            f"    moment   {desc}",
            f"    from     {datetime.datetime.fromtimestamp(plan['start']):%Y-%m-%d %H:%M:%S}"
            f"  (−{_fmt_dur(lead)} lead)",
            f"    length   {_fmt_dur(plan['length'])}",
            f"    source   part {plan['part']:02d} @ {_fmt_hms(plan['offset'])}",
            f"    out      {plan['out']}",
        ]
        for n in plan["notes"]:
            lines.append(f"    note     {n}")
        lines += ["", "  (cutting in background — see the daemon log)", ""]
        return "\n".join(lines)

    def _plan_clip(self, stream: dict, epoch: float, length: float,
                   lead: float, label: str | None,
                   now: float) -> tuple[dict | None, str]:
        """Resolve one requested instant into a concrete ffmpeg job.

        Returns (plan, "") or (None, reason). Shared by the single-clip path
        and --all so both clamp, refuse and name files identically.
        """
        # The lead shifts the window earlier without lengthening it, so the
        # clip is exactly as long as asked and the named moment sits `lead`
        # seconds in. You notice a funny thing after it lands, and the
        # stream's own latency pushes the same way.
        start = epoch - lead
        notes = []

        first_zero = stream["_part_history"][0]["zero_epoch"]
        if start < first_zero:
            notes.append("clamped to the start of the recording")
            start = first_zero
        edge = now - CLIP_TAIL_GUARD_S
        if start >= edge:
            return None, (f"at (or past) the live edge — nothing on disk yet "
                          f"(wait {int(start - edge) + CLIP_TAIL_GUARD_S}s)")
        if start + length > edge:
            length = edge - start
            notes.append(f"trimmed to {_fmt_dur(length)} at the live edge")

        resolved = self._resolve_part(stream, start)
        if resolved is None:
            return None, "not inside any part of this recording"
        part, offset = resolved
        sources = self._part_sources(stream["stream_title"], part["num"])
        if not sources:
            return None, f"no media file on disk yet for part {part['num']:02d}"

        # A window starting in one part and ending after the next one began
        # would run off the end of the file it is cutting from.
        idx = stream["_part_history"].index(part)
        if idx + 1 < len(stream["_part_history"]):
            nxt = stream["_part_history"][idx + 1]["started_ts"]
            if start + length > nxt:
                length = nxt - start
                notes.append(f"trimmed to {_fmt_dur(length)} at the part boundary")

        return {
            "start":   start,
            "offset":  offset,
            "length":  length,
            "sources": sources,
            "part":    part["num"],
            "notes":   notes,
            "out":     self._clip_out_path(stream, start, length, label),
        }, ""

    # ── --all: cut every ! note on the current entry ───────────────────────

    def _clip_all(self, stream: dict, length: float | None,
                  lead: float) -> str:
        """Cut every `!` note under this stream's Obsidian entry, in order."""
        idx = stream.get("obsidian_index")
        if not idx:
            return "\n  That recording has no Obsidian entry to read.\n"
        pending = [n for n in ls_common.obsidian_entry_notes(self.config, idx)
                   if n.lstrip().startswith("!")]
        if not pending:
            return f"\n  No ! notes on entry {idx:03d}.\n"

        rec0 = stream.get("_record_start_epoch")
        brd0 = stream.get("_stream_start_epoch")
        now  = time.time()
        jobs, skipped = [], []
        for raw in pending:
            note = _parse_note(raw)
            if note is None:
                skipped.append((raw, "no timestamp"))
                continue
            zero = rec0 if note["from_record"] else brd0
            if not zero:
                skipped.append((raw, "no broadcast start for that timebase"))
                continue
            want = note["length"] or length or float(
                self.config.get("clip_length_s", 180))
            plan, err = self._plan_clip(stream, zero + note["offset"], want,
                                        lead, note["text"], now)
            if plan is None:
                skipped.append((raw, err))
                continue
            plan["raw"] = raw
            jobs.append(plan)

        if jobs:
            threading.Thread(target=self._run_all, args=(idx, jobs),
                             daemon=True).start()

        lines = ["", f"  Entry {idx:03d} — {len(jobs)} queued"
                     + (f", {len(skipped)} skipped" if skipped else "")]
        for p in jobs:
            lines.append(f"    {_fmt_hms(p['offset'])}  {_fmt_dur(p['length']):>6}  "
                         f"{os.path.basename(p['out'])}")
            for n in p["notes"]:
                lines.append(f"              ↳ {n}")
        for raw, why in skipped:
            lines.append(f"    skipped   {raw[:48]} — {why}")
        lines += ["", "  (cutting in order — ! clears as each one lands)", ""]
        return "\n".join(lines)

    def _run_all(self, index: int, jobs: list[dict]):
        """Cut queued clips one at a time, clearing each note as it lands.

        Sequential on purpose: they are stream copies off one disk so there
        is nothing to win by racing them, and one worker means only one
        writer touching the Obsidian file. A failed cut keeps its `!`, so
        the next --all retries it instead of losing it silently.
        """
        for p in jobs:
            if not self._cut(p["sources"], p["offset"], p["length"], p["out"]):
                continue
            cleared = p["raw"].lstrip()[1:].lstrip()
            if not ls_common.obsidian_replace_note(self.config, index,
                                                   p["raw"], cleared):
                logger.warning(f"Clip cut but ! not cleared: {p['raw'][:60]}")

    # ── completion & upload ───────────────────────────────────────────────

    def _stop_process(self, process, timeout=45):
        if process is None or process.poll() is not None:
            return
        try:
            pgid = os.getpgid(process.pid)
        except ProcessLookupError:
            return

        for sig, wait_s in ((signal.SIGINT, timeout),
                            (signal.SIGTERM, 15),
                            (signal.SIGKILL, 10)):
            try:
                os.killpg(pgid, sig)
            except ProcessLookupError:
                return            # group already gone — clean exit
            try:
                process.wait(timeout=wait_s)
                return
            except subprocess.TimeoutExpired:
                continue

    # Hard cap on holding a capture open for chat alone. Chat cannot reliably
    # see the broadcast end by itself, so liveness is what ends this, and the
    # cap is only a backstop against a probe that never gives a clear answer.
    ABANDONED_MAX_S = 12 * 3600

    def _wait_chat_then_complete(self, stream_key: str):
        """
        Hold the capture open for a chat whose video gave up — but only while
        the broadcast is actually live.

        Liveness decides, not the chat thread: chat has no view of the video
        stream ending and would otherwise sit in a segment loop indefinitely.
        Polls at the platform's own configured interval so this adds no
        traffic beyond what monitoring would have done anyway, and an
        inconclusive probe keeps waiting rather than discarding a live chat —
        ABANDONED_MAX_S is what guarantees it ends.
        """
        stream = self.active_streams.get(stream_key)
        if not stream:
            return
        title = stream["stream_title"]
        interval = self._platform_interval(stream["platform"])
        deadline = time.time() + self.ABANDONED_MAX_S
        reason = "cap reached"

        while time.time() < deadline:
            stream = self.active_streams.get(stream_key)
            if not stream:
                return                          # completed elsewhere
            chat = stream.get("chat_thread")
            if not (chat and chat.is_alive()):
                reason = "chat ended"
                break
            if not self._source_still_live(stream):
                reason = "stream ended"
                break
            time.sleep(interval)

        logger.info(f"Video abandoned, {reason}; completing: {title}")
        self._handle_completion(stream_key)     # sets chat_stop_event

    def _handle_completion(self, stream_key: str, upload: bool = True):
        """Stop chat, merge parts, upload, write final metadata."""
        if stream_key not in self.active_streams:
            return
        stream   = self.active_streams[stream_key]
        title    = stream["stream_title"]
        platform = stream["platform"]
        obs_idx  = stream.get("obsidian_index")
        logger.info(f"Completing: {title}")

        # Tracked out here because every `return` below is a real outcome the
        # archive wants: no parts, merge failed, upload failed, upload=False.
        # The packet fires from `finally` with whatever turned out to be true,
        # so a broadcast that happened and produced no file is recorded as a
        # broadcast with no file rather than as silence.
        _have_video = False
        _have_chat  = False
        _duration   = None

        try:
            self._stop_process(stream.get("video_process"))

            if stream.get("chat_stop_event"):
                stream["chat_stop_event"].set()
                chat = stream.get("chat_thread")
                if chat and chat is not threading.current_thread():
                    chat.join(timeout=15)

            if not upload or not os.path.exists(self.config["nas_path"]):
                return

            # ── Chat (.json) ──
            chat_file = os.path.join(self.config["output"], f"{title}.json")
            if os.path.exists(chat_file) and os.path.getsize(chat_file) > 100:
                chat_dst = os.path.join(self.config["nas_path"], f"{title}.json")
                _have_chat = self._upload(chat_file, chat_dst)

            # ── Video: merge parts → .mp4 with faststart → upload ──
            parts = self._find_part_files(title)
            if not parts:
                if stream.get("_from_start") and glob.glob(os.path.join(
                        self.config["output"], f"{glob.escape(title)}.part*.f*.*")):
                    logger.warning(
                        f"No merged file for {title}: yt-dlp left unmerged from-start "
                        f"fragments (stopped before its own merge). Fragments preserved; "
                        f"re-run yt-dlp to resume+finalize, or merge the f-streams manually."
                    )
                else:
                    logger.warning(f"No video parts found for: {title}")
                return

            merged_local = os.path.join(self.config["output"], f"{title}.mp4")
            ok, duration = self._merge_parts(parts, merged_local)
            if not ok:
                logger.error(f"Merge failed; parts left in place for: {title}")
                return
            # A duration measured off the merged file is true whether or not the
            # upload then works, so it is claimed here rather than at the end.
            _duration = duration

            dst = os.path.join(self.config["nas_path"], f"{title}.mp4")
            if not self._upload(merged_local, dst):
                return
            _have_video = True

            # ── Obsidian + cache ──
            if obs_idx:
                ls_common.obsidian_update_entry(
                    self.config, obs_idx, platform,
                    stream_title=title, duration_seconds=duration,
                    video_ext=".mp4",
                )
            if duration:
                cache = ls_common.load_cache()
                vod   = ls_common.find_vod(cache, stream["identifier"], platform)
                if vod:
                    vod["duration"] = int(duration)
                    ls_common.save_cache(cache)
            logger.info(f"Uploaded and logged: {title}")

        except Exception as e:
            logger.error(f"Completion error for {title}: {e}")
        finally:
            try:
                ls_archive.post_done(
                    self.config,
                    platform=platform,
                    video_id=stream["identifier"],
                    stream_title=title,
                    duration_seconds=_duration,
                    video_ext=".mp4",
                    have_video=_have_video,
                    have_chat=_have_chat,
                )
            except Exception as e:
                logger.warning(f"archive completion packet failed: {e}")
            # yt-dlp leaves fragments behind when a chat segment is killed
            # mid-write (.part-FragNN, orphan .live_chat.json), and an empty
            # part log per failed retry. None of it is recoverable data, and
            # left alone it accumulates in the output dir forever.
            out = self.config["output"]
            esc = glob.escape(title)
            junk = (glob.glob(os.path.join(out, f"{esc}.chatseg*.part-Frag*"))
                    + glob.glob(os.path.join(out, f"{esc}.chatseg*.ytdl"))
                    + glob.glob(os.path.join(out, f"{esc}.chatseg*.live_chat.json")))
            for lg in glob.glob(os.path.join(out, f"{esc}.part*.log")):
                try:
                    if os.path.getsize(lg) == 0:
                        junk.append(lg)
                except OSError:
                    pass
            if junk:
                self._cleanup(junk)
                logger.info(f"Removed {len(junk)} leftover temp file(s): {title}")

            self.active_streams.pop(stream_key, None)
            # AFTER the pop, not before: the heartbeat sends whatever this dict
            # holds, and sending it a line earlier would announce the stream
            # that just finished as still running.
            ls_archive.post_live(self.config, self.active_streams)
            logger.info(f"Cleanup done: {title}")

    def _upload(self, src: str, dst: str) -> bool:
        """Move file to NAS via rsync. Merge step already handles mp4 faststart,
        so this is just file transport now.
        """
        try:
            if os.path.exists(dst):
                logger.info(f"Already on NAS: {os.path.basename(src)}")
                os.remove(src)
                return True
            subprocess.run(
                ["rsync", "-av", "--remove-source-files", src, dst], check=True,
            )
            logger.info(f"Uploaded: {os.path.basename(src)}")
            return True
        except Exception as e:
            logger.error(f"Upload failed: {os.path.basename(src)}: {e}")
            return False

    # ── main loop ─────────────────────────────────────────────────────────

    def run(self):
        logger.info("=" * 60)
        logger.info("ls-rec starting")
        logger.info("=" * 60)
        logger.info(f"  > Check interval: YT {self._platform_interval('youtube')}s / "
                    f"TW {self._platform_interval('twitch')}s")
        logger.info(f"  > Cooldown: {self.config['cooldown_duration']}s")
        logger.info(f"  > Watchdog stall threshold: {WATCHDOG_STALL_S}s")
        logger.info("  > Archive: "
                    + (self.config.get("archive_url") if ls_archive.enabled(self.config)
                       else "off (archive_url/archive_token unset)"))
        self.command_server.start()
        print("-" * 80)

        try:
            while True:
                # Drain anything the archive missed while it was down. Before
                # the cooldown check on purpose: a backlog should clear whether
                # or not we are currently allowed to record.
                try:
                    ls_archive.flush(self.config)
                except Exception as e:
                    logger.warning(f"archive flush failed: {e}")

                # Say what is recording, every tick, whether or not anything
                # is. Up here beside flush() and above the cooldown check for
                # the same reason that one is: the `continue` below skips the
                # rest of the loop, and a tick that sends nothing reads to the
                # archive as "the recorder has gone quiet" rather than "nothing
                # is running" — which are different states and drawn
                # differently. Never fatal; post_live swallows its own failures.
                ls_archive.post_live(self.config, self.active_streams)

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
        # A farewell, so a planned restart drops the badge now instead of three
        # intervals from now. Belt and braces — each _handle_completion above
        # already beat on its way out — but it also covers the case where one
        # of them threw, and it costs one request on a path taken once.
        ls_archive.post_live(self.config, self.active_streams)
        logger.info("Shutdown complete.")


def _count_chat_lines(path: str) -> int:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return sum(1 for ln in f if ln.strip())


def _find_live_chat(nas_path: str, video_id: str,
                    exclude: str | None = None) -> str | None:
    """Existing non-posthoc chat json on NAS for this video_id."""
    ex = os.path.abspath(exclude) if exclude else None
    for path in sorted(glob.glob(os.path.join(nas_path, "*.json"))):
        if path.endswith(".posthoc.json"):
            continue
        if ex and os.path.abspath(path) == ex:
            continue
        if ls_common.extract_video_id_from_filename(os.path.basename(path)) == video_id:
            return path
    return None

def _merge_posthoc_chat(nas_path: str, video_id: str, posthoc: str):
    """Merge posthoc YT chat into the live capture in place.

    Live's pre-stream chat + any dropped middle ← backfilled by posthoc.
    Output replaces the live file under its existing name, so obsidian /
    audit links stay valid. No live capture → posthoc becomes canonical.
    """
    if not os.path.exists(posthoc):
        print("  ⚠ Posthoc chat not produced.")
        return

    live = _find_live_chat(nas_path, video_id, exclude=posthoc)
    if not live:
        canonical = posthoc[: -len(".posthoc.json")] + ".json"
        os.replace(posthoc, canonical)
        print(f"  ✔ Chat (posthoc only): {os.path.basename(canonical)}")
        return

    merge_script = os.path.join(ls_common.SCRIPT_DIR, "merge_yt_chats.py")
    tmp = live + ".merging"
    r = subprocess.run([sys.executable, merge_script, live, posthoc, "-o", tmp])

    # Safety: never let a bad merge shrink the live capture.
    if (r.returncode == 0 and os.path.exists(tmp)
            and _count_chat_lines(tmp) >= _count_chat_lines(live)):
        os.replace(tmp, live)
        os.remove(posthoc)
        print(f"  ✔ Chat merged → {os.path.basename(live)}")
    else:
        if os.path.exists(tmp):
            os.remove(tmp)
        print("  ⚠ Merge failed/suspect; live + posthoc both kept for manual merge.")

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

    release_ts = ls_common.stream_start_epoch(data)
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
            # Pull posthoc to a distinct name so it can't clobber a live capture.
            cmd = ls_common.ytdlp_chat_cmd(
                config, url, f"{safe_title}.posthoc.%(ext)s",
            )
            subprocess.run(cmd, cwd=nas_path)
            lc = os.path.join(nas_path, f"{safe_title}.posthoc.live_chat.json")
            posthoc = os.path.join(nas_path, f"{safe_title}.posthoc.json")
            if os.path.exists(lc):
                os.rename(lc, posthoc)

            if platform == "youtube":
                _merge_posthoc_chat(nas_path, video_id, posthoc)
            elif os.path.exists(posthoc):
                os.replace(posthoc, os.path.join(nas_path, f"{safe_title}.json"))

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

    # Everything else → daemon over socket.
    # shlex.join, not " ".join: `clip "2026.08.20 21:42" 5` and
    # `clip -12m 3 --name "monkey bit"` both have to arrive as the same
    # argv the shell handed us, not as a re-split word soup.
    send_command_and_print(shlex.join(sys.argv[1:]))


if __name__ == "__main__":
    main()
