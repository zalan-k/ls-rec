#!/usr/bin/env python3
"""Do what the archive asks. One job at a time, on this machine's terms.

The archive holds no write handle inside the media tree — that is the whole
point of the arrangement, and it is why /media is mounted read-only in its
compose file. So when a clip is approved, or purged, or pasted in as a link,
the archive cannot act. It writes down what it wants and this worker comes and
takes it.

    fetch       a url someone pasted -> a file in quarantine, for review
    promote     an approved file     -> quarantine into the media tree
    purge       an admin said so     -> gone
    music_probe a song's link        -> what the page says about it
    music_fetch an approved song     -> the media tree, for keeping

Run as its own service, deliberately. ls_archive.py is a library on the
recorder's poll tick, and its own first paragraph says nothing in it may ever
break a recording: six-second timeouts, every call wrapped, failures spooled.
The work here is minutes of yt-dlp, multi-gigabyte moves and unlink() on
masters. That does not belong on the tick that notices she has gone live.

WHAT THE ARCHIVE MAY SAY
    A job carries a kind, a snippet id, a url, and one or two RELATIVE names —
    `4f3c….mp4` in quarantine, `snippets/4f3c….mp4` in the media tree. Never a
    directory, never an absolute path, never a command. Where those roots are
    is resolved here, from this machine's config, and every name is checked to
    land inside them with symlinks already resolved. A compromised archive can
    ask for a file that does not exist; it cannot ask for one outside the two
    directories this worker was pointed at, and it cannot name a host to
    download from that is not on the list below.

    That last one is the important one. The archive has its own copy of the
    host allowlist and refuses links that fail it, but that copy is a courtesy
    to whoever is pasting. This one is the rule.

USAGE
    ls_jobs.py                 poll forever (what the service runs)
    ls_jobs.py --check         resolve the roots, prove them, print, exit
    ls_jobs.py --once          take one pass through the queue and exit
    ls_jobs.py --kinds promote,purge      leave fetch to somebody else
"""

from __future__ import annotations

import argparse
import datetime
import errno
import glob
import json
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import ls_archive
import ls_common

logger = logging.getLogger("ls-jobs")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Everything here can be overridden in config.json; this is what runs if it
# says nothing.
DEFAULTS = {
    "archive_job_interval":    20,        # seconds between polls when idle
    "archive_job_kinds":       list(ls_archive.PI_KINDS),
    "archive_fetch_max_s":     600,       # the archive's own caps: 10 minutes
    "archive_fetch_max_mb":    200,       # and 200 MB
    # A download not finished by now will not be. Longer than the archive's
    # five-minute lease on purpose: with one worker there is nobody to steal
    # the job, and cutting a legitimate download short to satisfy a lease
    # nobody is contending for would be the wrong trade.
    "archive_fetch_timeout_s": 1800,
    "archive_fetch_hosts": [
        "youtube.com", "youtu.be",
        "twitch.tv",
        "twitter.com", "x.com",
        # Not a yt-dlp site — a plain https download. These links expire, so a
        # job that sits in the queue overnight can find a dead url. That
        # reports as a failure carrying the host's own words, which is the
        # right answer for a link that has genuinely gone.
        "cdn.discordapp.com", "media.discordapp.net",
    ],

    # ── music ────────────────────────────────────────────────────────────
    # Where songs land under the media root. The archive stores the path it is
    # told and never derives one, so this is the only place the word lives.
    "archive_music_prefix":    "music/",
    # Its own caps, because these are not the same thing as a pasted clip. A
    # music video is three minutes and worth keeping at a decent size; the
    # generous byte ceiling is the point of the module, not an oversight.
    "archive_music_max_s":     900,
    "archive_music_max_mb":    500,
    "archive_music_timeout_s": 1800,
    # THE FORMAT POLICY LIVES HERE, and that is deliberate: the archive names
    # an id and a verb and has never told a worker what to fetch. Change this
    # to `bestaudio/best` for an audio-only collection and nothing on the other
    # side needs to know.
    "archive_music_format":    "bv*[height<=1080]+ba/b[height<=1080]/b",
    "archive_music_container": "mp4",
}

# Fetched with a plain https GET rather than yt-dlp: these are direct file
# urls, not pages with a video somewhere in them.
DIRECT_HOSTS = ("cdn.discordapp.com", "media.discordapp.net")

# The archive's own word for a transfer that has not finished. Its quarantine
# view reads this prefix and shows such a file as `unfinished` rather than as
# an orphan with no row — so a fetch that dies halfway looks like what it is.
PART = ".part-"

# Extensions a fetch may keep. Not a security control — the containment check
# is that — but a fetch that came back as a .html error page should not be
# handed to the archive as a snippet.
KEEP_EXT = {".mp4", ".mkv", ".webm", ".mov", ".m4v", ".gif",
            ".m4a", ".mp3", ".opus", ".ogg", ".wav", ".flac"}


def setting(config: dict, key: str):
    v = config.get(key)
    return DEFAULTS[key] if v in (None, "", []) else v


def _mb(n: int) -> str:
    """Sized for a log line a person reads. A 300 KB clip rounding to "0 MB"
    reads as a failure, which is exactly the wrong impression for a purge."""
    return f"{n / 1048576:.0f} MB" if n >= 1048576 else f"{n / 1024:.0f} KB"


def _mmss(s: float) -> str:
    s = int(s)
    return f"{s // 60}:{s % 60:02d}"


# ══════════════════════════════════════════════════════════════════════════
#  WHERE THE FILES ARE
# ══════════════════════════════════════════════════════════════════════════
#
# Derived from what the recorder already knows, and overridable when the guess
# is wrong. `nas_path` is where raws are uploaded and `archive_media_prefix` is
# where that sits inside the archive's media root — so the root is nas_path
# with the prefix walked back off it. Quarantine is the archive's sibling
# directory, the one place inside the share the archive itself may write.
#
# Both are printed at startup and both are proved before any work is taken.
# Getting one of these wrong quietly is the failure this file most has to
# avoid: a promote into the wrong directory is a master that no longer exists
# anywhere the archive can see.

# Lives in ls_archive now — ls_assets needs the same answer, and two copies of
# a path derivation is how the two of them come to disagree. Kept as a name
# here because everything below and the startup banner call it.
media_root = ls_archive.media_root


def quarantine_dir(config: dict) -> str | None:
    explicit = str(config.get("archive_quarantine_dir") or "").strip()
    if explicit:
        return os.path.abspath(os.path.expanduser(explicit))
    m = media_root(config)
    return os.path.join(os.path.dirname(m), "quarantine") if m else None


def music_dir(config: dict) -> str | None:
    """Where songs live, under the media root.

    Not a sibling of it like quarantine: a song that has been approved IS
    archive content, and it goes where the archive can read it. The prefix is
    config here and a stored path there — nothing derives it twice.
    """
    m = media_root(config)
    if not m:
        return None
    rel = str(setting(config, "archive_music_prefix")).strip().strip("/")
    return os.path.join(m, *rel.split("/")) if rel else m


# ══════════════════════════════════════════════════════════════════════════
#  NAMES THE ARCHIVE MAY SAY
# ══════════════════════════════════════════════════════════════════════════

def _under(root: str, path: str) -> bool:
    """Is `path` inside `root` once every symlink is resolved?

    realpath and not normpath: a symlink dropped in quarantine pointing at
    /etc would survive a lexical check and promote whatever it aims at. For a
    destination that does not exist yet realpath resolves the part that does
    and appends the rest, which is the answer wanted.
    """
    r = os.path.realpath(root)
    p = os.path.realpath(path)
    return p == r or p.startswith(r + os.sep)


def resolve_name(root: str | None, name, *, bare: bool = False) -> str | None:
    """One relative name from a job, as a path here. None means refuse.

    Rejected: anything absolute, anything with a `..` or an empty segment,
    anything with a drive letter, and — when `bare` — anything with a
    separator at all. Quarantine names are minted by the archive as one ULID
    plus an extension, so a promote source with a directory in it is already
    wrong before the containment check gets a say.
    """
    if not root:
        return None
    n = str(name or "").strip().replace("\\", "/")
    if not n or n.startswith("/") or (len(n) > 1 and n[1] == ":"):
        return None
    parts = n.split("/")
    if any(p in ("", ".", "..") for p in parts):
        return None
    if bare and len(parts) != 1:
        return None
    p = os.path.join(root, *parts)
    return p if _under(root, p) else None


def allowed_host(config: dict, url: str) -> str | None:
    """The hostname, if this machine is willing to fetch from it.

    Matched on a dot boundary, so `youtube.com.evil.tld` is not youtube and
    `www.youtube.com` is. https only: there is no reason to pull a video over
    a connection anyone on the path can rewrite.
    """
    try:
        u = urllib.parse.urlparse(str(url or "").strip())
    except ValueError:
        return None
    if u.scheme != "https" or not u.hostname:
        return None
    host = u.hostname.lower().rstrip(".")
    for allowed in setting(config, "archive_fetch_hosts"):
        a = str(allowed).lower().strip().lstrip(".")
        if a and (host == a or host.endswith("." + a)):
            return host
    return None


# ══════════════════════════════════════════════════════════════════════════
#  PROMOTE
# ══════════════════════════════════════════════════════════════════════════

def do_promote(config: dict, job: dict):
    """Quarantine into the media tree. Returns (status, result_path, error)."""
    pay = job.get("payload") or {}
    q, m = quarantine_dir(config), media_root(config)
    rel = str(pay.get("to") or "")
    src = resolve_name(q, pay.get("from"), bare=True)
    dst = resolve_name(m, rel)
    if not src or not dst:
        return ("failed", None, "the archive named a file this worker will not touch")

    if not os.path.isfile(src):
        # Already there, and quarantine already empty: this job ran, and its
        # report was the thing that went missing. Saying `done` again is the
        # honest answer and the archive treats a repeat as idempotent anyway.
        if os.path.isfile(dst) and os.path.getsize(dst) > 0:
            logger.info(f"promote {rel}: already moved")
            return ("done", rel, None)
        return ("failed", None, "there is nothing in quarantine by that name")

    if os.path.exists(dst):
        # Names are minted ULIDs, so this cannot happen by chance — and the
        # thing sitting there is a master. Refuse rather than overwrite it.
        return ("failed", None, "something is already at that name in the media tree")

    size = os.path.getsize(src)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    try:
        # The whole reason quarantine is a sibling of the media tree rather
        # than somewhere convenient: same filesystem, so this is a rename(2) —
        # atomic, instant, and it copies no bytes. A 200 MB clip moves in
        # microseconds and there is never a half-written master.
        os.replace(src, dst)
        how = "renamed"
    except OSError as e:
        if e.errno != errno.EXDEV:
            return ("failed", None, f"could not move it: {e.strerror or e}")
        # Different filesystems after all. Copy to a `.part-` name first and
        # rename into place, so a power cut in the middle leaves an obvious
        # scrap rather than a truncated master.
        tmp = os.path.join(os.path.dirname(dst), f"{PART}{job['id']}")
        try:
            shutil.copyfile(src, tmp)
            with open(tmp, "rb+") as f:
                os.fsync(f.fileno())
            os.replace(tmp, dst)
            d = os.open(os.path.dirname(dst), os.O_DIRECTORY)
            try:
                os.fsync(d)
            finally:
                os.close(d)
            os.remove(src)
        except Exception as e2:
            for leftover in (tmp,):
                try:
                    os.remove(leftover)
                except OSError:
                    pass
            return ("failed", None, f"could not copy it across: {e2}")
        how = "copied across filesystems"

    landed = os.path.getsize(dst) if os.path.isfile(dst) else 0
    if landed != size:
        return ("failed", None, f"it arrived {landed} bytes and left as {size}")
    logger.info(f"promote {rel}: {how}, {_mb(size)}")
    return ("done", rel, None)


# ══════════════════════════════════════════════════════════════════════════
#  PURGE
# ══════════════════════════════════════════════════════════════════════════

def do_purge(config: dict, job: dict):
    """Delete a master. The one irreversible thing in here.

    It is already guarded twice before it reaches this queue: purge is
    admin-only, and the archive refuses to purge anything still published, so
    a clip has to be unlisted first. What is left for this end is making sure
    the name really is inside the media tree.
    """
    pay = job.get("payload") or {}
    rel = str(pay.get("path") or "")
    target = resolve_name(media_root(config), rel)
    if not target:
        return ("failed", None, "the archive named a file this worker will not touch")

    if not os.path.lexists(target):
        # Gone is the outcome asked for. A purge that finds nothing has
        # succeeded, and reporting otherwise would leave a job to retry that
        # can only ever find the same nothing.
        logger.info(f"purge {rel}: already gone")
        return ("done", rel, None)
    if os.path.isdir(target) and not os.path.islink(target):
        return ("failed", None, "that is a directory, not a file")

    try:
        size = os.path.getsize(target) if os.path.isfile(target) else 0
        os.remove(target)
    except OSError as e:
        return ("failed", None, f"could not delete it: {e.strerror or e}")
    logger.warning(f"purge {rel}: deleted, {_mb(size)}")
    return ("done", rel, None)


# ══════════════════════════════════════════════════════════════════════════
#  FETCH
# ══════════════════════════════════════════════════════════════════════════
#
# Probe first. A four-hour VOD pasted in by mistake is refused in a couple of
# seconds instead of after the download, and the person who pasted it gets a
# reason rather than a shrug. Hosts that will not report a duration fall
# through to the byte cap and a second check once the bytes are here.

def _ytdlp(config: dict) -> list[str]:
    """The binary, without cookies.

    ls_common builds this too, but with `--cookies-from-browser` on by
    default — right for recording her streams while signed in, wrong for a
    public link on a headless Pi where there is no browser to read cookies
    out of and the flag is a hard failure rather than a fallback.
    """
    venv = config.get("venv")
    return [os.path.join(venv, "bin", "yt-dlp") if venv else "yt-dlp"]


def _run(cmd: list[str], timeout: int):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _tail(text: str, n: int = 300) -> str:
    """The last thing a tool said, cleaned up enough to put in front of a
    person. yt-dlp's real complaint is on the last non-empty line; everything
    above it is progress."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return ""
    msg = lines[-1]
    return re.sub(r"^ERROR:\s*", "", msg)[:n]


def probe_duration(config: dict, url: str) -> float | None:
    """Seconds, or None when the host will not say."""
    try:
        r = _run(_ytdlp(config) + ["--no-warnings", "--no-playlist", "--skip-download",
                                   "--print", "%(duration)s", url], 90)
    except subprocess.TimeoutExpired:
        return None
    if r.returncode != 0:
        return None
    raw = (r.stdout or "").strip().splitlines()
    try:
        return float(raw[-1]) if raw and raw[-1] not in ("NA", "None", "") else None
    except ValueError:
        return None


def file_duration(path: str) -> float | None:
    """What ffprobe makes of what actually arrived."""
    try:
        r = _run(["ffprobe", "-v", "error", "-show_entries", "format=duration",
                  "-of", "default=nw=1:nk=1", path], 60)
        return float((r.stdout or "").strip()) if r.returncode == 0 else None
    except (subprocess.TimeoutExpired, ValueError, FileNotFoundError):
        return None


def _direct_download(url: str, dest: str, max_bytes: int, timeout: int) -> str | None:
    """A plain https GET with a hard byte ceiling. Returns an error, or None.

    Streamed in chunks and stopped at the cap rather than read into memory:
    the cap is what makes an unbounded url safe to pull at all, and a Discord
    attachment can be anything somebody dropped in a channel.
    """
    req = urllib.request.Request(url, headers={"user-agent": "ls-rec/jobs"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            declared = r.headers.get("content-length")
            if declared and declared.isdigit() and int(declared) > max_bytes:
                return f"that is {_mb(int(declared))}; the cap is {_mb(max_bytes)}"
            got, deadline = 0, time.time() + timeout
            with open(dest, "wb") as f:
                while True:
                    if time.time() > deadline:
                        return "the download took too long"
                    chunk = r.read(1 << 20)
                    if not chunk:
                        break
                    got += len(chunk)
                    if got > max_bytes:
                        return f"it went past {_mb(max_bytes)} while downloading"
                    f.write(chunk)
    except Exception as e:
        return f"{type(e).__name__}: {e}"[:300]
    return None


def _scraps(q: str, job_id: str) -> None:
    for f in glob.glob(os.path.join(q, f"{PART}{job_id}*")):
        try:
            os.remove(f)
        except OSError:
            pass


def do_fetch(config: dict, job: dict):
    """A url into quarantine. Returns (status, result_path, error)."""
    url = str(job.get("url") or "").strip()
    snip = str(job.get("snippet_id") or "").strip()
    q = quarantine_dir(config)
    # The host first, before anything about this end. It is the check that
    # decides whether the url may be touched at all, and its answer is the one
    # the person who pasted the link needs to read — a malformed job should not
    # be able to mask a refused host with a complaint about itself.
    host = allowed_host(config, url)
    if not host:
        # Named, because "not allowed" without saying what was not allowed is
        # not something anybody can act on.
        shown = (urllib.parse.urlparse(url).hostname or url)[:80]
        return ("failed", None, f"{shown} is not on this recorder's allowlist")

    if not q or not os.path.isdir(q):
        return ("failed", None, "this worker has nowhere to put it")
    if not snip or not re.fullmatch(r"[0-9A-Za-z]{1,64}", snip):
        return ("failed", None, "the job does not name a snippet this worker can name a file after")

    max_bytes = int(setting(config, "archive_fetch_max_mb")) * 1048576
    max_s = int(setting(config, "archive_fetch_max_s"))
    timeout = int(setting(config, "archive_fetch_timeout_s"))
    stem = os.path.join(q, f"{PART}{job['id']}")
    _scraps(q, job["id"])

    try:
        if host in DIRECT_HOSTS:
            # No page to parse and no duration to ask for: the ceiling is
            # bytes, and the duration is checked once the file is here.
            ext = os.path.splitext(urllib.parse.urlparse(url).path)[1].lower()[:8] or ".mp4"
            tmp = stem + (ext if ext in KEEP_EXT else ".mp4")
            err = _direct_download(url, tmp, max_bytes, timeout)
            if err:
                return ("failed", None, err)
        else:
            secs = probe_duration(config, url)
            if secs and secs > max_s:
                return ("failed", None,
                        f"that is {_mmss(secs)} long; the cap is {_mmss(max_s)}")
            r = _run(_ytdlp(config) + [
                "--no-warnings", "--no-playlist", "--no-progress",
                # Nothing above 1080p: the archive re-encodes everything it
                # keeps, and pulling 4K to throw the pixels away is minutes of
                # somebody's bandwidth for no difference on screen.
                "-f", "bv*[height<=1080]+ba/b[height<=1080]/b",
                "--merge-output-format", "mp4",
                # Belt to the probe's braces, for a host that reports a size
                # but no duration.
                "--max-filesize", f"{max_bytes}",
                "-o", stem + ".%(ext)s", url], timeout)
            if r.returncode != 0:
                return ("failed", None,
                        _tail(r.stderr) or _tail(r.stdout) or f"yt-dlp exited {r.returncode}")
            found = sorted(glob.glob(stem + ".*"), key=os.path.getsize, reverse=True)
            if not found:
                # --max-filesize aborts by writing nothing at all, which is
                # otherwise indistinguishable from a silent success.
                return ("failed", None, f"nothing came back — it may be over {_mb(max_bytes)}")
            tmp = found[0]
            for extra in found[1:]:
                try:
                    os.remove(extra)
                except OSError:
                    pass

        ext = os.path.splitext(tmp)[1].lower()
        if ext not in KEEP_EXT:
            return ("failed", None, f"that came back as {ext or 'something with no extension'}")
        got = os.path.getsize(tmp)
        if got == 0:
            return ("failed", None, "the file came back empty")
        if got > max_bytes:
            return ("failed", None, f"it is {_mb(got)}; the cap is {_mb(max_bytes)}")

        # The second look, for every host that would not say up front. ffprobe
        # missing is not a reason to refuse — the archive probes this file
        # again a second later and has the last word on it either way.
        secs = file_duration(tmp)
        if secs and secs > max_s + 1:
            return ("failed", None, f"that is {_mmss(secs)} long; the cap is {_mmss(max_s)}")

        # Named after the snippet, which is what the archive names an upload
        # too. The name comes from the job rather than from the url, so a
        # remote title can never become a filename here.
        final = f"{snip}{ext}"
        dest = resolve_name(q, final, bare=True)
        if not dest:
            return ("failed", None, "could not name the file safely")
        os.replace(tmp, dest)
        logger.info(f"fetch {host}: {final}, {_mb(got)}"
                    + (f", {_mmss(secs)}" if secs else ""))
        return ("done", final, None)

    except subprocess.TimeoutExpired:
        return ("failed", None, f"it was still going after {timeout // 60} minutes")
    finally:
        _scraps(q, job["id"])


# ══════════════════════════════════════════════════════════════════════════
#  THE LOOP
# ══════════════════════════════════════════════════════════════════════════

def probe_file(path: str) -> dict:
    """Everything ffprobe will say about a file, in one call.

    One call rather than one per field: this runs over every capture on a
    stream and ffprobe is the slow part. A file that will not probe returns an
    empty dict — the archive writes nothing for what is not in it, so "could
    not read it" and "read it and it was empty" stay different answers.
    """
    try:
        r = _run(["ffprobe", "-v", "error", "-show_entries",
                  "format=duration,format_name:stream=codec_type,codec_name,"
                  "width,height,avg_frame_rate",
                  "-of", "json", path], 120)
        if r.returncode != 0:
            return {}
        doc = json.loads(r.stdout or "{}")
    except (subprocess.TimeoutExpired, ValueError, FileNotFoundError,
            json.JSONDecodeError):
        return {}

    fmt = doc.get("format") or {}
    out: dict = {}
    try:
        out["file_duration_s"] = int(round(float(fmt["duration"])))
    except (KeyError, TypeError, ValueError):
        pass
    if fmt.get("format_name"):
        # "mov,mp4,m4a,3gp,3g2,mj2" — the first is the one anyone means.
        out["container"] = str(fmt["format_name"]).split(",")[0]

    vid = next((s for s in doc.get("streams", []) if s.get("codec_type") == "video"), None)
    aud = next((s for s in doc.get("streams", []) if s.get("codec_type") == "audio"), None)
    if vid:
        if vid.get("codec_name"):
            out["video_codec"] = str(vid["codec_name"])
        for k in ("width", "height"):
            if isinstance(vid.get(k), int):
                out[k] = vid[k]
        # "30000/1001", which is 29.97 and not 30. Kept as the float it is.
        fr = str(vid.get("avg_frame_rate") or "")
        if "/" in fr:
            try:
                n, d = fr.split("/")
                if float(d):
                    out["fps"] = round(float(n) / float(d), 3)
            except (ValueError, ZeroDivisionError):
                pass
    if aud and aud.get("codec_name"):
        out["audio_codec"] = str(aud["codec_name"])
    out["has_audio"] = 1 if aud else 0
    return out


def do_rescan(config: dict, job: dict):
    """Re-read a stream from its files and its links.

    Returns (status, result_path, error, findings). The findings are per
    capture and keyed by the id the archive sent, so nothing here has to guess
    which row it is talking about.

    `alive` is reported ONLY on a definite answer. `ok` means the platform
    served it; `gone` means the platform said it is not there any more. A probe
    that merely failed — no network, a bot check, a timeout — reports no
    `alive` at all, and the archive leaves the column exactly as it found it.
    That asymmetry is the whole design: a takedown noticed late costs a stale
    badge, and a bot check read as a takedown marks a living VOD dead.
    """
    pay = job.get("payload") or {}
    caps = pay.get("captures") or []
    if not isinstance(caps, list) or not caps:
        return ("failed", None, "the archive named no captures to look at", None)
    m = media_root(config)

    out = []
    for c in caps:
        cid = str(c.get("id") or "")
        if not cid:
            continue
        found: dict = {"id": cid}

        rel = c.get("video_path")
        if rel:
            # Resolved against this worker's own media root and refused if it
            # escapes — same discipline as promote, in the other direction.
            path = resolve_name(m, rel)
            if not path:
                found["note"] = "that path is outside the media root"
            elif not os.path.isfile(path):
                found["note"] = "no file at that path"
            else:
                probed = probe_file(path)
                if probed:
                    found.update(probed)
                else:
                    found["note"] = "ffprobe would not read the file"

        url = c.get("url")
        if url and not allowed_host(config, url):
            # The same allowlist fetch runs on, for the same reason. The route
            # that makes these jobs builds the payload out of capture rows, so
            # in practice this never fires — which is exactly when a check is
            # worth having, because the day it does fire is the day something
            # else learned to write a payload.
            found["note"] = (found.get("note", "") + " this worker will not "
                             "probe that host").strip()
            url = None
        if url:
            data, why = ls_common.ytdlp_probe(config, url, with_reason=True)
            if why == "ok" and data is not None:
                found["alive"] = 1
                # The platform's own length, which is not the file's and is not
                # written as one. Reported so a human can see the two disagree.
                if data.get("duration"):
                    try:
                        found["remote_duration_s"] = int(round(float(data["duration"])))
                    except (TypeError, ValueError):
                        pass
            elif why == "gone":
                found["alive"] = 0
            else:
                # offline / failed. Nothing is claimed about the video.
                found["note"] = (found.get("note", "") + f" probe: {why}").strip()

        if len(found) > 1:
            out.append(found)

    if not out:
        return ("failed", None, "nothing could be read for any capture", None)
    n_alive = sum(1 for x in out if "alive" in x)
    logger.info(f"rescan {str(pay.get('stream_id', '?'))[:8]}: "
                f"{len(out)} capture(s), {n_alive} answered on liveness")
    return ("done", None, None, {"captures": out})


# ══════════════════════════════════════════════════════════════════════════
#  HARVEST
# ══════════════════════════════════════════════════════════════════════════

# Hosts this worker will read a description from. Narrower than the fetch
# allowlist on purpose: fetch pulls a video somebody linked, harvest copies
# TEXT into the archive under somebody's licence, and the set of places that
# is defensible from is small and known. Fandom is CC-BY-SA, which is why the
# archive stores the url alongside whatever comes back.
WIKI_HOSTS = ("wikipedia.org", "fandom.com", "wikia.org")


def _wiki_api(url: str):
    """(api.php, page title) for a MediaWiki article url, or None.

    Both families put the article at /wiki/<Title> and expose the API at the
    site root — Fandom at /api.php, Wikipedia at /w/api.php. Anything that does
    not look like an article is refused rather than guessed at, because a guess
    here is a request to an arbitrary path on somebody else's host.
    """
    try:
        u = urllib.parse.urlparse(url)
    except ValueError:
        return None
    if u.scheme != "https":
        return None
    host = (u.hostname or "").lower().rstrip(".")
    if not any(host == h or host.endswith("." + h) for h in WIKI_HOSTS):
        return None
    parts = [p for p in (u.path or "").split("/") if p]
    if len(parts) < 2 or parts[-2] != "wiki":
        return None
    title = urllib.parse.unquote(parts[-1]).replace("_", " ")
    base = f"{u.scheme}://{u.netloc}"
    api = f"{base}/w/api.php" if host.endswith("wikipedia.org") else f"{base}/api.php"
    return (api, title)


def _get_json(url: str, timeout: int = 30):
    req = urllib.request.Request(url, headers={"user-agent": "ls-rec/jobs"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            # A wiki API answer is kilobytes. Reading unbounded is how a
            # redirect to something else becomes this worker's problem.
            return json.loads(r.read(2 << 20).decode("utf-8", "replace"))
    except Exception as e:
        logger.warning("harvest GET failed: %s: %s", type(e).__name__, str(e)[:160])
        return None


def do_harvest(config: dict, job: dict):
    """Read a wiki page for a tag. Returns (status, result_path, error, findings).

    Nothing is decided here. The archive stores what comes back as `seeded`,
    which is the row saying out loud that a machine wrote it and no human has
    been over it — and the first hand edit clears that flag. This worker's job
    is to be accurate about what the page said, not about whether it is right.
    """
    pay = job.get("payload") or {}
    url = str(pay.get("url") or job.get("url") or "").strip()
    if not url:
        return ("failed", None, "the job names no link", None)

    # WIKI_HOSTS is the allowlist here, and `archive_fetch_hosts` is
    # deliberately NOT consulted. That list says where this recorder will
    # download a VIDEO from; adding wikipedia.org to it so a description works
    # would widen the fetch surface to make a text feature go, which is the
    # wrong direction. What guards this is stricter than that list anyway:
    # three domains, https only, and a path that has to look like an article.
    api = _wiki_api(url)
    if not api:
        return ("failed", None,
                "that is not a wiki article this worker knows how to read — it reads "
                "MediaWiki article urls (Wikipedia, Fandom)", None)
    api_url, title = api

    params = urllib.parse.urlencode({
        "action": "query", "format": "json", "redirects": "1",
        "prop": "extracts",
        # The lead section as plain text: what a person reads first, and not a
        # wall of infobox markup.
        "exintro": "1", "explaintext": "1", "exsectionformat": "plain",
        "titles": title,
    })
    doc = _get_json(f"{api_url}?{params}")
    if not doc:
        return ("failed", None, "that wiki did not answer", None)

    pages = ((doc.get("query") or {}).get("pages") or {})
    page = next((p for p in pages.values() if isinstance(p, dict)), None)
    if not page or "missing" in page:
        return ("failed", None, f"that wiki has no page called {title!r}", None)

    extract = str(page.get("extract") or "").strip()
    if not extract:
        return ("failed", None, "that page has no lead paragraph to read", None)

    # One paragraph. A lead section runs to six and a tag row is not where
    # anybody reads six — the link is right there for the rest.
    first = extract.split("\n\n")[0].strip()
    found = {"title": page.get("title") or title, "summary": first[:1200]}
    logger.info("harvest %s: %d chars", found["title"], len(found["summary"]))
    return ("done", None, None, found)




# ══════════════════════════════════════════════════════════════════════════
#  MUSIC
# ══════════════════════════════════════════════════════════════════════════
#
# Somebody else's music video, kept because it will not always be there.
#
# Two kinds, and the split is the whole design. `music_probe` reads what a page
# says so a person can decide whether the song belongs; `music_fetch` downloads
# it, and only ever after that decision went the right way. Fetching at
# submission time would mean this machine spent somebody's bandwidth and the
# archive held the bytes of things it turned down — and then somebody has to
# decide a second time whether to keep them.
#
# Neither writes to quarantine. Quarantine exists so that bytes nobody has
# approved can sit somewhere the archive does not serve from; by the time a
# fetch is queued the approval has already happened, so there is nothing left
# to hold it for and the file goes where it is going to live.

# A YouTube id, and nothing else is accepted as one. The archive mints this
# from a canonical watch url and it becomes a FILENAME here, so it is checked
# rather than trusted — the same reason a fetch names its file after the
# snippet id and never after the remote title.
VIDEO_ID = re.compile(r"[0-9A-Za-z_-]{6,24}")


def _music_target(config: dict, job: dict):
    """(url, video_id, music dir, prefix) or a refusal string."""
    pay = job.get("payload") or {}
    url = str(pay.get("url") or job.get("url") or "").strip()
    vid = str(pay.get("video_id") or "").strip()
    if not url:
        return "the job names no link"
    if not vid or not VIDEO_ID.fullmatch(vid):
        return "the job does not name a video this worker can name a file after"
    # The host first, before anything about this end — same order and same
    # reason as a fetch: a malformed job must not mask a refused host.
    if not allowed_host(config, url):
        shown = (urllib.parse.urlparse(url).hostname or url)[:80]
        return f"{shown} is not on this recorder's allowlist"
    d = music_dir(config)
    if not d:
        return "this worker cannot work out where songs go"
    prefix = str(setting(config, "archive_music_prefix")).strip().strip("/")
    return (url, vid, d, prefix)


def _uploaded_at(data: dict) -> int | None:
    """When the VIDEO went up, in unix seconds.

    Not when the row was made, and not when this ran. `release_timestamp` and
    `timestamp` are exact and are preferred; `upload_date` is a date with no
    time in it, so it reads as UTC midnight — which is the honest answer for a
    field that genuinely does not carry a time, and is what every other date in
    the archive does with one.
    """
    ts = ls_common.stream_start_epoch(data)
    if ts:
        return ts
    ud = str(data.get("upload_date") or "").strip()
    if not re.fullmatch(r"\d{8}", ud):
        return None
    try:
        d = datetime.datetime.strptime(ud, "%Y%m%d").replace(
            tzinfo=datetime.timezone.utc)
        return int(d.timestamp())
    except ValueError:
        return None


def do_music_probe(config: dict, job: dict):
    """What a video says about itself. Returns (status, result_path, error, findings).

    Facts and no file. The archive puts these on a card so somebody can judge
    the song without leaving the page, and it decides nothing here — the same
    arrangement as harvest, for the same reason: this worker's job is to be
    accurate about what the page said.
    """
    t = _music_target(config, job)
    if isinstance(t, str):
        return ("failed", None, t, None)
    url, vid, _, _ = t

    # The cookied prober with the anonymous-first fallback, because YouTube
    # answers a bare probe with a bot check often enough that a plain call
    # would report half these songs as broken.
    data, why = ls_common.ytdlp_probe(config, url, timeout=45, with_reason=True)
    if data is None:
        if why == "gone":
            # Worth saying plainly rather than as a generic failure: a song
            # that has already been taken down is exactly what the collection
            # exists to catch, and the person who pasted it should be told that
            # is what happened rather than that "the probe failed".
            return ("failed", None,
                    "that video is gone — taken down, private, or region-locked", None)
        if why == "offline":
            return ("failed", None, "that link is a stream that is not on right now", None)
        return ("failed", None, "could not read that video's page", None)

    title = str(data.get("title") or "").strip()
    if not title:
        return ("failed", None, "that page has no title to read", None)
    secs = data.get("duration")
    found = {
        "title": title[:300],
        # `channel` is the display name and `uploader` is the older spelling of
        # it; channel_id survives a rename, which display names do not.
        "channel": (str(data.get("channel") or data.get("uploader") or "").strip() or None),
        "channel_id": (str(data.get("channel_id") or data.get("uploader_id") or "").strip()
                       or None),
        "uploaded_at": _uploaded_at(data),
        "duration_s": int(secs) if isinstance(secs, (int, float)) and secs > 0 else None,
    }
    logger.info("music probe %s: %s%s", vid, found["title"][:60],
                f" ({_mmss(secs)})" if secs else "")
    return ("done", None, None, found)


def do_music_fetch(config: dict, job: dict):
    """An approved song into the media tree.

    Returns (status, result_path, error, findings). `result_path` is relative
    to the MEDIA ROOT and carries the prefix — the archive stores it verbatim
    and resolves it against its own root, so a path that arrived without the
    prefix would resolve to a file that is not there.
    """
    t = _music_target(config, job)
    if isinstance(t, str):
        return ("failed", None, t, None)
    url, vid, mdir, prefix = t

    rel = lambda name: f"{prefix}/{name}" if prefix else name
    max_bytes = int(setting(config, "archive_music_max_mb")) * 1048576
    max_s = int(setting(config, "archive_music_max_s"))
    timeout = int(setting(config, "archive_music_timeout_s"))

    try:
        os.makedirs(mdir, exist_ok=True)
    except OSError as e:
        return ("failed", None, f"cannot make {mdir}: {e.strerror or e}", None)

    existing = sorted(glob.glob(os.path.join(mdir, f"{vid}.*")))
    existing = [f for f in existing
                if os.path.splitext(f)[1].lower() in KEEP_EXT and os.path.getsize(f) > 0]
    if existing:
        # Already here. The job ran and its report was what went missing —
        # promote answers a repeat the same way, and for the same reason.
        got = existing[0]
        name = os.path.basename(got)
        logger.info("music fetch %s: already here as %s", vid, name)
        thumb = os.path.join(mdir, f"{vid}.jpg")
        return ("done", rel(name), None,
                {"bytes": os.path.getsize(got),
                 "thumb_path": rel(f"{vid}.jpg") if os.path.isfile(thumb) else None})

    # Asked before anything is pulled, because a duration is the one cap that
    # can be checked without spending the bandwidth it is protecting.
    secs = probe_duration(config, url)
    if secs and secs > max_s:
        return ("failed", None,
                f"that is {_mmss(secs)} long; the cap is {_mmss(max_s)}", None)

    # The part file sits in the music directory rather than in quarantine, so
    # the rename at the end is a rename(2) on one filesystem instead of a copy
    # across two. Nothing serves it in the meantime: the archive resolves a
    # song from the path on its row, and no row says `.part-`.
    stem = os.path.join(mdir, f"{PART}{job['id']}")
    _scraps(mdir, job["id"])
    try:
        r = _run(_ytdlp(config) + [
            "--no-warnings", "--no-playlist", "--no-progress",
            "-f", str(setting(config, "archive_music_format")),
            "--merge-output-format", str(setting(config, "archive_music_container")),
            # The cover, in the same pass. A second job for one JPEG would be a
            # second thing to fail, and the archive falls back to YouTube's own
            # thumbnail url until this lands anyway.
            "--write-thumbnail", "--convert-thumbnails", "jpg",
            "--max-filesize", f"{max_bytes}",
            "-o", stem + ".%(ext)s", url], timeout)
        if r.returncode != 0:
            return ("failed", None,
                    _tail(r.stderr) or _tail(r.stdout) or f"yt-dlp exited {r.returncode}", None)

        got = sorted((f for f in glob.glob(stem + ".*")
                      if os.path.splitext(f)[1].lower() in KEEP_EXT),
                     key=os.path.getsize, reverse=True)
        if not got:
            # --max-filesize aborts by writing nothing, which is otherwise
            # indistinguishable from a silent success.
            return ("failed", None, f"nothing came back — it may be over {_mb(max_bytes)}", None)
        media = got[0]
        for extra in got[1:]:
            try:
                os.remove(extra)
            except OSError:
                pass

        ext = os.path.splitext(media)[1].lower()
        size = os.path.getsize(media)
        if size == 0:
            return ("failed", None, "the file came back empty", None)
        if size > max_bytes:
            return ("failed", None, f"it is {_mb(size)}; the cap is {_mb(max_bytes)}", None)

        name = f"{vid}{ext}"
        dest = resolve_name(mdir, name, bare=True)
        if not dest:
            return ("failed", None, "could not name the file safely", None)
        if os.path.exists(dest):
            return ("failed", None, "something is already at that name in the media tree", None)
        os.replace(media, dest)

        # The cover rides along or it does not. A song without one is a card
        # that falls back to YouTube's copy, which is what it was showing while
        # this ran — so a missing JPEG is not worth failing a download over.
        thumb_rel = None
        shot = next((f for f in glob.glob(stem + ".jpg")), None)
        if shot and os.path.getsize(shot) > 0:
            tdest = resolve_name(mdir, f"{vid}.jpg", bare=True)
            if tdest and not os.path.exists(tdest):
                try:
                    os.replace(shot, tdest)
                    thumb_rel = rel(f"{vid}.jpg")
                except OSError as e:
                    logger.warning("music fetch %s: kept the song, not the cover: %s", vid, e)

        logger.info("music fetch %s: %s, %s%s", vid, name, _mb(size),
                    ", with cover" if thumb_rel else "")
        return ("done", rel(name), None, {"bytes": size, "thumb_path": thumb_rel})

    except subprocess.TimeoutExpired:
        return ("failed", None, f"it was still going after {timeout // 60} minutes", None)
    finally:
        _scraps(mdir, job["id"])


HANDLERS = {"fetch": do_fetch, "promote": do_promote, "purge": do_purge,
            "rescan": do_rescan, "harvest": do_harvest,
            "music_probe": do_music_probe, "music_fetch": do_music_fetch}

_stop = False


def _on_signal(signum, _frame):
    """Finish what is in hand, then stop.

    Not an immediate exit: a promote interrupted between the rename and the
    report leaves the archive believing a file is still in quarantine that is
    not, and the whole point of the lease is that this end gets to finish a
    sentence.
    """
    global _stop
    _stop = True
    logger.info(f"signal {signum}: stopping after this job")


def handle(config: dict, job: dict) -> bool:
    kind = job.get("kind")
    fn = HANDLERS.get(kind)
    if not fn:
        ls_archive.report_job(config, job["id"], "failed",
                              error=f"this worker does not do {kind}")
        return False
    t0 = time.time()
    try:
        # Four, optionally. A handler that has findings to report — rescan does
        # — hands them back as the fourth; everything else answers in three and
        # this pads it, so no existing handler had to change.
        out = fn(config, job)
        status, result, err = out[0], out[1], out[2]
        found = out[3] if len(out) > 3 else None
    except Exception as e:
        # A handler that threw is a bug here, not a verdict on the job — but
        # the job still has to be answered, or it sits claimed until the lease
        # lapses and comes straight back to be crashed on again.
        logger.exception(f"{kind} {job['id'][:8]} crashed")
        status, result, err = "failed", None, f"the worker crashed: {type(e).__name__}: {e}"
        found = None
    if status != "done":
        logger.warning(f"{kind} {job['id'][:8]} failed: {err}")
    ls_archive.report_job(config, job["id"], status, result_path=result, error=err,
                          result=found)
    logger.debug(f"{kind} {job['id'][:8]} {status} in {time.time() - t0:.1f}s")
    return status == "done"


def preflight(config: dict, kinds, *, loud: bool = True) -> bool:
    """Resolve the roots and prove them before taking any work.

    Proved by writing, not by stat: a share that is mounted read-only, or
    mounted at all but with the wrong credentials, looks perfectly fine to
    os.path.isdir and fails on the first promote — after the archive has
    already been told the clip is approved.
    """
    ok = True

    def say(s):
        if loud:
            print(s)

    if not ls_archive.enabled(config):
        say("  archive_url / archive_token are not set — there is nothing to poll")
        return False
    say(f"  archive        {config['archive_url']}")

    m, q = media_root(config), quarantine_dir(config)
    # rescan reads the masters; it never writes to them.
    # music_fetch writes INTO the media tree rather than into quarantine — an
    # approved song is archive content and goes where the archive reads from.
    need_media = bool({"promote", "purge", "rescan", "music_fetch"} & set(kinds))
    need_q = bool({"promote", "fetch"} & set(kinds))

    for label, path, needed, why in (
            ("media root ", m, need_media,
             "set archive_media_root, or check that nas_path ends with archive_media_prefix"),
            ("quarantine ", q, need_q,
             "set archive_quarantine_dir")):
        if not needed:
            say(f"  {label}    (not needed for {','.join(kinds)})")
            continue
        if not path:
            say(f"  {label}    COULD NOT BE RESOLVED — {why}")
            ok = False
            continue
        note = ""
        if not os.path.isdir(path):
            note, ok = "  MISSING", False
        else:
            probe = os.path.join(path, f"{PART}preflight-{os.getpid()}")
            try:
                with open(probe, "wb") as f:
                    f.write(b"x")
                os.remove(probe)
            except OSError as e:
                note, ok = f"  NOT WRITABLE ({e.strerror})", False
        say(f"  {label}   {path}{note}")

    if ok and need_media and need_q:
        try:
            same = os.stat(m).st_dev == os.stat(q).st_dev
        except OSError:
            same = False
        say("  moves          rename(2), instant" if same else
            "  moves          DIFFERENT FILESYSTEMS — promote will copy the bytes")

    if "fetch" in kinds:
        say(f"  fetch hosts    {', '.join(setting(config, 'archive_fetch_hosts'))}")
        say(f"  fetch caps     {_mmss(int(setting(config, 'archive_fetch_max_s')))}"
            f", {setting(config, 'archive_fetch_max_mb')} MB")
    if "music_fetch" in kinds:
        d = music_dir(config)
        say(f"  music dir      {d}"
            + ("" if d and os.path.isdir(d) else "  WILL BE CREATED"))
        say(f"  music caps     {_mmss(int(setting(config, 'archive_music_max_s')))}"
            f", {setting(config, 'archive_music_max_mb')} MB")
        say(f"  music format   {setting(config, 'archive_music_format')}")
    # Asked once for whichever kinds need it. It used to hang off `fetch`
    # alone, which would have let a music-only worker start up clean and then
    # fail every job on a binary that was never there.
    if {"fetch", "music_probe", "music_fetch"} & set(kinds):
        if not shutil.which(_ytdlp(config)[0]):
            say(f"  yt-dlp         NOT FOUND at {_ytdlp(config)[0]}")
            ok = False
    spooled = ls_archive.spooled_reports()
    if spooled:
        say(f"  spool          {spooled} finished job(s) still to report")
    return ok


def run(config: dict, *, kinds, once: bool = False, interval: int | None = None) -> int:
    worker = str(config.get("archive_worker_name") or socket.gethostname())[:64]
    idle = int(interval or setting(config, "archive_job_interval"))
    done = 0
    logger.info(f"ls-jobs: {worker} polling for {','.join(kinds)} every {idle}s")
    while not _stop:
        # Before claiming, not after: a spooled report is about a job whose
        # lease may be about to lapse, and saying it is finished is what stops
        # the archive handing it out again.
        ls_archive.flush_reports(config)
        # One at a time. A fetch can run for minutes and the lease is five, so
        # claiming a handful would mean the ones waiting their turn lapse and
        # get handed out from under this worker.
        jobs = ls_archive.claim_jobs(config, worker=worker, kinds=kinds, limit=1)
        if not jobs:
            if once:
                break
            for _ in range(idle):
                if _stop:
                    break
                time.sleep(1)
            continue
        for job in jobs:
            if handle(config, job):
                done += 1
            if _stop:
                break
        if once:
            break
    return done


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="ls-jobs",
                                 description="Do what the tenma archive asks.")
    ap.add_argument("--once", action="store_true",
                    help="one pass through the queue, then exit")
    ap.add_argument("--check", action="store_true",
                    help="resolve and prove the roots, print them, exit")
    ap.add_argument("--kinds", default=None,
                    help=f"comma separated, from {','.join(ls_archive.PI_KINDS)}")
    ap.add_argument("--interval", type=int, default=None, help="seconds between polls")
    ap.add_argument("--config", default=None, help="path to config.json")
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S")

    try:
        config = ls_common.load_config(a.config)
    except Exception as e:
        print(f"config: {e}", file=sys.stderr)
        return 2

    raw = a.kinds or setting(config, "archive_job_kinds")
    wanted = raw.split(",") if isinstance(raw, str) else list(raw)
    kinds = [k.strip() for k in wanted if k.strip() in ls_archive.PI_KINDS]
    if not kinds:
        print(f"nothing to do: kinds must be some of {','.join(ls_archive.PI_KINDS)}",
              file=sys.stderr)
        return 2

    print(f"ls-jobs, doing {','.join(kinds)}")
    ready = preflight(config, kinds)
    if a.check:
        print("  ready" if ready else "  NOT READY")
        return 0 if ready else 1
    if not ready:
        print("  refusing to start — fix the above", file=sys.stderr)
        return 1

    for s in (signal.SIGTERM, signal.SIGINT):
        signal.signal(s, _on_signal)
    n = run(config, kinds=kinds, once=a.once, interval=a.interval)
    logger.info(f"ls-jobs: stopped after {n} job(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
