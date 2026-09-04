#!/usr/bin/env python3
"""Do what the archive asks. One job at a time, on this machine's terms.

The archive holds no write handle inside the media tree — that is the whole
point of the arrangement, and it is why /media is mounted read-only in its
compose file. So when a clip is approved, or purged, or pasted in as a link,
the archive cannot act. It writes down what it wants and this worker comes and
takes it.

    fetch     a url someone pasted -> a file in quarantine, for review
    promote   an approved file     -> quarantine into the media tree
    purge     an admin said so     -> gone

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
import errno
import glob
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

def media_root(config: dict) -> str | None:
    explicit = str(config.get("archive_media_root") or "").strip()
    if explicit:
        return os.path.abspath(os.path.expanduser(explicit))
    nas = str(config.get("nas_path") or "").rstrip("/")
    if not nas:
        return None
    root = os.path.abspath(os.path.expanduser(nas))
    prefix = str(config.get("archive_media_prefix") or "")
    for part in reversed([p for p in prefix.split("/") if p]):
        # The prefix does not describe nas_path. Say nothing rather than guess.
        if os.path.basename(root) != part:
            return None
        root = os.path.dirname(root)
    return root


def quarantine_dir(config: dict) -> str | None:
    explicit = str(config.get("archive_quarantine_dir") or "").strip()
    if explicit:
        return os.path.abspath(os.path.expanduser(explicit))
    m = media_root(config)
    return os.path.join(os.path.dirname(m), "quarantine") if m else None


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

HANDLERS = {"fetch": do_fetch, "promote": do_promote, "purge": do_purge}

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
        status, result, err = fn(config, job)
    except Exception as e:
        # A handler that threw is a bug here, not a verdict on the job — but
        # the job still has to be answered, or it sits claimed until the lease
        # lapses and comes straight back to be crashed on again.
        logger.exception(f"{kind} {job['id'][:8]} crashed")
        status, result, err = "failed", None, f"the worker crashed: {type(e).__name__}: {e}"
    if status != "done":
        logger.warning(f"{kind} {job['id'][:8]} failed: {err}")
    ls_archive.report_job(config, job["id"], status, result_path=result, error=err)
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
    need_media = bool({"promote", "purge"} & set(kinds))
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
