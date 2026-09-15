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
import unicodedata
import urllib.error
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
        # Where a tweet's PICTURES live. `twitter.com` above gets a tweet's
        # video through yt-dlp and can never get its images, because yt-dlp
        # has no image support — so the image address is what gets pasted,
        # and it is a plain file on a CDN like the two above it.
        "pbs.twimg.com",
    ],

    # ── music ────────────────────────────────────────────────────────────
    # Where songs land under the media root. The archive stores the path it is
    # told and never derives one, so this is the only place the word lives.
    "archive_music_prefix":    "music/",
    # Its own caps, because these are not the same thing as a pasted clip. A
    # music video is three minutes and worth keeping at a decent size; the
    # generous byte ceiling is the point of the module, not an oversight.
    #
    # ZERO MEANS NO CEILING, for both of these, and the duration one ships at
    # zero. A concert set is two hours and is exactly the thing this module is
    # for, so a cap that admits one says nothing useful about a three-minute
    # single: there is no number here that is right for both, which is why
    # there is no number. The fifteen minutes this used to be refused every
    # concert in the collection before a byte moved.
    "archive_music_max_s":     0,
    # Bytes stay a number, because bytes are the wall a mispasted twelve-hour
    # stream actually hits. Raised with the duration cap rather than left
    # behind it: two hours at 1080p is several GB, so 500 MB would have gone on
    # refusing the same concerts with a different sentence.
    "archive_music_max_mb":    8192,
    # Wall-clock for the whole yt-dlp run, merge included, and NOT zeroable: a
    # run with no timeout holds a lease it cannot renew, and the archive would
    # hand the same job to the next worker while this one is still pulling.
    # Well past the five-minute lease on purpose, for the same reason
    # archive_fetch_timeout_s is — with one worker there is nobody to steal the
    # job, and cutting a two-hour set short to satisfy a lease nobody is
    # contending for is the wrong trade.
    "archive_music_timeout_s": 7200,
    # THE FORMAT POLICY LIVES HERE, and that is deliberate: the archive names
    # an id and a verb and has never told a worker what to fetch. Change this
    # to `bestaudio/best` for an audio-only collection and nothing on the other
    # side needs to know.
    "archive_music_format":    "bv*[height<=1080]+ba/b[height<=1080]/b",
    "archive_music_container": "mp4",
}

# Fetched with a plain https GET rather than yt-dlp: these are direct file
# urls, not pages with a video somewhere in them.
#
# `pbs.twimg.com` joined them because a tweet is TWO different things behind
# one link. yt-dlp handles the video ones and has no image support at all, so
# an image-only post could never arrive however it was pasted — it came back
# "No video could be found in this tweet", which is also what X says when it
# is stonewalling a caller it does not recognise, so the failure read as rate
# limiting rather than as "that post has pictures in it". The picture itself
# is a plain file on a CDN, which is the case this branch already exists for.
DIRECT_HOSTS = ("cdn.discordapp.com", "media.discordapp.net", "pbs.twimg.com")

# The archive's own word for a transfer that has not finished. Its quarantine
# view reads this prefix and shows such a file as `unfinished` rather than as
# an orphan with no row — so a fetch that dies halfway looks like what it is.
PART = ".part-"

# Extensions a fetch may keep. Not a security control — the containment check
# is that — but a fetch that came back as a .html error page should not be
# handed to the archive as a snippet.
#
# The stills are here because memes and the gallery can be linked now, and a
# Discord attachment is the commonest way one arrives. Without them a pasted
# png was downloaded, renamed .mp4 by the DIRECT_HOSTS branch below — which
# falls back to .mp4 for anything not on this list — and handed to the archive
# as a video that is not one. The archive names the file after the snippet
# using this extension, so getting it wrong here is a wrong name on disk
# forever, not just a wrong guess in a log line.
KEEP_EXT = {".mp4", ".mkv", ".webm", ".mov", ".m4v", ".gif",
            ".m4a", ".mp3", ".opus", ".ogg", ".wav", ".flac",
            ".png", ".jpg", ".jpeg", ".webp"}

# What a SONG may be, which is not the same question.
#
# `do_music_fetch` asks yt-dlp for the audio and its cover in one pass, then
# picks the largest file it kept and DELETES the rest. While KEEP_EXT was
# media-only that was right. Adding the image extensions to it — correct for
# the fetch route, where a linked meme really is a .png — silently broke this:
# the cover became a candidate, so every music fetch deleted the JPEG it had
# just asked for, and a cover that happened to outweigh a short song was
# picked as the song itself. Two consumers, two questions, two lists.
MUSIC_EXT = {".mp4", ".mkv", ".webm", ".mov", ".m4v",
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

# The daemon's command socket. Named here rather than imported from ls_rec:
# that module is the recorder, and importing it would pull yt_dlp and a whole
# logging setup into a worker that wants neither.
RECORDER_SOCKET    = "/tmp/livestream-recorder.sock"
CLIP_ASK_TIMEOUT_S = 25     # planning is arithmetic; this is already generous
CLIP_WAIT_S        = 150    # bounded well inside the archive's 5-minute lease
CLIP_SETTLE_S      = 1.5    # an unchanged size for this long means ffmpeg is done


def _ask_recorder(config: dict, line: str) -> str:
    """One request to the daemon's command socket. Raises on anything."""
    path = str(config.get("recorder_socket") or RECORDER_SOCKET)
    if not os.path.exists(path):
        raise FileNotFoundError("the recorder is not running")
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(CLIP_ASK_TIMEOUT_S)
    try:
        s.connect(path)
        s.sendall(line.encode("utf-8"))
        chunks = []
        while True:
            b = s.recv(8192)
            if not b:
                break
            chunks.append(b)
    finally:
        s.close()
    return b"".join(chunks).decode("utf-8", "replace").strip()


def _clip_live(config: dict, job: dict, pay: dict, q: str, rel: str):
    """Cut from the part the recorder is writing right now.

    No arithmetic here, and for the opposite reason to the master path below.
    There the archive holds the clocks because it holds the capture rows; here
    the DAEMON holds them — which part file is open, what wall time its frame 0
    is, how close the live edge has crept — in memory nothing else can see. So
    this asks, the daemon's own planner answers, and a clip from the browser is
    the same cut as one typed at `ls-rec clip`.

    Returns a fourth element: what the cut actually turned out to be, so the
    archive can say "you asked for a minute and the last nineteen seconds had
    not happened yet" instead of quietly handing over a short file.
    """
    req = {
        "at":       pay.get("at_wall"),
        "length":   pay.get("duration_s"),
        "lead":     pay.get("lead_s"),
        "platform": pay.get("platform"),
        "name":     pay.get("label") or None,
    }
    if req["at"] is None or not req["length"]:
        return ("failed", None, "the job did not name a moment and a length")
    try:
        reply = _ask_recorder(config, "clipjob " + json.dumps(req))
    except Exception as e:
        return ("failed", None, f"could not reach the recorder: {type(e).__name__}")
    try:
        ans = json.loads(reply)
    except ValueError:
        return ("failed", None, f"the recorder said: {_tail(reply)}")
    if not ans.get("ok"):
        return ("failed", None, str(ans.get("error") or "the recorder refused the cut"))

    out = str(ans.get("out") or "")
    if not out:
        return ("failed", None, "the recorder named no output")

    # The cut runs on a thread in the daemon, so the file appears after the
    # reply does. Wait for it to exist and then to stop growing: ffmpeg writes
    # the moov atom last and +faststart rewrites the file to move it to the
    # front, so a clip taken mid-write is a clip that will not play.
    deadline = time.time() + CLIP_WAIT_S
    size, still = -1, 0.0
    while time.time() < deadline:
        try:
            grown = os.path.getsize(out)
        except OSError:
            time.sleep(0.5)
            continue
        if grown > 0 and grown == size:
            still += 0.5
            if still >= CLIP_SETTLE_S:
                break
        else:
            size, still = grown, 0.0
        time.sleep(0.5)
    else:
        return ("failed", None, "the recorder did not finish the cut in time")

    # Into quarantine under the name the archive asked for. clips_dir is on
    # this machine's own disk and quarantine sits beside the media tree, so
    # this is usually a cross-device move: copy to a `.part-` scrap, fsync,
    # rename into place, then drop the original. MOVED and not copied — the
    # archive asked for this cut and the archive's copy is the deliverable, so
    # clips_dir is not left holding a duplicate to be swept up later.
    dst = os.path.join(q, rel)
    tmp = os.path.join(q, f"{PART}{job['id']}.mp4")
    _scraps(q, job["id"])
    try:
        try:
            os.replace(out, dst)
        except OSError as e:
            if e.errno != errno.EXDEV:
                raise
            shutil.copyfile(out, tmp)
            with open(tmp, "rb+") as f:
                os.fsync(f.fileno())
            os.replace(tmp, dst)
            os.remove(out)
    except OSError as e:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return ("failed", None, f"could not move the clip: {e.strerror or e}")

    got = file_duration(dst)
    asked = float(ans.get("asked") or 0)
    cut = float(ans.get("length") or 0)
    logger.info(f"clip {rel}: live {_mmss(cut)} from part {ans.get('part')}"
                f" @ {_mmss(float(ans.get('offset') or 0))}"
                + (f" (got {_mmss(got)})" if got else "")
                + (f" — asked {_mmss(asked)}"
                   if asked and abs(asked - cut) >= 1 else ""))
    return ("done", rel, None, {
        "live":     True,
        "platform": ans.get("platform"),
        "part":     ans.get("part"),
        "offset_s": ans.get("offset"),
        "asked_s":  asked or None,
        "length_s": cut or None,
        "actual_s": got,
        "notes":    ans.get("notes") or [],
    })


def do_clip(config: dict, job: dict):
    """Cut a range out of a master into quarantine. (status, result_path, error).

    The archive has already worked out WHICH file and WHICH second — it holds
    the clocks — so this end does no arithmetic beyond the seek itself. That
    split is deliberate: the wall-clock-to-file-offset conversion is the part
    with the subtle bugs, it lives in one place, and that place has tests.

    Stream copy, never a re-encode. The point of cutting from the master is to
    get the master's own bytes: an encode would cost minutes of Pi CPU and
    hand back something worse than what is already on disk.

    `-ss` BEFORE `-i` so ffmpeg seeks rather than decoding to the start point —
    the difference between instant and several minutes on a four-hour file. It
    seeks to the nearest keyframe, so the real start can be a second or two off
    the number asked for. With a lead of any size that is invisible; it is the
    reason a zero-lead clip can open a moment late, and the reason not to
    "fix" it with an accurate seek that re-encodes.
    """
    pay = job.get("payload") or {}
    q = quarantine_dir(config)
    if not q or not os.path.isdir(q):
        return ("failed", None, "this worker has nowhere to put it")
    name = resolve_name(q, pay.get("name"), bare=True)
    if not name:
        return ("failed", None, "the archive named an output this worker will not write")
    rel = os.path.basename(name)

    # Two sources, one output, and the split is about who holds the clocks.
    # A promoted master is a file this worker can open and seek itself, and the
    # archive did the wall-clock arithmetic because the archive owns the
    # capture rows. The part being written right now is the other way round:
    # only the daemon knows which file is open and when its frame 0 was.
    if pay.get("live"):
        return _clip_live(config, job, pay, q, rel)

    m = media_root(config)
    src = resolve_name(m, pay.get("path"))
    if not src:
        return ("failed", None, "the archive named a file this worker will not touch")
    if not os.path.isfile(src):
        return ("failed", None, "that master is not on this recorder's mount")

    try:
        start = float(pay.get("start_s"))
        dur = float(pay.get("duration_s"))
    except (TypeError, ValueError):
        return ("failed", None, "the job did not name a start and a duration")
    if start < 0 or dur <= 0:
        return ("failed", None, "that is not a range")
    # A `.part-` name while it is being written, renamed when it is whole, for
    # the same reason a fetch does it: the archive's quarantine view reads that
    # prefix and shows an unfinished file as unfinished rather than as an
    # orphan it might serve half of.
    tmp = os.path.join(q, f"{PART}{job['id']}.mp4")
    _scraps(q, job["id"])

    cmd = ["ffmpeg", "-nostdin", "-loglevel", "error", "-y",
           "-ss", f"{start:.3f}", "-i", src, "-t", f"{dur:.3f}",
           "-c", "copy",
           # Without this the copied stream keeps the master's timestamps, so
           # the clip reports itself as starting two hours in and some players
           # show a two-hour-long file with nothing before the cut.
           "-avoid_negative_ts", "make_zero",
           "-movflags", "+faststart", tmp]
    try:
        r = _run(cmd, int(setting(config, "archive_fetch_timeout_s")))
    except subprocess.TimeoutExpired:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return ("failed", None, "ffmpeg took too long")
    if r.returncode != 0 or not os.path.isfile(tmp) or os.path.getsize(tmp) == 0:
        try:
            os.remove(tmp)
        except OSError:
            pass
        return ("failed", None, _tail(r.stderr) or _tail(r.stdout) or "ffmpeg wrote nothing")

    dst = os.path.join(q, rel)
    try:
        os.replace(tmp, dst)
    except OSError as e:
        return ("failed", None, f"could not name it: {e.strerror or e}")
    got = file_duration(dst)
    logger.info(f"clip {rel}: {_mmss(dur)} from {_mmss(start)} of "
                f"{os.path.basename(src)}"
                + (f" (got {_mmss(got)})" if got else ""))
    # The path is relative to quarantine, which is what the archive resolves
    # against when it serves the one download.
    return ("done", rel, None)


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

def _ytdlp(config: dict, cookies: bool = True) -> list[str]:
    """The binary, with this recorder's browser cookies on it.

    ls_common's builder, not a second copy of it: the venv path and the browser
    name are config and belong in one place, and a private name across two
    files of the same package is a smaller thing to carry than two argvs that
    drift.

    Cookied, and that is the point. Post-hoc VOD downloads have always been
    cookied — `ytdlp_vod_cmd` never asked — and this end was the outlier, on a
    rationale about a headless Pi with no browser to read cookies out of that
    was never true of THIS Pi: it records Twitch signed in on every stream. A
    members-only concert or a bot-checked video is a download that cannot
    happen any other way.

    `cookies=False` exists for one thing only, and it is not a public link: a
    jar that will not open. `--cookies-from-browser` is a hard failure rather
    than a degradation, so a locked, moved or missing profile would otherwise
    take down every download here — including the ones that never needed a
    session. See _ytdlp_run.
    """
    return ls_common._ytdlp_base(config, cookies=cookies)


def _run(cmd: list[str], timeout: int):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


# What yt-dlp says when the COOKIES are the problem rather than the video.
#
# Narrow on purpose. An anonymous retry is only ever the right answer when the
# jar could not be read; a members-only video retried without cookies fails a
# second time, spends the bandwidth twice, and reports the less useful of its
# two refusals — so everything not matched here is taken at its word.
#
# Which is why the flag's own name is NOT in here, tempting as it is. yt-dlp
# prints `--cookies-from-browser` as ADVICE inside the bot-check message —
# "Sign in to confirm you're not a bot. Use --cookies-from-browser ... for the
# authentication" — so a pattern that matched the flag would read a bot check
# as a broken jar, retry it with the cookies taken away, and report the one
# refusal of the two that the person reading it can do nothing about.
_COOKIE_TROUBLE = re.compile(
    r"could not (?:find|copy|open|read|decrypt)[^\n]{0,60}cookie"
    r"|cookie[s]?[^\n]{0,20}database"
    r"|cookies\.sqlite"
    r"|unsupported browser"
    r"|failed to decrypt",
    re.I)


def _ytdlp_run(config: dict, args: list[str], timeout: int):
    """yt-dlp, cookied, falling back to anonymous if the jar will not open.

    One entry point for every download this worker makes, so "use my cookies"
    is true of all of them and stays true of the next one somebody adds.

    The fallback is not a retry policy. It fires on a complaint about the
    cookies and on nothing else, and it is here so that a Firefox profile that
    has moved or locked degrades this worker to what it did before it was
    cookied instead of stopping it dead. The cookie complaint arrives before
    any bytes do — extraction has not started yet — so the second attempt is
    not a second download.
    """
    r = _run(_ytdlp(config) + args, timeout)
    if r.returncode == 0:
        return r
    said = (r.stderr or "") + "\n" + (r.stdout or "")
    if not _COOKIE_TROUBLE.search(said):
        return r
    logger.warning("yt-dlp could not read this recorder's cookies, so this one "
                   "goes out anonymous: %s", _tail(said))
    return _run(_ytdlp(config, cookies=False) + args, timeout)


def _tail(text: str, n: int = 300) -> str:
    """The last thing a tool said, cleaned up enough to put in front of a
    person. yt-dlp's real complaint is on the last non-empty line; everything
    above it is progress."""
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return ""
    msg = lines[-1]
    return re.sub(r"^ERROR:\s*", "", msg)[:n]


# yt-dlp's complaint, turned into something the person who pasted the link can
# act on. Only the ones that are ABOUT the link rather than about this end —
# everything else falls through to _tail unchanged, because a message nobody
# anticipated is more useful verbatim than flattened into "something failed".
#
# The X one is the reason this exists. Roughly one post in twenty comes back
# without its media when the caller is not recognised: X hands a guest
# incomplete JSON, yt-dlp finds no media in it and says so, and the sentence it
# says reads like a bug in the archive rather than what it is — a post this
# recorder cannot have. The recorder's cookies cover the sites it is signed in
# to on this Pi and X is not usually one of them, so the answer to that 1-in-20
# is to say so and name the way round it: download it yourself and upload it.
_EXPLAIN = (
    (r"No video could be found in this tweet",
     "there is no video on that post. If it is a picture you want, right-click "
     "the image and copy the IMAGE address — a pbs.twimg.com link — and paste "
     "that. If the post does have a video, X is stonewalling this recorder, and "
     "saving the file yourself and uploading it is the way through."),
    (r"NSFW tweet requires authentication|Requires authentication",
     "that post is behind a sign-in wall this recorder's cookies did not get "
     "past. Save the file yourself and upload it instead."),
    (r"Unable to find playlist|nothing to download",
     "there is no media on that page — check the link points at the post with "
     "the video in it, not at a reply or a profile."),
    (r"Private video|This video is private",
     "that one is private."),
    (r"Video unavailable|has been removed|no longer available",
     "that one is gone from the host."),
    (r"Sign in to confirm|age.?restricted|age.?gated",
     "the host wants an account before it will serve that one, and this "
     "recorder's cookies did not satisfy it."),
)


def _explain(raw: str) -> str:
    for pattern, said in _EXPLAIN:
        if re.search(pattern, raw, re.I):
            return said
    return raw


def probe_duration(config: dict, url: str) -> float | None:
    """Seconds, or None when the host will not say."""
    try:
        r = _ytdlp_run(config, ["--no-warnings", "--no-playlist", "--skip-download",
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


def _direct_ext(url: str) -> str:
    """What to call a directly-downloaded file, from the URL alone.

    The archive names the file after the snippet using this extension, so a
    wrong answer here is a wrong name on disk forever — which is why the old
    one-liner's `.mp4` fallback mattered: it was right for Discord, where the
    filename is in the path, and wrong for every host that does not put it
    there.

    `pbs.twimg.com` is that host. Twitter serves a picture from a path with no
    extension at all and says what it is in the query string
    (`/media/GxAbC?format=jpg&name=orig`), and from an older shape that puts
    the size after a colon (`/media/GxAbC.jpg:large`) — which `splitext` reads
    as the extension `.jpg:large`. Both are handled here rather than at the
    call site, because "what is this file" is one question however the host
    chooses to answer it.
    """
    u = urllib.parse.urlparse(url)
    # `.jpg:large` -> `.jpg`. Harmless anywhere a colon does not appear.
    ext = os.path.splitext(u.path)[1].lower().split(":")[0][:8]
    if ext in KEEP_EXT:
        return ext
    # Nothing usable in the path: ask the query string.
    fmt = (urllib.parse.parse_qs(u.query).get("format") or [""])[0].lower()
    fmt = "." + re.sub(r"[^a-z0-9]", "", fmt)[:8]
    if fmt == ".jpeg":
        fmt = ".jpg"
    return fmt if fmt in KEEP_EXT else ".mp4"


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
            tmp = stem + _direct_ext(url)
            err = _direct_download(url, tmp, max_bytes, timeout)
            if err:
                return ("failed", None, err)
        else:
            secs = probe_duration(config, url)
            if secs and secs > max_s:
                return ("failed", None,
                        f"that is {_mmss(secs)} long; the cap is {_mmss(max_s)}")
            r = _ytdlp_run(config, [
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
                return ("failed", None, _explain(
                    _tail(r.stderr) or _tail(r.stdout) or f"yt-dlp exited {r.returncode}"))
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

# The catalogue, and the one host its pictures come from. Two tuples and not
# one: the archive hands over an `art_url` it got from a search, and this end
# is what decides whether it will go there. Same rule as everywhere else in
# this file — the archive's copy of an allowlist is a courtesy to whoever is
# pasting, and this copy is the rule.
IGDB_API = "https://api.igdb.com/v4/games"
IGDB_ART_HOSTS = ("images.igdb.com",)
# A cover is tens of kilobytes. The cap is here so a redirect to something
# else cannot become this worker's disk problem.
POSTER_CAP = 8 << 20

IGDB_FIELDS = ("name,slug,summary,first_release_date,game_type,"
               "cover.image_id,platforms.abbreviation,url,total_rating_count")

# `game_type`, and NOT the `category` this was first written against. category
# is deprecated and simply stops being returned, which does not read as an
# error — every row came back `None` and that looked exactly like "these are
# all ordinary games". An absent field reads as a benign default, so a check
# that trusts one is not a weak check, it is no check. Verified against live
# answers: game_type reuses the old numbering.
GAME_TYPE = {0: "main game", 1: "dlc", 2: "expansion", 3: "bundle",
             4: "standalone expansion", 5: "mod", 6: "episode", 7: "season",
             8: "remake", 9: "remaster", 10: "expanded", 11: "port",
             12: "fork", 13: "pack", 14: "update"}

# Cached, because an IGDB token is good for about sixty days and fetching one
# per job would be a second use of the credential per tag for nothing. Not
# trusted to an expiry though — a 401 clears it and the call is retried once,
# which is correct whatever the server decides the lifetime is.
_IGDB_TOK = {"tok": None, "at": 0.0}
_IGDB_TOK_TTL = 12 * 3600


def _fold(s: str) -> str:
    """For deciding whether two NAMES are the same, and nothing else.

    IGDB's `where name = "x"` is case-sensitive, so the tag `The World Ends
    With You` missed the catalogue's `The World Ends with You` over one letter
    and the answer came back labelled as a guess when it was the right row.
    NFKD then drop the combining marks, so Pokemon and Pokémon compare equal
    too.
    """
    d = unicodedata.normalize("NFKD", (s or "").casefold())
    return "".join(c for c in d if not unicodedata.combining(c)).strip()


def _igdb_token(config: dict, *, force: bool = False) -> str | None:
    now_s = time.time()
    if not force and _IGDB_TOK["tok"] and now_s - _IGDB_TOK["at"] < _IGDB_TOK_TTL:
        return _IGDB_TOK["tok"]
    # IGDB authenticates through Twitch, so this is the credential the recorder
    # already holds and ls_common already knows how to exchange. No second
    # account, and no second copy of the grant flow.
    tok = ls_common.twitch_get_token(config)
    if tok:
        _IGDB_TOK.update(tok=tok, at=now_s)
    return tok


def _igdb(config: dict, body: str, *, retry: bool = True):
    """One Apicalypse POST. The body IS the query language, not JSON."""
    cid = config.get("igdb_client_id") or config.get("twitch_client_id")
    tok = _igdb_token(config)
    if not (cid and tok):
        return None
    req = urllib.request.Request(
        IGDB_API, data=body.encode("utf-8"), method="POST",
        headers={"user-agent": "ls-rec/jobs", "accept": "application/json",
                 "Client-ID": cid, "Authorization": f"Bearer {tok}"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read(2 << 20).decode("utf-8", "replace"))
    except urllib.error.HTTPError as e:
        if e.code == 401 and retry:
            # The cached token has expired or been revoked. One forced refresh
            # and one more go; a second 401 is a real answer.
            _igdb_token(config, force=True)
            return _igdb(config, body, retry=False)
        logger.warning("igdb HTTP %s: %s", e.code,
                       e.read().decode("utf-8", "replace")[:200] if hasattr(e, "read") else "")
        return None
    except Exception as e:
        logger.warning("igdb %s: %s", type(e).__name__, str(e)[:160])
        return None


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
    """Three errands on one kind. Returns (status, result_path, error, findings).

    A harvest used to mean one thing — read a wiki page named by a url — and
    it is three now, told apart by what the payload carries:

        q        look this name up in the catalogue -> candidates
        art_url  fetch this cover -> a file in the media tree
        url      read this wiki article -> its lead paragraph

    ONE KIND AND NOT THREE, deliberately. A new kind has to be added to
    JOB_KINDS in the archive, to PI_KINDS in the archive, to PI_KINDS in
    ls_archive.py, AND to `archive_job_kinds` in this machine's config — and
    that last one is how a clip sat WAITING for ever while this worker
    cheerfully took everything else. A kind that already travels cannot fall
    into that.

    Nothing here decides anything. A search returns what the catalogue said
    and the archive parks it for a person to pick from; three different games
    are called Summer Camp and no amount of matching tells you which one a
    stream was about.
    """
    pay = job.get("payload") or {}
    if str(pay.get("art_url") or "").strip():
        return _harvest_art(config, pay)
    if str(pay.get("q") or "").strip():
        return _harvest_search(config, pay)
    return _harvest_wiki(config, pay, job)


def _harvest_search(config: dict, pay: dict):
    """What the catalogue has under a name. Never a verdict, always a list."""
    q = str(pay.get("q") or "").strip()[:120]
    if not q:
        return ("failed", None, "the job names nothing to look up", None)
    if not (config.get("igdb_client_id") or config.get("twitch_client_id")):
        return ("failed", None,
                "no igdb credentials on this recorder — set igdb_client_id and "
                "igdb_client_secret, or the twitch_ pair, in config.json", None)

    esc = q.replace("\\", "\\\\").replace('"', '\\"')
    rows = _igdb(config, f'fields {IGDB_FIELDS}; where name = "{esc}"; limit 5;')
    if rows is None:
        return ("failed", None, "the catalogue did not answer", None)
    served_exact = bool(rows)
    if not rows:
        rows = _igdb(config, f'search "{esc}"; fields {IGDB_FIELDS}; limit 8;') or []

    out = []
    for g in rows:
        img = (g.get("cover") or {}).get("image_id")
        plats = ",".join(sorted({p.get("abbreviation") or "?"
                                 for p in (g.get("platforms") or [])}))
        ts = g.get("first_release_date")
        yr = time.strftime("%Y", time.gmtime(ts)) if ts else "????"
        gt = g.get("game_type")
        out.append({
            "name": g.get("name") or "",
            # 1:1. IGDB's summary is a paragraph and a paragraph is what the
            # tag card wants, so nothing here truncates it.
            "summary": " ".join((g.get("summary") or "").split()),
            "art": (f"https://images.igdb.com/igdb/image/upload/t_cover_big_2x/"
                    f"{img}.jpg") if img else None,
            "url": g.get("url"),
            "meta": (f"{yr} · {GAME_TYPE.get(gt, 'type ' + str(gt))}"
                     f" · {g.get('total_rating_count') or 0} ratings"
                     + (f" · {plats[:46]}" if plats else "")),
            "_gt": gt if isinstance(gt, int) else 99,
            "_rank": int(g.get("total_rating_count") or 0),
            "same": _fold(g.get("name")) == _fold(q),
        })

    if not served_exact and out:
        # Sorted HERE and not asked for: `sort` has no effect alongside
        # `search`, and a client-side sort is right whatever the server does.
        # Exact name, then MAIN GAMES above the rest — which only became
        # possible once game_type started working, and which matters when a
        # popular expansion out-rates the game it expands — then how many
        # people rated it. Demoted, never dropped: a collab event is a thing
        # somebody may well have streamed.
        out.sort(key=lambda c: (not c["same"], c["_gt"] != 0, -c["_rank"]))

    # How many carry EXACTLY this name. Three games are called Summer Camp, so
    # a bare "exact" over the first of them is a picker choosing and not
    # saying so.
    twins = sum(1 for c in out if c["same"])
    for c in out:
        c["exact"] = served_exact or c["same"]
        c["twins"] = twins
        c.pop("_gt", None)
        c.pop("_rank", None)

    logger.info("harvest %r: %d candidate(s)%s", q, len(out),
                " (exact)" if served_exact else "")
    # `done` with an empty list is the right answer for a person's name. It is
    # not a failure and the panel should not draw it as one.
    return ("done", None, None, {"candidates": out, "q": q})


def _harvest_art(config: dict, pay: dict):
    """One cover, into the media tree at the name the archive minted."""
    url = str(pay.get("art_url") or "").strip()
    rel = str(pay.get("art_to") or "").strip()
    host = urllib.parse.urlparse(url).hostname or ""
    if not url.lower().startswith("https://") or not any(
            host == h or host.endswith("." + h) for h in IGDB_ART_HOSTS):
        return ("failed", None, "this worker does not fetch pictures from there", None)
    target = resolve_name(media_root(config), rel)
    if not target or not re.fullmatch(r"posters/[0-9A-HJKMNP-TV-Z]{26}\.(jpg|png)", rel):
        return ("failed", None, "the archive named a file this worker will not write", None)

    # Named, and caught. Under `ProtectSystem=strict` the unit only gets to
    # write the paths its ReadWritePaths lists, so a media root that is
    # readable and a posters/ that is not is the likely shape of a failure
    # here — and "Read-only file system" with a traceback is a much worse
    # answer than the directory's own name. handle() would catch a raise and
    # report it, but it would report it as a worker crash.
    try:
        os.makedirs(os.path.dirname(target), exist_ok=True)
    except OSError as e:
        return ("failed", None,
                f"cannot write {os.path.dirname(target)}: {e.strerror or e} — if "
                f"ls-jobs runs under ProtectSystem=strict, that directory needs "
                f"a ReadWritePaths entry", None)
    part = target + ".part"
    req = urllib.request.Request(url, headers={"user-agent": "ls-rec/jobs"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            blob = r.read(POSTER_CAP + 1)
    except Exception as e:
        return ("failed", None, f"could not fetch the cover: {type(e).__name__}", None)
    if not blob:
        return ("failed", None, "the cover came back empty", None)
    if len(blob) > POSTER_CAP:
        return ("failed", None, f"that cover is over {_mb(POSTER_CAP)}", None)
    # The bytes decide, not the extension and not the content-type — both of
    # those are things the sender says, and the archive will serve this file by
    # the name it now holds.
    if not (blob[:2] == b"\xff\xd8" or blob[:8] == b"\x89PNG\r\n\x1a\n"):
        return ("failed", None, "that is not a JPEG or a PNG", None)
    try:
        with open(part, "wb") as fh:
            fh.write(blob)
        # Atomic, so the archive can never point a row at half a picture.
        os.replace(part, target)
    except OSError as e:
        try:
            os.unlink(part)
        except OSError:
            pass
        return ("failed", None, f"could not store the cover: {e.strerror or e}", None)

    logger.info("harvest art %s: %s", rel, _mb(len(blob)))
    return ("done", rel, None, None)


def _harvest_wiki(config: dict, pay: dict, job: dict):
    """Read a wiki page for a tag.

    Kept, and kept reachable, though the archive does not queue these yet: a
    MediaWiki lead paragraph is the right description for a person and the
    wrong one for a game, and `virtualyoutuber.fandom.com` is where the indie
    VTubers are. Reachable rather than commented out because unreachable code
    rots — a harvest carrying `url` still lands here today.

    Nothing is decided here either. The archive stores what comes back as
    `seeded`, which is the row saying out loud that a machine wrote it and no
    human has been over it; the first hand edit clears that flag.
    """
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
    # Either cap set to zero is no cap at all, and the floor is there so a
    # negative in somebody's config reads as "off" rather than as a ceiling
    # that nothing can come in under.
    max_bytes = max(0, int(setting(config, "archive_music_max_mb"))) * 1048576
    max_s = max(0, int(setting(config, "archive_music_max_s")))
    timeout = int(setting(config, "archive_music_timeout_s"))

    try:
        os.makedirs(mdir, exist_ok=True)
    except OSError as e:
        return ("failed", None, f"cannot make {mdir}: {e.strerror or e}", None)

    # MUSIC_EXT, so the cover sitting beside the song is not mistaken for it.
    # Sorted, so `.jpg` came first and a re-run of a song that HAD a cover
    # reported the cover as the song's own path.
    existing = sorted(glob.glob(os.path.join(mdir, f"{vid}.*")))
    existing = [f for f in existing
                if os.path.splitext(f)[1].lower() in MUSIC_EXT and os.path.getsize(f) > 0]
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
    # can be checked without spending the bandwidth it is protecting. Skipped
    # entirely when there is no duration cap — a probe whose only reader is a
    # comparison that cannot fail is a round trip for nothing.
    if max_s:
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
        r = _ytdlp_run(config, [
            "--no-warnings", "--no-playlist", "--no-progress",
            "-f", str(setting(config, "archive_music_format")),
            "--merge-output-format", str(setting(config, "archive_music_container")),
            # The cover, in the same pass. A second job for one JPEG would be a
            # second thing to fail, and the archive falls back to YouTube's own
            # thumbnail url until this lands anyway.
            "--write-thumbnail", "--convert-thumbnails", "jpg",
            *(["--max-filesize", f"{max_bytes}"] if max_bytes else []),
            "-o", stem + ".%(ext)s", url], timeout)
        if r.returncode != 0:
            return ("failed", None, _explain(
                _tail(r.stderr) or _tail(r.stdout) or f"yt-dlp exited {r.returncode}"), None)

        got = sorted((f for f in glob.glob(stem + ".*")
                      if os.path.splitext(f)[1].lower() in MUSIC_EXT),
                     key=os.path.getsize, reverse=True)
        if not got:
            # --max-filesize aborts by writing nothing, which is otherwise
            # indistinguishable from a silent success — so it is named as the
            # likely reason only when it was a flag that went in.
            return ("failed", None,
                    (f"nothing came back — it may be over {_mb(max_bytes)}" if max_bytes
                     else "nothing came back, and yt-dlp said it went fine"), None)
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
        if max_bytes and size > max_bytes:
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
            "music_probe": do_music_probe, "music_fetch": do_music_fetch,
            "clip": do_clip}

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
    # `harvest` is in here now: it writes a tag's cover into posters/ under the
    # media root, which it did not used to do because it never fetched art at
    # all. `clip` is here for the same reason and was missed — it reads a
    # master off the media root and writes the cut into quarantine, so
    # `ls-jobs --kinds clip` used to pass preflight without checking either
    # root and then fail on the job.
    need_media = bool({"promote", "purge", "rescan", "music_fetch",
                       "harvest", "clip"} & set(kinds))
    need_q = bool({"promote", "fetch", "clip"} & set(kinds))

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
        # Printed as "no limit" rather than as 0:00 / 0 MB, because a cap of
        # zero is the one this module ships with and a preflight that reads
        # "0:00" looks like the reason nothing downloads.
        m_s = max(0, int(setting(config, "archive_music_max_s")))
        m_mb = max(0, int(setting(config, "archive_music_max_mb")))
        say(f"  music caps     {_mmss(m_s) if m_s else 'no length limit'}"
            f", {f'{m_mb} MB' if m_mb else 'no size limit'}"
            f", gives up after {int(setting(config, 'archive_music_timeout_s')) // 60} min")
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
    logger.info(f"ls-jobs: {worker} polling for {','.join(kinds)}, holding each "
                f"claim open up to {min(idle, 25)}s")
    while not _stop:
        # Before claiming, not after: a spooled report is about a job whose
        # lease may be about to lapse, and saying it is finished is what stops
        # the archive handing it out again.
        ls_archive.flush_reports(config)
        # One at a time. A fetch can run for minutes and the lease is five, so
        # claiming a handful would mean the ones waiting their turn lapse and
        # get handed out from under this worker.
        #
        # `wait` asks the archive to hold the request open instead of answering
        # "nothing" — so work starts when it is queued rather than up to
        # `idle` seconds later. That matters for anything a person is sitting
        # in front of: a clip cut, or looking a tag up in the catalogue.
        #
        # Bounded by `idle` so `--interval 5` still means "do not sit on a
        # socket longer than five seconds", and by 25 because that is the
        # ceiling the archive enforces anyway. `--once` waits for nothing: it
        # is a single pass over what is already there.
        hold = 0 if once else max(0, min(idle, 25))
        jobs = ls_archive.claim_jobs(config, worker=worker, kinds=kinds, limit=1,
                                     wait=hold)
        if not jobs:
            if once:
                break
            # Already waited inside the claim, so sleeping `idle` again on top
            # of it would double the latency this is here to remove. A short
            # breath instead, which is also what keeps a server that ignores
            # `wait` from becoming a busy loop.
            for _ in range(1 if hold else idle):
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
