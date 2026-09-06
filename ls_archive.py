"""Post what the recorder sees to the tenma archive.

Two packets on one endpoint, plus a heartbeat on another.
`POST /api/ingest/capture` upserts on (platform, remote_id), so a start and a
completion are the same call twice and replaying either is free — there is no
ordering requirement and no way to make a duplicate.

`POST /api/ingest/live` is the odd one out and is documented at post_live():
it is the only call here that must NOT be retried.

The recorder is the ONLY thing that is present at both of these moments:

    broadcast_started_at   the wall time of the platform player's t=0
    record_started_at      the wall time of this file's frame 0

Nothing recovers those afterwards. The archive's 371 imported captures have
both columns NULL because the importer never had them, and mtime arithmetic is
a guess. Everything else in a packet can be corrected by hand later; these two
cannot.

Nothing in here may ever break a recording. Every call is wrapped, the timeout
is short, and a packet that cannot be delivered goes to a small on-disk outbox
and is retried on the next poll tick.
"""

from __future__ import annotations

import datetime
import itertools
import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid

logger = logging.getLogger(__name__)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTBOX_PATH = os.path.join(SCRIPT_DIR, ".archive_outbox.json")

TIMEOUT = 6          # seconds; the archive is never worth waiting on
OUTBOX_MAX = 500     # packets; beyond this something is wrong, not backlogged

PLATFORM = {"youtube": "YT", "twitch": "TW"}


def enabled(config: dict) -> bool:
    return bool(config.get("archive_url") and config.get("archive_token"))


def _media_prefix(config: dict) -> str:
    """Where the NAS sits inside TENMA_MEDIA_ROOT.

    The archive stores media paths relative to its own root and the vault used
    `raws/`, so a file uploaded to nas_path/NNN_title.mp4 is `raws/NNN_title.mp4`
    to the archive. Getting this wrong does not error — it makes every capture
    read `lost`, which is the worst possible way to be wrong quietly.
    """
    p = config.get("archive_media_prefix", "raws/")
    return p if not p or p.endswith("/") else p + "/"


def archive_path(config: dict, filename: str | None) -> str | None:
    """A NAS filename as the archive refers to it. None stays None."""
    return f"{_media_prefix(config)}{filename}" if filename else None


def media_root(config: dict) -> str | None:
    """The archive's media root, as this machine sees it.

    `nas_path` is where raws are uploaded and `archive_media_prefix` says where
    that sits inside the root, so the root is nas_path with the prefix walked
    back off it. Overridable for the cases where that guess is wrong.

    Returns None rather than a guess when the prefix does not describe
    nas_path: everything built on this writes files, and the failure mode of a
    wrong answer here is a directory of pictures nothing can find.
    """
    explicit = str(config.get("archive_media_root") or "").strip()
    if explicit:
        return os.path.abspath(os.path.expanduser(explicit))
    nas = str(config.get("nas_path") or "").rstrip("/")
    if not nas:
        return None
    root = os.path.abspath(os.path.expanduser(nas))
    for part in reversed([p for p in _media_prefix(config).split("/") if p]):
        if os.path.basename(root) != part:
            return None
        root = os.path.dirname(root)
    return root


def _tz_offset_min() -> int:
    off = datetime.datetime.now().astimezone().utcoffset()
    return int(off.total_seconds() // 60) if off else 0


# ── the outbox ────────────────────────────────────────────────────────────

def _load_outbox() -> list[dict]:
    try:
        with open(OUTBOX_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning(f"archive outbox unreadable, starting a new one: {e}")
        return []


def _save_outbox(items: list[dict]) -> None:
    try:
        if not items:
            if os.path.exists(OUTBOX_PATH):
                os.remove(OUTBOX_PATH)
            return
        tmp = OUTBOX_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items[-OUTBOX_MAX:], f, ensure_ascii=False, indent=1)
        os.replace(tmp, OUTBOX_PATH)
    except Exception as e:
        logger.warning(f"could not write the archive outbox: {e}")


def _queue(body: dict, why: str) -> None:
    items = _load_outbox()
    items.append({"queued_at": int(datetime.datetime.now().timestamp()), "body": body})
    _save_outbox(items)
    logger.warning(f"archive unreachable ({why}); queued packet "
                   f"{body.get('platform')} {body.get('remote_id')} "
                   f"({len(items)} waiting)")


# ── the wire ──────────────────────────────────────────────────────────────

def _headers(config: dict) -> dict:
    return {"content-type": "application/json",
            "authorization": f"Bearer {config['archive_token']}"}


def _post(config: dict, body: dict, *, path: str = "/api/ingest/capture") -> dict | None:
    """One request. Returns the parsed response, or None on any failure."""
    url = config["archive_url"].rstrip("/") + path
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers=_headers(config))
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8") or "{}")


def _get(config: dict, path: str, params: list[tuple[str, str]]) -> dict | None:
    """One read. Raises on failure — reads are never queued, they are retried
    by the human running the command again."""
    url = (config["archive_url"].rstrip("/") + path + "?"
           + urllib.parse.urlencode(params))
    req = urllib.request.Request(url, headers=_headers(config))
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8") or "{}")


def _send(config: dict, body: dict, *, queue_on_failure: bool = True) -> dict | None:
    if not enabled(config):
        return None
    try:
        return _post(config, body)
    except urllib.error.HTTPError as e:
        # 4xx is the archive saying the packet is wrong. Retrying an identical
        # bad packet forever is worse than dropping it, so only 5xx queues.
        detail = ""
        try:
            detail = e.read().decode("utf-8", "replace")[:200]
        except Exception:
            pass
        if 500 <= e.code < 600 and queue_on_failure:
            _queue(body, f"HTTP {e.code}")
        else:
            logger.error(f"archive refused the packet: HTTP {e.code} {detail}")
        return None
    except Exception as e:
        if queue_on_failure:
            _queue(body, type(e).__name__)
        else:
            logger.warning(f"archive unreachable: {e}")
        return None


def flush(config: dict) -> int:
    """Send anything queued, oldest first. Safe to call on every tick.

    Stops at the first failure rather than hammering a server that is down, and
    leaves the rest in place — the upsert means order does not matter, but
    sending fifty packets into a timeout would stall the poll loop.
    """
    if not enabled(config):
        return 0
    items = _load_outbox()
    if not items:
        return 0
    sent = 0
    for i, item in enumerate(items):
        if _send(config, item["body"], queue_on_failure=False) is None:
            _save_outbox(items[i:])
            if sent:
                logger.info(f"archive: flushed {sent}, {len(items) - sent} still waiting")
            return sent
        sent += 1
    _save_outbox([])
    logger.info(f"archive: flushed {sent} queued packet(s)")
    return sent


# ── the two packets ───────────────────────────────────────────────────────

def post_start(config: dict, *, platform: str, video_id: str, title: str,
               url: str, obsidian_index: int | None,
               broadcast_started_at: int | None,
               record_started_at: int) -> dict | None:
    """A recording has begun. Creates the stream, or joins the one the other
    platform already made.

    `broadcast_started_at` is omitted when unknown rather than filled in with
    the record start. They differ by however long the probe took to notice, and
    the archive would rather hold NULL — meaning "nobody measured this" — than a
    number that looks measured.
    """
    body = {
        "platform": PLATFORM.get(platform, platform.upper()[:2]),
        "remote_id": video_id,
        "title": title,
        "url": url,
        "record_started_at": int(record_started_at),
        "tz_offset_min": _tz_offset_min(),
    }
    if obsidian_index:
        body["index"] = int(obsidian_index)
    if broadcast_started_at:
        body["broadcast_started_at"] = int(broadcast_started_at)
        # started_at seeds the stream row's own clock and is what the server
        # pairs the two platforms on. The broadcast start is the better number
        # for both; without it the server falls back to now(), which is within
        # a poll interval and correctable by hand.
        body["started_at"] = int(broadcast_started_at)
    r = _send(config, body)
    if r:
        logger.info(f"archive: {'created' if r.get('created') else 'updated'} "
                    f"#{r.get('index')}"
                    + (f" (paired with {r['paired_with']})" if r.get("paired_with") else ""))
    return r


def post_done(config: dict, *, platform: str, video_id: str,
              stream_title: str | None = None,
              duration_seconds: float | None = None,
              video_ext: str = ".mp4",
              have_video: bool = False,
              have_chat: bool = False) -> dict | None:
    """The wrapup finished. Sends only what is now true.

    Called even when nothing was uploaded — a broadcast that happened and has no
    file is the case the archive most wants to hear about, and it is exactly the
    case that used to send nothing at all. With no paths, recompute() marks the
    capture unverified rather than present, which is the honest answer.
    """
    body = {
        "platform": PLATFORM.get(platform, platform.upper()[:2]),
        "remote_id": video_id,
    }
    if stream_title:
        prefix = _media_prefix(config)
        ext = video_ext if video_ext.startswith(".") else f".{video_ext or 'mp4'}"
        if have_video:
            body["video_path"] = f"{prefix}{stream_title}{ext}"
        if have_chat:
            body["chat_path"] = f"{prefix}{stream_title}.json"
    if duration_seconds:
        body["duration_s"] = int(duration_seconds)
    r = _send(config, body)
    if r:
        logger.info(f"archive: wrapped #{r.get('index')} "
                    f"vod={r.get('vod_state')} chat={r.get('chat_state')}")
    return r


# ── the heartbeat ─────────────────────────────────────────────────────────
#
# Not a packet in the sense the other two are, and the difference decides
# everything about how it is sent.
#
# post_start and post_done record things that HAPPENED. They must eventually
# land, nothing else in the world knows their numbers, and so they queue.
#
# This one records what is true RIGHT NOW, and a heartbeat that arrives late is
# worse than one that never arrives at all: replaying a queued backlog would
# walk the archive through a series of "she is live" claims that are every one
# of them already false. So it never queues, never retries, and a failure is a
# debug line rather than a warning — a long broadcast with an unreachable
# archive would otherwise write one complaint per minute for five hours.
#
# The archive needs no help recovering. Every beat carries the FULL set of what
# is recording, including the empty set, so a lost beat cannot strand a stuck
# LIVE badge, and whatever it last heard expires on its own after three
# intervals. There is no "stopped" event to send, and therefore none to forget.

_BOOT_ID = uuid.uuid4().hex[:16]    # new on every process start
_seq = itertools.count()
_live_err: str | None = None        # last failure, so it is logged once not forever


def post_live(config: dict, active_streams: dict, *,
              interval_s: int | None = None) -> bool:
    """Tell the archive what is being recorded at this instant.

    `active_streams` is the recorder's own dict, passed straight in — every
    field below already lives there, so this adds no state to track and cannot
    drift from what is really running.
    """
    global _live_err
    if not enabled(config):
        return False

    recording = []
    for s in (active_streams or {}).values():
        try:
            recording.append({
                "platform":  PLATFORM.get(s["platform"], str(s["platform"]).upper()[:2]),
                "remote_id": s["identifier"],
                # The archive's own stream index, so it can resolve this to the
                # row post_start created without guessing.
                "index":     s.get("obsidian_index"),
                "title":     s.get("obsidian_title"),
                "url":       s.get("url"),
                # Sent for completeness; the archive prefers its OWN started_at
                # for anything it measures against, and rightly so.
                "broadcast_started_at": (int(s["_stream_start_epoch"])
                                         if s.get("_stream_start_epoch") else None),
                "record_started_at": (int(s["_record_start_epoch"])
                                      if s.get("_record_start_epoch") else None),
                # Recording, but the file has stopped growing. Still live —
                # the badge should stay lit — but the archive is told the
                # difference rather than left to assume all is well.
                "stalled":   bool(s.get("_watchdog_triggered")),
            })
        except Exception as e:
            # One malformed stream must not cost the whole heartbeat.
            logger.debug(f"archive: leaving a stream out of the heartbeat: {e}")

    body = {
        "boot_id":    _BOOT_ID,
        "seq":        next(_seq),
        "sent_at":    int(datetime.datetime.now().timestamp()),
        # What the archive multiplies by three to decide we have gone quiet.
        # Sent rather than configured there, so changing the poll interval here
        # does not silently start flapping the badge between beats.
        "interval_s": int(interval_s or config.get("check_interval", 60)),
        "recording":  recording,
    }

    try:
        _post(config, body, path="/api/ingest/live")
        _live_err = None
        return True
    except urllib.error.HTTPError as e:
        # 4xx means the packet itself is wrong, which is a bug here and not a
        # network condition. Worth saying out loud — but once, not every minute.
        detail = ""
        try:
            detail = e.read().decode("utf-8", "replace")[:200]
        except Exception:
            pass
        msg = f"HTTP {e.code} {detail}"
        if msg != _live_err:
            _live_err = msg
            logger.warning(f"archive refused the heartbeat: {msg}")
        return False
    except Exception as e:
        msg = type(e).__name__
        if msg != _live_err:
            _live_err = msg
            logger.debug(f"archive heartbeat not delivered: {e}")
        return False


# ── reconciliation, for ls-audit ──────────────────────────────────────────
#
# The recorder posts blind, and that is right for it: it is the only witness,
# there is nothing to compare against, and the numbers it holds exist nowhere
# else. ls-audit is the opposite. It reconstructs long after the fact from
# files, caches and log lines, and some of what it reconstructs is *worse* than
# what the recorder already wrote down — a filename gives a start time to the
# minute where the recorder measured it to the second.
#
# So this half reads first, diffs, and puts anything that collides in front of
# a human as before → after. Three outcomes are decided without asking, because
# there is nothing to ask about:
#
#   new     the archive holds nothing        → write it
#   same    the two agree                    → skip
#   refine  more precise, and inside the old → write it
#           value's own margin of error
#
# Everything else is a collision and gets prompted. A non-interactive caller
# (a batch audit) rejects collisions rather than guessing, so the worst a sweep
# can do is fill in blanks.

# The archive's capture column for each field we send.
_CAP_COLUMN = {
    "url": "url", "title": "title",
    "video_path": "video_path", "chat_path": "chat_path",
    "duration_s": "file_duration_s",
    "broadcast_started_at": "remote_start_wall",
    "record_started_at": "local_start_wall",
}
_CLOCK_PRECISION = {"record_started_at": "local_start_precision_s"}

# Durations come from ffprobe on one side and a stored integer on the other.
_TOLERANCE = {"duration_s": 2}


def _norm_platform(p: str) -> str:
    return PLATFORM.get(p, str(p).upper()[:2])


def lookup(config: dict, *, idx: int | None = None,
           ids: list[tuple[str, str]] = ()) -> dict | None:
    """What the archive currently holds for this entry.

    `ids` is [(platform, remote_id), ...]. Matching prefers remote_id — it is
    the identity anchor — and falls back to the vault index. When the two
    disagree the response carries a `conflict` and the caller must stop: that
    is the two systems having drifted apart, and picking a winner
    automatically is how one of them silently loses.
    """
    if not enabled(config):
        return None
    params = []
    if idx is not None:
        params.append(("idx", str(idx)))
    for platform, remote in ids:
        if remote:
            params.append(("id", f"{_norm_platform(platform)}:{remote}"))
    try:
        return _get(config, "/api/ingest/lookup", params)
    except urllib.error.HTTPError as e:
        logger.error(f"archive lookup failed: HTTP {e.code}")
    except Exception as e:
        logger.error(f"archive lookup failed: {e}")
    return None


def _classify(field, before, after, *, stored_prec=None, want_prec=None):
    """new | same | refine | downgrade-noise | collision."""
    if after is None or after == "":
        return None                      # nothing to say about this field
    if before is None or before == "":
        return "new"

    tol = _TOLERANCE.get(field, 0)
    if isinstance(before, (int, float)) and isinstance(after, (int, float)):
        if abs(before - after) <= tol:
            return "same"
    elif str(before) == str(after):
        return "same"

    # Clocks carry how well they were measured, so a disagreement inside the
    # margin of error is not a disagreement.
    if stored_prec and want_prec and isinstance(before, (int, float)):
        gap = abs(before - after)
        if want_prec < stored_prec and gap <= stored_prec:
            return "refine"              # sharper, and consistent with the old
        if want_prec > stored_prec and gap <= want_prec:
            return "noise"               # blurrier, and says nothing new
    return "collision"


def build_plan(config: dict, *, idx: int, stream_fields: dict,
               captures: list[dict], found: dict | None = None) -> dict:
    """Diff what ls-audit reconstructed against what the archive holds."""
    ids = [(c["platform"], c.get("remote_id")) for c in captures if c.get("remote_id")]
    got = found if found is not None else lookup(config, idx=idx, ids=ids)
    if got is None:
        return {"ok": False, "reason": "archive unreachable", "items": []}
    if got.get("conflict"):
        return {"ok": False, "reason": "identity conflict",
                "conflict": got["conflict"], "items": []}

    stream = got.get("stream") or {}
    by_remote = {(c["platform"], c["remote_id"]): c
                 for c in stream.get("captures", [])}
    by_platform = {c["platform"]: c for c in stream.get("captures", [])}

    plan = {
        "ok": True, "idx": idx,
        "stream_id": stream.get("id"),
        "archive_idx": stream.get("idx"),
        "creating": not got.get("found"),
        "matched_by": got.get("by"),
        "items": [], "captures": captures, "stream_fields": stream_fields,
        "orphans": [],
    }

    # Stream-level.
    for field, after in stream_fields.items():
        kind = _classify(field, stream.get(field), after)
        if kind in (None, "same"):
            continue
        plan["items"].append({
            "scope": "stream", "field": field,
            "before": stream.get(field), "after": after, "kind": kind,
            "human": field in (stream.get("human_fields") or []),
            "accepted": kind != "collision",
        })

    # Per capture.
    for cap in captures:
        plat = _norm_platform(cap["platform"])
        remote = cap.get("remote_id")
        cur = by_remote.get((plat, remote)) or {}
        # A capture on this stream with the same platform but a different id is
        # not this capture. Report it; never repoint or delete it from here.
        clash = by_platform.get(plat)
        if remote and clash and clash.get("remote_id") not in (None, remote):
            plan["orphans"].append({
                "platform": plat, "archive_remote_id": clash.get("remote_id"),
                "audit_remote_id": remote,
            })
            continue
        # Fields the audit says are no longer true. Emitted as their own kind
        # so the diff shows a removal rather than a silent absence, and so a
        # headless run still honours a path somebody set by hand.
        for field in cap.get("clear", []):
            col = _CAP_COLUMN.get(field, field)
            if cur.get(col) in (None, ""):
                continue
            plan["items"].append({
                "scope": plat, "field": field, "column": col,
                "before": cur.get(col), "after": None, "kind": "clear",
                "human": col in (cur.get("human_fields") or []),
                "accepted": True, "precision": None,
            })
        for field, after in cap.items():
            col = _CAP_COLUMN.get(field)
            if not col:
                continue
            prec_col = _CLOCK_PRECISION.get(field)
            kind = _classify(
                field, cur.get(col), after,
                stored_prec=cur.get(prec_col) if prec_col else None,
                want_prec=cap.get(prec_col) if prec_col else None,
            )
            if kind in (None, "same", "noise"):
                continue
            plan["items"].append({
                "scope": plat, "field": field, "column": col,
                "before": cur.get(col), "after": after, "kind": kind,
                "human": col in (cur.get("human_fields") or []),
                "accepted": kind != "collision",
                "precision": ((cur.get(prec_col), cap.get(prec_col))
                              if prec_col else None),
            })
    return plan


# ── showing it ────────────────────────────────────────────────────────────

def _fmt(field: str, v) -> str:
    if v is None or v == "":
        return "—"
    if field in ("broadcast_started_at", "record_started_at", "started_at") \
            and isinstance(v, (int, float)):
        return datetime.datetime.fromtimestamp(v).strftime("%Y-%m-%d %H:%M:%S")
    if field == "duration_s" and isinstance(v, (int, float)):
        h, rem = divmod(int(v), 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    s = str(v)
    return s if len(s) <= 46 else s[:43] + "..."


_MARK = {"new": "+", "refine": "↑", "collision": "!", "clear": "−"}


def _line(it: dict) -> str:
    scope = "   " if it["scope"] == "stream" else f"[{it['scope']}]"
    body = f"  {_MARK.get(it['kind'], ' ')} {scope} {it['field']:<20}"
    if it["kind"] == "new":
        return f"{body} {_fmt(it['field'], it['after'])}"
    if it["kind"] == "clear":
        return (f"{body} {_fmt(it['field'], it['before'])}  →  "
                f"(forgotten — the merged chat holds it now)")
    extra = ""
    if it.get("precision") and it["precision"][0] and it["precision"][1]:
        old, new = it["precision"]
        if new != old:
            extra = f"   ({old}s → {new}s accuracy)"
    if it.get("human"):
        extra += "   ← edited by hand"
    return (f"{body} {_fmt(it['field'], it['before'])}"
            f"  →  {_fmt(it['field'], it['after'])}{extra}")


def render_plan(plan: dict) -> str:
    if not plan.get("ok"):
        out = [f"  ✗ {plan.get('reason')}"]
        c = plan.get("conflict")
        if c:
            out.append(f"    {c.get('detail')}")
            out.append("    Nothing was sent. Resolve this in the archive first.")
        return "\n".join(out)
    out = []
    if plan["creating"]:
        out.append(f"  Archive: no stream for #{plan['idx']} — will create it")
    else:
        out.append(f"  Archive: #{plan['archive_idx']} "
                   f"(matched by {plan['matched_by']})")
    for o in plan.get("orphans", []):
        out.append(f"  ! [{o['platform']}] archive has {o['archive_remote_id']}, "
                   f"vault says {o['audit_remote_id']} — skipped, fix by hand")
    if not plan["items"]:
        out.append("  ✔ nothing to change")
        return "\n".join(out)
    for it in plan["items"]:
        out.append(_line(it))
    return "\n".join(out)


def confirm_plan(plan: dict, *, interactive: bool = True) -> dict:
    """Ask about collisions only. Everything else was already decided."""
    items = plan.get("items", [])
    clashes = [i for i in items if i["kind"] == "collision"]

    if not interactive:
        # A headless sweep may only fill in blanks. Collisions are left alone
        # because there is nobody to ask, and anything a person has already
        # decided is left alone even when the archive currently holds nothing
        # — a field somebody deliberately emptied is still their answer, and a
        # timer running at 3am is the last thing that should overrule it.
        held = 0
        for it in items:
            if it["kind"] == "collision" or it.get("human"):
                it["accepted"] = False
                held += 1
        if held:
            logger.info(f"archive: {held} field(s) left alone (headless: "
                        f"{len(clashes)} collision(s), "
                        f"{held - len(clashes)} decided by hand)")
        return plan

    if not clashes:
        return plan
    print("\n  Collisions — the archive already holds a different value:\n")
    for it in clashes:
        print(_line(it))
        ans = input("      overwrite? (y/N): ").strip().lower()
        it["accepted"] = ans == "y"
    return plan


# ── sending it ────────────────────────────────────────────────────────────

def push_plan(config: dict, plan: dict) -> list[dict]:
    """Send only the accepted fields, one packet per capture."""
    if not plan.get("ok"):
        return []
    accepted = [i for i in plan["items"] if i.get("accepted")]
    if not accepted:
        logger.info("archive: nothing accepted, nothing sent")
        return []

    skipped = {(i["scope"], i["field"]) for i in plan["items"] if not i.get("accepted")}
    stream_id = plan.get("stream_id")
    stream_body = {i["field"]: i["after"] for i in accepted if i["scope"] == "stream"}

    results = []
    first = True
    for cap in plan["captures"]:
        plat = _norm_platform(cap["platform"])
        if not cap.get("remote_id"):
            continue
        fields = [i for i in accepted if i["scope"] == plat]
        if not fields and not (first and stream_body):
            continue
        body = {"platform": plat, "remote_id": cap["remote_id"]}
        if stream_id:
            body["stream_id"] = stream_id
        elif plan["idx"]:
            body["index"] = plan["idx"]
        for i in fields:
            if i["kind"] == "clear":
                body.setdefault("clear", []).append(i["field"])
            else:
                body[i["field"]] = i["after"]
        # Precision rides along with the clock it describes, and only then.
        if any(i["field"] == "record_started_at" and i["kind"] != "clear"
               for i in fields):
            if cap.get("local_start_precision_s"):
                body["local_start_precision_s"] = int(cap["local_start_precision_s"])
        # Stream-level fields go with the first packet; the rest would only
        # rewrite the same row.
        if first and stream_body:
            body["stream"] = stream_body
        r = _send(config, body)
        if r:
            results.append(r)
            stream_id = stream_id or r.get("id")
            first = False
    if skipped:
        logger.info(f"archive: left {len(skipped)} field(s) as they were")
    return results


# ══════════════════════════════════════════════════════════════════════════
#  JOBS — what the archive asks this machine to do
# ══════════════════════════════════════════════════════════════════════════
#
# The direction of everything above this line is recorder → archive: packets
# about things that happened, which the archive stores. This half is the other
# direction, and it is the only place where the archive gets to ask for
# anything.
#
# It is still not allowed to TELL us anything. A job carries a kind, a snippet
# id, a url and a couple of relative names — never a directory, never an
# absolute path, never a command. Where the files are is this machine's
# business and is resolved from this machine's config, and ls_jobs.py refuses
# any name that does not stay inside the roots it resolved. That is the whole
# security posture in one sentence: the archive publishes intent, and the
# recorder decides what intent is allowed to mean.
#
# Claiming takes a five-minute lease. A worker that dies holding one frees it
# when the lease lapses, and the job is handed to whoever asks next — so the
# only thing that must not be lost is the REPORT. A report that never arrives
# means work already done gets done again, which for a fetch is a second
# download and for a purge is a file that is already gone. So reports retry,
# and then spool, and are replayed before the next claim. The archive answers
# `already: true` to a report it has already seen, so replaying one is free.

JOBS_SPOOL_PATH = os.path.join(SCRIPT_DIR, ".archive_jobs_outbox.json")

# What this machine is willing to be asked for. The archive has its own copy of
# this list and will refuse to hand out anything else, but that copy is a
# courtesy: this one is the one that decides.
PI_KINDS = ("fetch", "promote", "purge", "rescan", "harvest")

JOB_TIMEOUT = 15     # longer than TIMEOUT: a claim writes, and may wait on a lock


def _job_post(config: dict, path: str, body: dict) -> dict:
    url = config["archive_url"].rstrip("/") + path
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers=_headers(config))
    with urllib.request.urlopen(req, timeout=JOB_TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8") or "{}")


# ── the spool ─────────────────────────────────────────────────────────────

def _load_reports() -> list[dict]:
    try:
        with open(JOBS_SPOOL_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except FileNotFoundError:
        return []
    except Exception as e:
        logger.warning(f"archive job spool unreadable, starting a new one: {e}")
        return []


def _save_reports(items: list[dict]) -> None:
    try:
        if not items:
            if os.path.exists(JOBS_SPOOL_PATH):
                os.remove(JOBS_SPOOL_PATH)
            return
        tmp = JOBS_SPOOL_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items[-OUTBOX_MAX:], f, ensure_ascii=False, indent=1)
        os.replace(tmp, JOBS_SPOOL_PATH)
    except Exception as e:
        logger.warning(f"could not write the archive job spool: {e}")


def spooled_reports() -> int:
    """How many finished jobs have not been reported. For the status line."""
    return len(_load_reports())


def flush_reports(config: dict) -> int:
    """Replay anything the archive did not hear. Call before claiming.

    Before, and not after: a spooled report is about a job whose lease may be
    about to lapse, and telling the archive it is done is what stops it being
    handed out again.
    """
    if not enabled(config):
        return 0
    items = _load_reports()
    if not items:
        return 0
    sent = 0
    for i, item in enumerate(items):
        try:
            _job_post(config, f"/api/ingest/jobs/{item['job_id']}", item["body"])
        except urllib.error.HTTPError as e:
            # 404 means the job is gone from the archive entirely — there is
            # nothing left to tell, and keeping the report forever would mean
            # never draining the spool.
            if e.code == 404:
                sent += 1
                continue
            _save_reports(items[i:])
            return sent
        except Exception:
            _save_reports(items[i:])
            if sent:
                logger.info(f"archive: reported {sent}, {len(items) - sent} still waiting")
            return sent
        sent += 1
    _save_reports([])
    logger.info(f"archive: reported {sent} finished job(s) from the spool")
    return sent


# ── claiming ──────────────────────────────────────────────────────────────

def claim_jobs(config: dict, *, worker: str, kinds=PI_KINDS, limit: int = 1) -> list[dict]:
    """Take a lease on up to `limit` jobs. Never raises.

    An empty list is the normal answer and is not worth a log line — this is
    polled every few seconds and the queue is empty almost always.
    """
    if not enabled(config):
        return []
    wanted = [k for k in kinds if k in PI_KINDS]
    if not wanted:
        return []
    try:
        r = _job_post(config, "/api/ingest/jobs/claim",
                      {"worker": worker, "kinds": wanted, "limit": int(limit)})
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", "replace")[:200]
        except Exception:
            pass
        logger.warning(f"archive refused the claim: HTTP {e.code} {detail}")
        return []
    except Exception as e:
        logger.debug(f"archive not reachable for a claim: {e}")
        return []
    jobs = r.get("jobs") or []
    if jobs:
        logger.info("archive: claimed " + ", ".join(
            f"{j.get('kind')} {j.get('id', '')[:8]}" for j in jobs))
    return jobs


# ── reporting ─────────────────────────────────────────────────────────────

def report_job(config: dict, job_id: str, status: str, *,
               result_path: str | None = None, error: str | None = None,
               result: dict | None = None) -> bool:
    """Say what happened. Spools rather than gives up.

    `status` is 'done' or 'failed' and both are terminal at the archive — there
    is no "try again later", because the thing that decides whether to try
    again is the human looking at the queue. A fetch that failed because
    YouTube was rate-limiting is a fetch somebody re-presses the button for.
    """
    if not enabled(config):
        return False
    body: dict = {"status": status}
    # Facts, not a verdict. Only rescan sends any; the archive ignores the key
    # on every other kind, so an older archive simply drops it.
    if result:
        body["result"] = result
    if result_path:
        body["result_path"] = str(result_path)
    if error:
        body["error"] = str(error)[:4000]
    try:
        _job_post(config, f"/api/ingest/jobs/{job_id}", body)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning(f"archive does not know job {job_id[:8]}; dropping the report")
            return False
        # 4xx otherwise is a bug in this file, and spooling a packet the
        # archive will refuse forever is how a spool stops draining.
        if e.code < 500:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")[:200]
            except Exception:
                pass
            logger.error(f"archive refused the report: HTTP {e.code} {detail}")
            return False
    except Exception as e:
        logger.warning(f"archive unreachable for the report on {job_id[:8]}: {e}")
    items = _load_reports()
    items.append({"job_id": job_id, "queued_at": int(datetime.datetime.now().timestamp()),
                  "body": body})
    _save_reports(items)
    logger.warning(f"archive: spooled the {status} report for {job_id[:8]} "
                   f"({len(items)} waiting)")
    return False
