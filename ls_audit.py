#!/usr/bin/env python3
"""
ls-audit - Audit and reconstruct Obsidian livestream entries.

Usage:
    ls-audit <index>                        Reconstruct entry
    ls-audit <index> --yt-id ID             Override YouTube video ID
    ls-audit <index> --tw-id ID             Override Twitch video ID
    ls-audit --refresh [youtube|twitch]     Refresh VOD cache
    ls-audit --inject URL                   Add video to cache from URL
    ls-audit --inject --manual              Manually add to cache
    ls-audit --cache-info ID                Look up cached video by ID
"""

import os, re, glob, gzip, sys, json, shutil, subprocess, datetime, argparse, calendar, logging
from yt_dlp.utils import sanitize_filename

import ls_common
import ls_chat
import ls_archive
import ls_assets
import ls_witness


# ═══════════════════════════════════════════════════════════════════════════
#  MEDIA ANALYSIS  (video duration + chat stats)
# ═══════════════════════════════════════════════════════════════════════════

def _seconds_to_hhmmss(value) -> str:
    """Convert a numeric seconds value to HH:MM:SS. Returns 'UNKNOWN' on any failure."""
    try:
        secs = int(float(value))
        if secs < 0:
            # Negative offset (pre-stream YT chat) — show with leading minus
            h, rem = divmod(-secs, 3600)
            m, s = divmod(rem, 60)
            return f"-{h:02d}:{m:02d}:{s:02d}"
        h, rem = divmod(secs, 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"
    except Exception:
        return "UNKNOWN"


def analyze_video_file(filepath: str) -> dict:
    """
    Return video duration via ffprobe.
    Result keys: duration_secs (float|None), duration_str (str).
    Never raises.
    """
    result = {"duration_secs": None, "duration_str": "UNKNOWN"}
    try:
        dur = ls_common.probe_duration(filepath)
        if dur is not None:
            result["duration_secs"] = dur
            result["duration_str"] = _seconds_to_hhmmss(dur)
    except Exception:
        pass
    return result


def _extract_yt_chat_timestamp_secs(entry: dict) -> float | None:
    """
    Pull videoOffsetTimeMsec from a yt-dlp live_chat JSONL entry.
    Returns seconds (may be negative for pre-stream), or None.

    The field lives at the top level of each JSONL object, not nested
    inside replayChatItemAction (which only contains the action payloads).
    """
    try:
        # Primary: top-level field (standard yt-dlp live_chat format)
        raw = entry.get("videoOffsetTimeMsec")
        if raw is not None:
            return int(raw) / 1000.0
        # Fallback: some older recordings nest it differently
        raw = entry.get("replayChatItemAction", {}).get("videoOffsetTimeMsec")
        if raw is not None:
            return int(raw) / 1000.0
    except Exception:
        pass
    return None


def analyze_chat_file(filepath: str) -> dict:
    """
    Analyze a chat JSON/JSONL file.

    Supports:
      • Twitch: JSON array, ``timestamp`` field in **microseconds** relative
        to stream start (produced by ls_common.record_twitch_chat).
      • YouTube: JSONL, ``replayChatItemAction.videoOffsetTimeMsec`` in
        **milliseconds** relative to video start (yt-dlp live_chat format).

    Result keys:
      count       – int or "UNKNOWN"
      first_ts    – "HH:MM:SS" of earliest message (or "UNKNOWN")
      last_ts     – "HH:MM:SS" of latest  message (or "UNKNOWN")
      last_secs   – float or None, raw offset of the latest message
      format      – "twitch" | "youtube" | "unknown"

    Never raises; any parse failure replaces the affected value with "UNKNOWN".
    """
    result: dict = {
        "count": "UNKNOWN",
        "first_ts": "UNKNOWN",
        "last_ts": "UNKNOWN",
        "last_secs": None,
        "format": "unknown",
    }

    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            raw = f.read().strip()
        if not raw:
            result["count"] = 0
            return result

        messages: list[dict] = []
        timestamps: list[float] = []

        # ── Try Twitch: well-formed JSON array ────────────────────────────
        parsed_as_array = False
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                parsed_as_array = True
                result["format"] = "twitch"
                for msg in data:
                    if not isinstance(msg, dict):
                        continue
                    messages.append(msg)
                    ts_raw = msg.get("timestamp")
                    if ts_raw is not None:
                        try:
                            timestamps.append(int(ts_raw) / 1_000_000.0)
                        except Exception:
                            pass
            # ── The OTHER Twitch spelling: what TwitchDownloaderCLI writes ──
            #
            # A JSON OBJECT with a `comments` array, not the recorder's array
            # of rows with a microsecond `timestamp`. Nothing here recognised
            # it, so it fell through to the YouTube JSONL branch below, which
            # split a pretty-printed object into lines, parsed exactly one of
            # them, and answered: 1 message.
            #
            # That is the shape EVERY offline pull arrives in, which made the
            # panel report a 560-message Twitch chat as holding one. Nobody
            # would keep a file the audit says is empty, and the audit was
            # describing a perfectly good one.
            #
            # Delegated to ls_chat rather than parsed again here: it owns the
            # format, it already knows all three spellings, and a fourth
            # partial parser in this file is how there came to be two.
            elif isinstance(data, dict) and isinstance(data.get("comments"), list):
                parsed_as_array = True
                result["format"] = "twitch"
                conv = ls_chat.convert_file(filepath)
                messages = list(conv.messages)
                # `ts` is ms from the source's own zero, which for this shape
                # is `content_offset_seconds` -- the same quantity the branch
                # above derives, in the same units, so `last_secs` means the
                # same thing to every caller either way.
                timestamps = [m.ts / 1000.0 for m in messages if m.ts is not None]
        except (json.JSONDecodeError, ValueError, OSError):
            pass

        # ── Try YouTube: JSONL ─────────────────────────────────────────────
        if not parsed_as_array:
            result["format"] = "youtube"
            for line in raw.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    if isinstance(entry, dict):
                        messages.append(entry)
                        secs = _extract_yt_chat_timestamp_secs(entry)
                        if secs is not None:
                            timestamps.append(secs)
                except (json.JSONDecodeError, Exception):
                    continue

        result["count"] = len(messages)

        if timestamps:
            result["last_secs"] = max(timestamps)
            result["first_ts"] = _seconds_to_hhmmss(min(timestamps))
            result["last_ts"]  = _seconds_to_hhmmss(result["last_secs"])

    except Exception:
        pass

    return result


def _print_media_analysis(config: dict, nas: dict):
    """
    Print ffmpeg duration and chat stats for all files found on NAS.
    Called after the NAS scan table inside audit(). Never raises.
    """
    nas_root = config.get("nas_path", "")
    rows = [
        ("yt_video", "YT video"),
        ("yt_chat",  "YT chat "),
        ("tw_video", "TW video"),
        ("tw_chat",  "TW chat "),
    ]

    if not any(nas.get(k) for k, _ in rows):
        return

    print("  Media analysis:")
    if nas.get("merged_chat"):
        merged_path = os.path.join(config.get("nas_path", ""), nas["merged_chat"])
        info = analyze_chat_file(merged_path)
        n_arch = len(nas.get("yt_chats_archived", [])) + len(nas.get("tw_chats_archived", []))
        print(f"    MERGED   : {nas['merged_chat']}"
              + (f"  ({n_arch} raw{'s' if n_arch != 1 else ''} in deep storage)"
                 if n_arch else ""))

    for key, label in rows:
        filename = nas.get(key)
        if not filename:
            # `—` on a chat row after a merge reads as data loss, which is the
            # opposite of what happened: the raw was folded into the merged
            # file and moved to deep storage deliberately. Say which nothing
            # this is.
            if key.endswith("_chat") and _chat_accounted(nas, key[:2]):
                print(f"    {label} : in the merged file")
            else:
                print(f"    {label} : —")
            continue
        filepath = os.path.join(nas_root, filename)
        if not os.path.exists(filepath):
            print(f"    {label} : ⚠ file missing from disk")
            continue

        ext = os.path.splitext(filename)[1].lower()
        if ext in ls_common.VIDEO_EXTS:
            info = analyze_video_file(filepath)
            print(f"    {label} : {info['duration_str']}")

        elif ext == ".json":
            info = analyze_chat_file(filepath)
            count   = info["count"]
            first   = info["first_ts"]
            last    = info["last_ts"]
            count_s = str(count).rjust(4) if isinstance(count, int) else count
            print(f"    {label} : {count_s} messages  ({first} → {last})")

    print()


# ═══════════════════════════════════════════════════════════════════════════
#  NAS SCANNER
# ═══════════════════════════════════════════════════════════════════════════

def scan_nas(config: dict, index: int) -> dict:
    """Scan NAS for files matching this index prefix.

    Returns dict with yt_video, yt_chat, tw_video, tw_chat filenames, plus
    yt_chats / tw_chats holding EVERY capture found for that platform.

    A restart, or a repair written beside the original, leaves two captures
    of one video id. Picking one is a guess -- the merge takes them all and
    dedupes. `*_chat` stays the single largest for callers that need one
    file (coverage check, meta sidecar, archive path).
    """
    found = {
        "yt_video": None, "yt_chat": None, "yt_chats": [],
        "tw_video": None, "tw_chat": None, "tw_chats": [],
        "yt_chats_archived": [], "tw_chats_archived": [],
        "merged_chat": None,
    }
    nas = config["nas_path"]
    if not os.path.exists(nas):
        print("  ⚠ NAS not mounted")
        return found

    idx_padded = f"{int(index):03d}"
    patterns = [f"{idx_padded}_*"]
    if str(index) != idx_padded:
        patterns.append(f"{index}_*")

    seen: set[str] = set()
    for pat in patterns:
        for filepath in glob.glob(os.path.join(nas, pat)):
            filename = os.path.basename(filepath)
            if filename in seen:
                continue
            seen.add(filename)

            # Skip intermediate fragment files like title.f140.m4a
            if re.search(r"\.f\d+\.\w+$", filename):
                continue
            # Only accept files whose numeric prefix matches exactly
            m = re.match(r"^(\d+)_", filename)
            if not m or int(m.group(1)) != int(index):
                continue

            vid = ls_common.extract_video_id_from_filename(filename)
            if not vid:
                continue

            platform = ls_common.classify_video_id(vid)
            ext = os.path.splitext(filename)[1].lower()
            prefix = "yt" if platform == "youtube" else "tw"

            if ext in ls_common.VIDEO_EXTS:
                existing = found[f"{prefix}_video"]
                # Prefer mp4 if multiple recordings exist
                if not existing or (ext == ".mp4" and not existing.lower().endswith(".mp4")):
                    found[f"{prefix}_video"] = filename
            elif ext == ".json" and not ls_chat.is_derived(filename):
                found[f"{prefix}_chats"].append(filename)

    # The merged chat, and the raws the merge moved to deep storage. Without
    # these an archived entry looks like one whose chat was never captured.
    found["merged_chat"] = find_merged_chat(nas, index)

    arch = config.get("chat_archive_path")
    if arch:
        arch = os.path.join(nas, arch)
        for pat in patterns:
            for filepath in glob.glob(os.path.join(arch, pat)):
                filename = os.path.basename(filepath)
                if os.path.splitext(filename)[1].lower() != ".json":
                    continue
                if ls_chat.is_derived(filename):
                    continue
                m = re.match(r"^(\d+)_", filename)
                if not m or int(m.group(1)) != int(index):
                    continue
                vid = ls_common.extract_video_id_from_filename(filename)
                if not vid:
                    continue
                p = "yt" if ls_common.classify_video_id(vid) == "youtube" else "tw"
                found[f"{p}_chats_archived"].append(filename)

    # Largest first: the best single representative, and a stable order for
    # the merge regardless of how glob happened to return them.
    for prefix in ("yt", "tw"):
        chats = sorted(
            found[f"{prefix}_chats"],
            key=lambda f: os.path.getsize(os.path.join(nas, f)),
            reverse=True)
        found[f"{prefix}_chats"] = chats
        found[f"{prefix}_chat"] = chats[0] if chats else None
        if len(chats) > 1:
            print(f"    {len(chats)} {prefix} chat captures; all will be merged")

    return found


# ═══════════════════════════════════════════════════════════════════════════
#  ID RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════
#
#  Priority: CLI override → entry URL → NAS filename → archive capture
#            → cache (by index) → cache (by date, with auto-refresh if stale)
#
#  The archive sits above the cache and below the filename for one reason:
#  the three above it are STATEMENTS of identity — somebody typed this id,
#  here or in the vault or into a filename — and the two below it are
#  GUESSES, a cache row that claims an index or happens to fall on the same
#  day. A stream broadcast from somebody else's channel is in neither cache,
#  so a date guess there is not a weaker answer than the pasted URL, it is a
#  wrong one.

# Refreshed at most once per run. The correction below wants a cache that has
# heard of this id, and a sweep over two hundred entries must not mean two
# hundred trips to Helix for the same answer.
_TW_REFRESHED = False

# ls_archive.PLATFORM the other way round: "YT" → "youtube".
_UNPLATFORM = {v: k for k, v in ls_archive.PLATFORM.items()}


def archive_ids(seen: dict | None) -> dict[str, str]:
    """{platform: remote_id} out of an `/api/ingest/lookup` response.

    Pure, and takes the response rather than fetching one, because the two
    callers that matter already have it in hand: the worker reads claims
    through the same lookup before it inspects anything.

    This is the only path by which an id a human typed into the WEBSITE
    reaches the recorder's side of the house. Without it, a capture added in
    the UI — right URL, id extracted, row written — is invisible here, and
    the audit goes on reporting the stream as never recorded and offering to
    give up on it. That is the archive and the Pi being two archives.
    """
    out = {}
    for cap in (((seen or {}).get("stream") or {}).get("captures") or []):
        remote = str(cap.get("remote_id") or "").strip()
        plat = _UNPLATFORM.get(str(cap.get("platform") or "").strip().upper())
        if remote and plat:
            out[plat] = remote
    return out


def resolve_id(config: dict, cache: list[dict], platform: str,
               entry: dict, nas: dict,
               cli_override: str | None = None,
               arch_ids: dict[str, str] | None = None) -> tuple[str | None, str | None]:
    """Resolve video ID for a platform, correcting a Twitch STREAM id.

    Everything upstream of this — the Obsidian entry, the NAS filename, and for
    years the archive itself — can be holding the id of the BROADCAST rather
    than of the video it became, because that is the only id that exists while
    a stream is being recorded. Left alone it makes a watch URL that 404s and
    an embed that plays nothing.

    The correction is deliberately at the END rather than as another priority
    step: it is not a fifth place to look, it is a fact about whatever the four
    places returned. Wherever the id came from, if some VOD in the cache claims
    that broadcast, the VOD's id is the answer.
    """
    global _TW_REFRESHED
    vid, src = _resolve_id_raw(config, cache, platform, entry, nas, cli_override,
                               arch_ids)
    if platform != "twitch" or not vid:
        return vid, src

    fixed, corrected = ls_common.twitch_correct_id(cache, vid)
    if corrected:
        return fixed, f"{src} → vod (was a stream id)"

    # Neither a VOD we know nor a broadcast we know. That is what a cache too
    # old to have seen this stream looks like, so ask Helix once and re-try.
    # An id that survives this really is a VOD id we simply have not cached.
    #
    # `find_confirmed_vod` and not `find_vod`, which is the whole of why this
    # refresh never ran for a stream recorded the same day: the recorder writes
    # its own cache row keyed by the BROADCAST id, so the plain lookup found
    # that row, concluded the id was a VOD it already knew, and skipped the one
    # call that could have learned otherwise. The row that makes the id wrong
    # cannot be the row that vouches for it.
    if not ls_common.find_confirmed_vod(cache, vid, "twitch") and not _TW_REFRESHED:
        _TW_REFRESHED = True
        print("  ⌛ Refreshing twitch cache (unknown id)...")
        if ls_common.refresh_twitch_cache(config, cache, full=True):
            ls_common.save_cache(cache)
            fixed, corrected = ls_common.twitch_correct_id(cache, vid)
            if corrected:
                return fixed, f"{src} → vod (was a stream id)"
    return vid, src


def _helix_floor_ms(cache: list[dict]) -> int | None:
    """The oldest Twitch VOD Helix has actually shown us, in epoch ms.

    Helix lists a bounded window of recent videos — there is no way to ask for
    all of them — so the cache's knowledge has a floor, and that floor is the
    only honest boundary for "we would have seen it if it existed".

    None when no row carries `stream_id`, which means no refresh has ever run
    against this cache since that field was added. That is not a small caveat:
    on the live Pi, 261 of 261 Twitch rows were in exactly that state, so the
    join every correction depends on was empty and no amount of guard-fixing
    could have helped until one refresh had run.
    """
    best = None
    for v in cache:
        if v.get("platform") != "twitch" or not v.get("stream_id"):
            continue
        raw = str(v.get("start_time") or "")
        if not raw:
            continue
        try:
            dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.astimezone()
        ms = int(dt.timestamp() * 1000)
        best = ms if best is None else min(best, ms)
    return best


def unpublished_vod(cache: list[dict], platform: str, video_id: str | None) -> bool:
    """Is this id a broadcast whose VOD we looked for and can say is not there?

    Three conditions, and the last two are what stop this from quietly blanking
    a link on every old entry in the vault:

      1. the id has a BROADCAST row — positive evidence, not absence of it;
      2. the cache has been refreshed at least once since Helix's `stream_id`
         started being kept, or there is no join to have failed and therefore
         nothing to conclude;
      3. the broadcast is INSIDE the window Helix showed us. A stream from
         last year is not missing from a listing of the last two hundred
         videos, it is simply older than the listing — and suppressing its
         link would be this function inventing a fact rather than reporting
         one.

    What it is for: Twitch mints the VOD minutes AFTER the broadcast ends, so
    auditing a stream the evening it happened lands here every time. Building
    `twitch.tv/videos/<broadcast id>` from that is a link that 404s — and it
    went on to overwrite a working channel URL in the archive, with a
    truncated confirmation prompt as the only thing in its way. There is a
    right answer and it arrives on its own: run the audit again later.
    """
    if platform != "twitch" or not video_id:
        return False
    row = ls_common.find_vod(cache, video_id, "twitch")
    if not row or not ls_common.is_broadcast_row(row):
        return False
    floor = _helix_floor_ms(cache)
    started = row.get("record_start_epoch_ms") or row.get("stream_start_epoch_ms")
    if floor is None or not started:
        return False
    return int(started) >= floor


def _resolve_id_raw(config: dict, cache: list[dict], platform: str,
                    entry: dict, nas: dict,
                    cli_override: str | None = None,
                    arch_ids: dict[str, str] | None = None) -> tuple[str | None, str | None]:
    """Where an id is looked for, in order. See resolve_id for the correction."""
    tag = "yt" if platform == "youtube" else "tw"

    # 1. CLI override
    if cli_override:
        return cli_override, "cli"

    # 2. URL already in obsidian entry
    entry_id = entry.get(f"{tag}_id")
    if entry_id:
        return entry_id, "entry"

    # 3. NAS filename
    nas_file = nas.get(f"{tag}_video")
    if nas_file:
        vid = ls_common.extract_video_id_from_filename(nas_file)
        if vid:
            return vid, "nas"

    # 4. The archive's capture row — a URL somebody entered on the website
    arch = (arch_ids or {}).get(platform)
    if arch:
        return arch, "archive"

    # 5. Cache by obsidian_index
    target_index = entry.get("_index")
    if target_index is not None:
        for vod in cache:
            if (vod.get("platform") == platform
                    and vod.get("obsidian_index") == int(target_index)):
                return vod["id"], "cache (index)"

    # 6. Cache by date (auto-refresh if stale)
    if entry["date_obj"]:
        target_index = entry.get("_index")

        def _match():
            return ls_common.find_vod_by_date(
                cache, platform, entry["date_obj"], claim_index=target_index,
            )

        vod = _match()
        if vod is None:
            newest_dates = [
                v.get("start_time", "")[:10]
                for v in cache if v.get("platform") == platform
            ]
            newest = max(newest_dates) if newest_dates else None
            target_date = entry["date_obj"].strftime("%Y-%m-%d")
            if newest is None or target_date > newest:
                print(f"  ⌛ Refreshing {platform} cache...")
                if platform == "youtube":
                    ls_common.refresh_youtube_cache(config, cache, full=(newest is None))
                else:
                    ls_common.refresh_twitch_cache(config, cache, full=(newest is None))
                ls_common.save_cache(cache)
                vod = _match()

        if vod:
            label = "cache (date)"
            try:
                vdt = (datetime.datetime
                       .fromisoformat(vod["start_time"].replace("Z", "+00:00"))
                       .replace(tzinfo=None))
                delta = abs(vdt - entry["date_obj"])
                if delta > datetime.timedelta(minutes=5):
                    label += f" (~{int(delta.total_seconds() // 60)}m off)"
            except Exception:
                pass
            return vod["id"], label

    return None, None


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY BUILDER
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
#  READ-BACK
# ═══════════════════════════════════════════════════════════════════════════
#
#  The audit used to run in one direction: derive from the files, post the
#  result, and never once look at what the archive already held. A derivation
#  that was wrong therefore could not be noticed -- it simply became the new
#  value. That is how `started_at` on #747 moved two hours and slid every chat
#  message off the video.
#
#  So the archive sends what it holds FIRST, and the recorder's job stops
#  being "work out the truth" and becomes "agree, disagree, or say I cannot
#  tell". Three verdicts, not two, and the third is the important one: the
#  length check has always been able to say "these two disagree and I am not
#  deciding", and it is the best-behaved check in the file. Everything else
#  had to answer, so everything else could be wrong quietly.
#
#  Nothing here writes. Nothing here proposes a `derived` field -- those are
#  consequences of the measurements below them, and a derivation you can
#  accept independently of its inputs is how a row becomes internally
#  inconsistent.

#  agrees   the archive's value and this machine's evidence match
#  differs  they do not, and there IS evidence for another value
#  unknown  no local evidence, or the witnesses conflict. Nothing proposed,
#           and the archive's value stands. A deleted video file lands here,
#           which is the difference between "the number is gone" and "the
#           number is fine, I just cannot re-check it from here".
AGREES, DIFFERS, UNKNOWN = "agrees", "differs", "unknown"

#  How close counts as the same, per field. A wall time measured off a
#  filename is good to the minute; a duration off ffprobe is good to a second
#  and the platform rounds. Zero means exact.
_EVAL_TOL = {
    "remote_start_wall": 2,
    "local_start_wall": 2,
    "file_duration_s": 2,
    "remote_duration_s": 2,
    "local_start_precision_s": 0,
}


def _verdict(field, held, saw, *, why=None, source=None, tol=None, quality=None):
    """One field, compared. `saw` None means there was nothing to compare."""
    out = {"field": field, "currently": held, "verdict": UNKNOWN,
           "proposed": None, "why": why, "source": source, "quality": quality}
    if saw is None:
        out["why"] = why or "nothing here measures this"
        return out
    tol = _EVAL_TOL.get(field, 0) if tol is None else tol
    same = (abs(held - saw) <= tol
            if isinstance(held, (int, float)) and isinstance(saw, (int, float))
            and not isinstance(held, bool) and not isinstance(saw, bool)
            else (held is not None and str(held) == str(saw)))
    if same:
        out["verdict"] = AGREES
        return out
    out["verdict"] = DIFFERS
    out["proposed"] = saw
    if held is None:
        out["why"] = why or "the archive holds nothing for this"
    return out


#  How good a settled number is, taken off the settlement `ls_witness` already
#  computed and then nobody read. It is all there in the sidecar on every run:
#
#      "agreement": "single", "precision_s": 60,
#      "witnesses": [{ "source": "filename", "corroborates": false }]
#
#  -- one witness, entitled to answer and not to vouch, accurate to the minute.
#  `_platform_timings` takes `settled[...]["value"]` and throws the rest away,
#  so a broadcast start nothing but a filename claims arrives downstream
#  looking exactly as solid as one the recorder measured at the time.
#
#  A filename is still allowed to answer -- it is the last fallback and that is
#  the right job for a minute-accurate guess. It is not allowed to arrive
#  silently, which is different.
def _quality(t, claim):
    st = ((t or {}).get("settled") or {}).get(claim) or {}
    if not st.get("source"):
        return None
    ws = st.get("witnesses") or []
    best = st.get("best") or {}
    return {"agreement": st.get("agreement"),
            #  The winner was carried forward rather than re-taken this run:
            #  the file it was measured on is gone, the cache row went, the
            #  log rolled over. The number is still the best anybody ever
            #  had; it is simply no longer checkable, and a panel that does
            #  not say so is showing a measurement of something that is not
            #  there as though it were of something that is.
            "stale": bool(best.get("stale")),
            "seen_at": best.get("seen_at"),
            "precision_s": st.get("precision_s"),
            "witnesses": len(ws),
            #  Not "did anybody agree" but "was anybody ENTITLED to". A cache
            #  row derived from the same reading as the sidecar is one witness
            #  spelled twice, and counting it would report corroboration that
            #  never happened.
            "corroborated": sum(1 for w in ws if w.get("corroborates", True)) > 1,
            "spread_s": st.get("spread_s")}


#  Which claim, if any, a capture field is a reading of. Only these can go
#  stale, because only these are settlements -- a path or an id is not a
#  measurement and has nothing to be stale about.
_FIELD_CLAIM = {
    "remote_start_wall": ls_witness.BROADCAST_START,
    "local_start_wall": ls_witness.RECORD_START,
    "local_start_precision_s": ls_witness.RECORD_START,
    "file_duration_s": ls_witness.FILE_DURATION,
}


def _stale_quality(t, field):
    """What the last live reading of this field said, when nothing can read it
    now. None when there is no settlement behind the field, or when the
    settlement is live and simply produced nothing this run."""
    q = _quality(t, _FIELD_CLAIM.get(field)) if _FIELD_CLAIM.get(field) else None
    if not q or not q.get("stale"):
        return None
    st = ((t or {}).get("settled") or {}).get(_FIELD_CLAIM[field]) or {}
    return {**q, "was": st.get("value"), "was_source": (st.get("best") or {}).get("label")}


def _cap_evidence(config, cache, nas, prefix, platform, timings):
    """{field: {value, source, quality}} for one platform, from this machine.

    Only what was actually SEEN. A key absent here means no evidence, which
    `_verdict` turns into `unknown` rather than into a proposal of None --
    proposing None would clear a column the recorder measured once and can
    never measure again.
    """
    out = {}
    t = (timings or {}).get(prefix) or {}
    nas_root = config.get("nas_path", "")

    def saw(field, value, source, claim=None):
        out[field] = {"value": value, "source": source,
                      "quality": _quality(t, claim) if claim else None}

    #  THE ID OFF THE FILENAME, WHICH ON TWITCH IS OFTEN THE WRONG KIND.
    #
    #  A Twitch BROADCAST and the VOD it becomes carry different numbers. The
    #  recorder catches the channel live, so the only id that exists while it
    #  is writing the file is the broadcast's -- and that is the one that ends
    #  up in the filename. The VOD is minted when the broadcast ends.
    #
    #  `resolve_id` has corrected for this for years; this did not, and the
    #  consequence was a read-back offering to replace a capture's good VOD id
    #  with the broadcast id out of its own filename. On #748 the archive held
    #  2884336619 and this proposed 318722472677, which is a number no watch
    #  URL resolves.
    #
    #  So the same correction runs here. When the cache cannot map it -- a
    #  broadcast it has never seen -- nothing is proposed at all: `unknown`
    #  with the reason, because "I have a number and it is the wrong kind" is
    #  not a better answer than "I cannot tell".
    vid = None
    for f in (nas.get(f"{prefix}_video"), nas.get(f"{prefix}_chat")):
        if not f:
            continue
        raw = ls_common.extract_video_id_from_filename(f)
        if not raw:
            continue
        if platform != "twitch":
            saw("remote_id", raw, f"the filename of {f}")
            vid = raw
            break
        raw = raw.lstrip("v") or raw
        fixed, corrected = ls_common.twitch_correct_id(cache, raw)
        if corrected:
            saw("remote_id", fixed, f"the VOD that {raw} became, per the cache")
            vid = fixed
            break
        row = ls_common.find_vod(cache, raw, "twitch")
        if row and ls_common.is_broadcast_row(row):
            #  Known to be a broadcast, and no VOD claims it yet. Saying so is
            #  the whole value: the archive's id is probably right and this
            #  one is definitely not a video.
            out["remote_id"] = {
                "value": None, "source": None,
                "quality": {"agreement": "single", "precision_s": 0,
                            "witnesses": 1, "corroborated": False,
                            "stale": False, "seen_at": None,
                            "was": raw, "was_source": "a broadcast id, not a VOD"}}
            vid = raw
            break
        saw("remote_id", raw, f"the filename of {f}")
        vid = raw
        break

    if nas.get(f"{prefix}_video"):
        saw("video_path", ls_archive.archive_path(config, nas[f"{prefix}_video"]),
            "the file on the NAS")
    if nas.get(f"{prefix}_chat"):
        saw("chat_path", ls_archive.archive_path(config, nas[f"{prefix}_chat"]),
            "the file on the NAS")

    if t.get("stream_start_epoch_ms"):
        saw("remote_start_wall", t["stream_start_epoch_ms"] // 1000,
            t.get("stream_start_source") or "measured here",
            ls_witness.BROADCAST_START)
    if t.get("record_start_epoch_ms"):
        saw("local_start_wall", t["record_start_epoch_ms"] // 1000,
            t.get("record_start_source") or "measured here",
            ls_witness.RECORD_START)
        saw("local_start_precision_s",
            60 if t.get("record_start_accuracy") == "minute" else 1,
            "how precisely the recording start could be read")
    #  Fresh only. A stale `file_duration` is a measurement of a file that is
    #  no longer on the NAS, and proposing one would be this machine telling
    #  the archive to adopt a number it cannot check -- about a copy somebody
    #  deleted on purpose, which on #747 was the doubled recording whose
    #  length is exactly the thing that was wrong with it.
    #
    #  It is still SAID, through `stale_quality` below: "the archive holds
    #  11374; the last thing that measured this file got 22746, on a file
    #  that is no longer here" is a sentence somebody can act on, and a bare
    #  "nothing measures this" is not.
    if t.get("measured_duration_s"):
        saw("file_duration_s", int(t["measured_duration_s"]), "ffprobe",
            ls_witness.FILE_DURATION)

    row = ls_common.find_vod(cache, vid, platform) if vid else None
    if row and row.get("duration_secs"):
        saw("remote_duration_s", int(row["duration_secs"]),
            f"what {platform} says about {vid}")
    return out


def evaluate(config: dict, index: int, state: dict, *,
             cache: list[dict] | None = None,
             nas: dict | None = None,
             timings: dict | None = None) -> dict:
    """Agree or disagree with every measured field the archive sent.

    `state` is the archive's `/api/ingest/lookup` -> `state` block: its own
    values, plus a manifest saying which of them are measurements and which
    are consequences of measurements.

    Writes nothing, proposes nothing for a `derived` field, and answers for
    every field it was sent -- including the ones it cannot check, which come
    back `unknown` with the archive's value still on them.

    `drift` is the other half of the manifest's point. A field the archive
    sends that this file has no check for, or a check this file has for a
    field the archive did not send, is the two ends having moved apart -- the
    same failure as a job kind missing from one of four lists, and the same
    remedy: say so out loud, on every run, rather than let it be silent.
    """
    man = (state or {}).get("manifest") or {}
    s_kinds = man.get("stream") or {}
    c_kinds = man.get("capture") or {}
    out = {"manifest_version": man.get("version"),
           "reference": (state or {}).get("reference"),
           "stream": [], "captures": [],
           "drift": {"sent_but_unchecked": [], "checked_but_unsent": []}}
    if not man:
        out["drift"]["sent_but_unchecked"] = ["(the archive sent no manifest)"]
        return out

    cache = ls_common.load_cache() if cache is None else cache
    nas = scan_nas(config, index) if nas is None else nas

    held_s = (state or {}).get("stream") or {}
    for field, kind in sorted(s_kinds.items()):
        if kind == "derived":
            out["stream"].append({
                "field": field, "currently": held_s.get(field), "kind": "derived",
                "verdict": UNKNOWN, "proposed": None, "source": None,
                "quality": None, "why": "derived from the measurements below it"})
            continue
        # Stream-level measurements are next round's work; answered honestly
        # rather than left out, so the drift check below stays meaningful.
        out["stream"].append({
            "field": field, "currently": held_s.get(field), "kind": "measured",
            "verdict": UNKNOWN, "proposed": None, "source": None,
            "quality": None, "why": "no check for this yet"})
        out["drift"]["sent_but_unchecked"].append(f"stream.{field}")

    known = {"remote_id", "video_path", "chat_path", "remote_start_wall",
             "local_start_wall", "local_start_precision_s", "file_duration_s",
             "remote_duration_s"}
    for field in sorted(known - set(c_kinds)):
        out["drift"]["checked_but_unsent"].append(f"capture.{field}")

    for cap in ((state or {}).get("captures") or []):
        plat = _UNPLATFORM.get(str(cap.get("platform") or "").strip().upper())
        prefix = {"youtube": "yt", "twitch": "tw"}.get(plat)
        saw = (_cap_evidence(config, cache, nas, prefix, plat, timings)
               if prefix else {})
        rows = []
        for field, kind in sorted(c_kinds.items()):
            held = cap.get(field)
            if kind == "derived":
                rows.append({"field": field, "currently": held, "kind": "derived",
                             "verdict": UNKNOWN, "proposed": None, "source": None,
                             "quality": None,
                             "why": "derived from the measurements above it"})
                continue
            if field not in known:
                rows.append({"field": field, "currently": held, "kind": "measured",
                             "verdict": UNKNOWN, "proposed": None, "source": None,
                             "quality": None, "why": "no check for this yet"})
                out["drift"]["sent_but_unchecked"].append(f"capture.{field}")
                continue
            got = saw.get(field)
            #  What the last live reading said, for a field nothing can read
            #  now. Attached to the verdict rather than offered as evidence:
            #  it explains the gap, it does not fill it.
            stale_q = (None if got else
                       _stale_quality((timings or {}).get(prefix), field))
            v = _verdict(field, held, got["value"] if got else None,
                         source=got["source"] if got else None,
                         quality=got["quality"] if got else stale_q,
                         why=None if got else (
                             "the platform this came from is not one this "
                             "recorder knows" if not prefix else
                             "nothing on the NAS measures this"))
            v["kind"] = "measured"
            rows.append(v)
        out["captures"].append({"id": cap.get("id"), "platform": cap.get("platform"),
                                "fields": rows})
    return out


def _title_from_filename(filename: str) -> str:
    """Extract clean title from NAS filename."""
    name = os.path.splitext(filename)[0]
    name = re.sub(r"^\d+_", "", name)
    name = re.sub(r"\s*\[[^\]]+\]\s*@\s*\d{4}-\d{2}-\d{2}_\d{2}-\d{2}$", "", name)
    return name


def _get_title(config: dict, cache: list[dict], video_id: str,
               platform: str, nas_file: str | None) -> str | None:
    """Resolve a display title: cache → NAS filename → API fetch."""
    # Cache
    vod = ls_common.find_vod(cache, video_id, platform)
    if vod and vod.get("title"):
        return vod["title"]
    # NAS filename
    if nas_file:
        return _title_from_filename(nas_file)
    # Fetch from API (and opportunistically cache it)
    try:
        url = ls_common.build_stream_url(config, platform, video_id)
        data = ls_common.ytdlp_probe(config, url, playlist_items="1")
        if data:
            title = data.get("title") or data.get("description")
            if title:
                ls_common.upsert_vod(cache, {
                    "id": video_id, "platform": platform, "title": title,
                    "start_time": data.get("upload_date", ""),
                })
                return title
    except Exception:
        pass
    return None


def _chat_link_target(nas: dict, prefix: str) -> str | None:
    """What the entry's chat icon should open.

    The raw while it is still on the NAS; once the merge has taken it, the
    merged file — that is where this platform's messages now live, and an
    empty link would say the chat was lost.
    """
    if nas.get(f"{prefix}_chat"):
        return nas[f"{prefix}_chat"]
    if nas.get(f"{prefix}_chats_archived"):
        return nas.get("merged_chat") or os.path.join(
            "deep-storage", nas[f"{prefix}_chats_archived"][0])
    return None


def _build_platform_line(config: dict, tag: str, video_id: str | None,
                         platform: str, title: str | None,
                         video_file: str | None,
                         chat_file: str | None,
                         video_x: bool = False,
                         chat_x: bool = False,
                         no_url: bool = False) -> str:
    if video_file:
        vid_link = f"[📁]({ls_common.build_shell_cmd(config, video_file)})"
    elif video_x or chat_file:        # explicit, or implied (chat but no video)
        vid_link = "[📁.×]()"
    else:
        vid_link = "[📁]()"

    if chat_file:
        chat_link = f"[📄]({ls_common.build_shell_cmd(config, chat_file)})"
    elif chat_x:
        chat_link = "[📄.×]()"
    else:
        chat_link = "[📄]()"

    display = title or "untitled"
    # `no_url` is the vault's half of the rule the archive push follows: a
    # broadcast whose VOD has not been minted has no watch link yet, and an
    # empty target is honest where `/videos/<broadcast id>` is a dead link
    # that reads like a live one. The entry keeps everything else and the next
    # run fills it in.
    url = ("" if (no_url or not video_id)
           else ls_common.build_stream_url(config, platform, video_id))
    return f"\t`{tag}` {vid_link} {chat_link} [ {display} ]({url})"


def build_entry(config: dict, cache: list[dict], index: int,
                entry: dict, nas: dict,
                yt_id: str | None, tw_id: str | None) -> list[str]:
    """Assemble the full Obsidian entry block from resolved data."""
    lines = []

    # Header: checkbox, index, date, timezone, duration
    date_str = entry["date_str"] or "UNKNOWN"
    tz_str = entry["tz_str"] or "(GMT-6)"

    # Duration: longest of the two platforms.
    # ffprobe the file actually held, and only fall back to the cache when
    # there is no file. The cache is written once at record time, so after a
    # re-download or a manual repair it reports the old length forever.
    durations = []
    for prefix, vid_id, plat in [("yt", yt_id, "youtube"),
                                 ("tw", tw_id, "twitch")]:
        measured = None
        filename = nas.get(f"{prefix}_video")
        if filename:
            path = os.path.join(config.get("nas_path", ""), filename)
            if os.path.exists(path):
                measured = analyze_video_file(path)["duration_secs"]

        vod = ls_common.find_vod(cache, vid_id, plat) if vid_id else None
        if measured:
            durations.append(measured)
            if vod and abs((vod.get("duration") or 0) - measured) > 5:
                print(f"    duration corrected from cache "
                      f"{_seconds_to_hhmmss(vod.get('duration') or 0)} → "
                      f"{_seconds_to_hhmmss(measured)} ({prefix})")
                vod["duration"] = int(measured)
        elif vod and vod.get("duration"):
            durations.append(vod["duration"])
    if durations:
        dur = max(durations)
        h, rem = divmod(int(dur), 3600)
        m, s = divmod(rem, 60)
        dur_str = f" [{h:02d}:{m:02d}:{s:02d}]"
    elif entry.get("duration_str"):
        dur_str = f" [{entry['duration_str']}]"
    else:
        dur_str = ""

    lines.append(
        f"- {entry['checkbox']} **{int(index):03d}** : "
        f"{date_str} {tz_str}{dur_str}  #stream"
    )

    # YouTube line
    if entry["no_yt"]:
        lines.append("\t`YT` ✗")
    else:
        yt_title = (_get_title(config, cache, yt_id, "youtube", nas["yt_video"])
                    if yt_id else None)
        lines.append(_build_platform_line(
            config, "YT", yt_id, "youtube", yt_title,
            nas["yt_video"], _chat_link_target(nas, "yt"),
            video_x=entry.get("yt_video_x", False),
            chat_x=entry.get("yt_chat_x", False),
        ))

    # Twitch line
    if entry["no_tw"]:
        lines.append("\t`TW` ✗")
    else:
        tw_title = (_get_title(config, cache, tw_id, "twitch", nas["tw_video"])
                    if tw_id else None)
        lines.append(_build_platform_line(
            config, "TW", tw_id, "twitch", tw_title,
            nas["tw_video"], _chat_link_target(nas, "tw"),
            video_x=entry.get("tw_video_x", False),
            chat_x=entry.get("tw_chat_x", False),
            no_url=unpublished_vod(cache, "twitch", tw_id),
        ))

    # User notes (preserved verbatim)
    for note in entry.get("notes", []):
        lines.append(note.rstrip("\n"))

    return lines


# ═══════════════════════════════════════════════════════════════════════════
#  DOWNLOADS
# ═══════════════════════════════════════════════════════════════════════════

def _chat_accounted(nas: dict, prefix: str) -> bool:
    """The raw is on the NAS, or the merge holds it and moved it to storage.

    Only an archived copy counts as folded in. With chat_archive_path unset
    the merge leaves raws in place, so a missing raw really is missing.
    """
    return bool(nas.get(f"{prefix}_chat") or nas.get(f"{prefix}_chats_archived"))


def _identify_missing(config: dict, nas: dict,
                      yt_id: str | None, tw_id: str | None,
                      absent: dict | None = None) -> list[dict]:
    """List files that should exist but don't, skipping known-absent (.×) ones."""
    absent = absent or {}
    missing = []
    # `video_id` rides along because the file a pull writes is NAMED after it
    # and a url is not a reliable place to get it back from. See `_pull_stem`.
    if yt_id:
        url = ls_common.build_stream_url(config, "youtube", yt_id)
        if not nas["yt_video"] and not absent.get("yt_video"):
            missing.append({"platform": "youtube", "type": "video",
                            "url": url, "video_id": yt_id, "label": "YT video"})
        if not _chat_accounted(nas, "yt") and not absent.get("yt_chat"):
            missing.append({"platform": "youtube", "type": "chat",
                            "url": url, "video_id": yt_id, "label": "YT chat"})
    if tw_id:
        url = ls_common.build_stream_url(config, "twitch", tw_id)
        if not nas["tw_video"] and not absent.get("tw_video"):
            missing.append({"platform": "twitch", "type": "video",
                            "url": url, "video_id": tw_id, "label": "TW video"})
        if not _chat_accounted(nas, "tw") and not absent.get("tw_chat"):
            missing.append({"platform": "twitch", "type": "chat",
                            "url": url, "video_id": tw_id, "label": "TW chat"})
    return missing


def _download_files(config: dict, missing: list[dict],
                    index: int) -> bool:
    """Offer interactive download of missing files. Returns True if any succeeded."""
    print("\n  Missing files:")
    for i, m in enumerate(missing, 1):
        print(f"    {i}) {m['label']}: {m['url']}")

    choice = input(
        "\n  Download (numbers / 'a' for all / Enter to skip): "
    ).strip().lower()
    if not choice:
        print("  Skipped.")
        return False

    if choice == "a":
        selected = missing
    else:
        try:
            indices = [int(x) - 1 for x in choice.split()]
            selected = [missing[i] for i in indices if 0 <= i < len(missing)]
        except ValueError:
            print("  ✗ Invalid input.")
            return False

    if not selected:
        print("  Nothing selected.")
        return False

    any_success = False
    for m in selected:
        got = _pull_one(config, index, m)
        print(f"  {'✔' if got['ok'] else '✗'} {got['why']}")
        any_success = any_success or got["ok"]

    return any_success


#  How long one pull may run before it is abandoned. A VOD is hours of video,
#  so this is not `archive_fetch_timeout_s`, which governs a pasted clip and is
#  sized for one. There is a number at all -- rather than None -- because a job
#  holds a lease it cannot renew while this runs.
PULL_TIMEOUT_S = 7200


def _pull_stem(config: dict, index: int, url: str, vid: str) -> str:
    """The name a file pulled for entry #index is written under.

    Asked of the PLATFORM rather than assembled from what is on disk, because
    the case this exists for is that nothing is on disk: a broadcast from a
    collab partner's channel has no local file to take a name from, which is
    exactly why its chat could never be fetched.

    THE `[id]` IS NOT DECORATION, and it is why the id is a parameter here
    rather than read off the probe. `scan_nas` skips any file it cannot read
    a video id out of — so a probe that fails, which is the ordinary outcome
    for a members-only VOD or a bot check, used to name the file
    `747_unknown @ 2026-09-26 05_31_13` and the download would land on the
    NAS invisible to every sweep afterwards. Gigabytes nothing would ever
    find. The id is the one part of the name that is known before the network
    is touched, so it is the one part that cannot go missing.
    """
    data = ls_common.ytdlp_probe(config, url, playlist_items="1")
    title = (data or {}).get("title") or "Unknown"
    release_ts = (data or {}).get("release_timestamp")
    upload_date = (data or {}).get("upload_date", "")
    if release_ts:
        ts = datetime.datetime.fromtimestamp(release_ts).strftime("%Y-%m-%d_%H-%M")
    elif upload_date and len(str(upload_date)) == 8:
        ts = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:]}_00-00"
    else:
        ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
    stem = sanitize_filename(f"{title} [{vid}] @ {ts}")
    return f"{int(index):03d}_{stem}"


def _pull_one(config: dict, index: int, item: dict, *,
              timeout: int | None = None) -> dict:
    """Download ONE missing file. Asks nothing, prints nothing a job cannot
    carry home. Returns {"ok", "file", "why"}.

    Split out of `_download_files` rather than written beside it: the terminal
    and the panel must not be able to drift into pulling different things
    under different names, and the name is most of what a pull IS.

    IT CHECKS THAT A FILE LANDED, which is the one thing the loop this came
    out of never did -- it set success unconditionally after `subprocess.run`,
    so a yt-dlp that exited 1 without writing a byte reported as a download.
    Harmless at a terminal, where the failure is on screen above the summary.
    Not harmless from a panel, where that is the whole of the answer.
    """
    url = item["url"]
    platform = item["platform"]
    what = item["type"]
    timeout = PULL_TIMEOUT_S if timeout is None else timeout
    nas_path = config["nas_path"]
    if not nas_path or not os.path.isdir(nas_path):
        return {"ok": False, "file": None, "why": "the NAS is not mounted"}
    # Nowhere to put it beats nothing to call it: the mount is the more
    # fundamental failure and the one a person fixes first.
    vid = item.get("video_id") or ls_common.extract_video_id_from_url(url)[0]
    # Canonical form, and this is not cosmetic. yt-dlp hands back a Twitch VOD
    # id as `v2883601443`; Helix, the archive and `build_stream_url` all use
    # the bare digits. A file named with the `v` still SCANS -- classify_video_id
    # strips it -- but `resolve_id` would then read that id off the filename,
    # and the archive push would propose `v2883601443` against the stored
    # `2883601443` on every sweep, for ever, over the same video.
    if vid and platform == "twitch":
        vid = vid.lstrip("v") or vid
    if not vid:
        return {"ok": False, "file": None,
                "why": f"no video id in {url} — a file named without one is "
                       f"invisible to every later sweep"}

    # Before the name, which costs a network round trip to build: a Twitch
    # chat needs a downloader this machine may not have, and spending a probe
    # to find that out means the answer arrives slower and says the wrong
    # thing about why.
    tdl = config.get("twitch_downloader_cli")
    if what == "chat" and platform == "twitch" and not (tdl and os.path.exists(tdl)):
        return {"ok": False, "file": None,
                "why": "twitch_downloader_cli is not configured; "
                       "a Twitch chat cannot be pulled without it"}

    stem = _pull_stem(config, index, url, vid)
    print(f"\n  ↓ {item['label']}: {stem}", flush=True)

    try:
        if what == "video":
            cmd = ls_common.ytdlp_vod_cmd(config, url, f"{stem}.%(ext)s")
            subprocess.run(cmd, cwd=nas_path, timeout=timeout)
            # Whatever container it settled on, and never a fragment: the
            # format string above can merge to mkv when a remux fails, and
            # `.f140.m4a` is an intermediate `scan_nas` skips for good reason.
            got = [f for f in glob.glob(os.path.join(nas_path, glob.escape(stem) + ".*"))
                   if os.path.splitext(f)[1].lower() in ls_common.VIDEO_EXTS
                   and not re.search(r"\.f\d+\.\w+$", f)
                   and os.path.getsize(f) > 0]
            if not got:
                return {"ok": False, "file": None,
                        "why": "nothing came back — no video file was written"}
            name = os.path.basename(max(got, key=os.path.getsize))
            return {"ok": True, "file": name,
                    "why": f"{name} — {os.path.getsize(os.path.join(nas_path, name)) / 2**30:.1f} GB"}

        if what == "chat":
            # Under the offline-pull name in both branches, which is beside any
            # capture rather than over it. See ls_common.OFFLINE_PULL_TAG: a
            # pull is one capture among however many the entry has, and the
            # merge is what decides between them.
            pull = ls_common.offline_pull_name(stem)
            final = os.path.join(nas_path, pull)
            if platform == "twitch":
                vod_id = url.rstrip("/").split("/")[-1]
                subprocess.run([tdl, "chatdownload", "--id", vod_id, "-o", final],
                               timeout=timeout)
            else:
                cmd = ls_common.ytdlp_chat_cmd(config, url, f"{stem}.%(ext)s")
                subprocess.run(cmd, cwd=nas_path, timeout=timeout)
                lc = os.path.join(nas_path, f"{stem}.live_chat.json")
                if os.path.exists(lc):
                    os.replace(lc, final)

            if not os.path.exists(final) or os.path.getsize(final) == 0:
                if os.path.exists(final):
                    os.remove(final)
                return {"ok": False, "file": None,
                        "why": "nothing came back — a broadcast id cannot be "
                               "downloaded from, and a muted or deleted VOD has "
                               "no chat to give"}
            # Parsed before it is kept, on the same bar as `_backfill_tw_chat`:
            # a file that does not read as chat is worse than no file, because
            # every later sweep sees a chat on disk and stops asking.
            try:
                got = ls_chat.convert_file(final)
            except (OSError, ValueError, json.JSONDecodeError) as e:
                os.remove(final)
                return {"ok": False, "file": None, "why": f"the pull does not parse: {e}"}
            if not got.messages:
                os.remove(final)
                return {"ok": False, "file": None,
                        "why": "the pull has no messages in it"}
            return {"ok": True, "file": pull,
                    "why": f"{pull} — {len(got.messages):,} messages"}

        return {"ok": False, "file": None, "why": f"nothing here pulls a {what!r}"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "file": None,
                "why": f"gave up after {timeout // 60} minutes"}
    except OSError as e:
        return {"ok": False, "file": None, "why": f"{type(e).__name__}: {e}"}


def pull(config: dict, index: int, platform: str, what: str,
         cache: list[dict] | None = None) -> dict:
    """Fetch a file this machine never recorded, because somebody said to.

    The sibling of `repair`, and the gap beside it. `repair` mends a capture
    that is HERE and short; this gets one that was never here at all -- the
    ordinary shape of a collab, where `ls-rec watch` was given one link and
    the other platform's broadcast exists only as a URL somebody pasted in
    afterwards. Until this, that URL could be entered, stored, resolved, and
    then acted on by nobody: the panel's only answers were "there was no
    broadcast" and "I did not keep it", and both of them are false.

    `what` is "video", "chat", or "both" -- "both" being the answer to the one
    question that covers a platform with nothing from it at all.

    Returns {"ran", "changed", "why", "files"}. `ran` False with a `why` is
    the honest no-op, not a failure.
    """
    if platform not in ("youtube", "twitch"):
        return {"ran": False, "changed": False, "files": [],
                "why": f"unknown platform {platform!r}"}
    if what not in ("video", "chat", "both"):
        return {"ran": False, "changed": False, "files": [],
                "why": f"nothing here pulls a {what!r}"}

    entry = ls_common.obsidian_parse_entry(config, index)
    if not entry["found"]:
        return {"ran": False, "changed": False, "files": [],
                "why": f"entry #{index} not found"}
    entry["_index"] = index
    cache = ls_common.load_cache() if cache is None else cache
    nas = scan_nas(config, index)
    # The same five sources the audit uses, in the same order -- including the
    # archive, which is where the URL for a broadcast on somebody else's
    # channel lives and the only place it can have come from.
    arch_ids = archive_ids(ls_archive.lookup(config, idx=index)
                           if ls_archive.enabled(config) else None)
    vid, _src = resolve_id(config, cache, platform, entry, nas, None, arch_ids)
    if not vid:
        return {"ran": False, "changed": False, "files": [],
                "why": f"no {platform} id for #{index}, so there is nothing to "
                       f"fetch from — paste the VOD link on the capture first"}

    yt_id = vid if platform == "youtube" else None
    tw_id = vid if platform == "twitch" else None
    want = {"video", "chat"} if what == "both" else {what}
    # Through `_identify_missing` rather than a url built here, so a pull can
    # never fetch something already on disk: the audit's idea of missing and
    # this one are the same idea.
    todo = [m for m in _identify_missing(config, nas, yt_id, tw_id)
            if m["type"] in want]
    if not todo:
        # Not a failure. The entry is in the state the asker wanted it in, and
        # the merge below may still have something to do.
        done = finish(config, index, cache=cache)
        return {"ran": False, "changed": done["changed"], "files": [],
                "why": f"the {platform} {'/'.join(sorted(want))} is already here"}

    files, why = [], []
    for m in todo:
        got = _pull_one(config, index, m)
        why.append(f"{m['label']}: {got['why']}")
        if got["ok"]:
            files.append(got["file"])

    # Whatever landed, finish what can be finished -- the merge is what makes
    # a pulled chat count as this entry's chat, and a pull nobody merged is a
    # file on a NAS that the next sweep reports as a second capture.
    changed = False
    try:
        done = finish(config, index, cache=cache)
        changed = done["changed"]
    except Exception as e:
        why.append(f"the merge afterwards failed: {type(e).__name__}: {e}")

    return {"ran": bool(files), "changed": changed, "files": files,
            "why": "; ".join(why)}


# ═══════════════════════════════════════════════════════════════════════════
#  CACHE MANAGEMENT COMMANDS
# ═══════════════════════════════════════════════════════════════════════════

def cmd_refresh(config: dict, platform: str):
    cache = ls_common.load_cache()
    if platform in ("all", "youtube"):
        print("  ⌛ Refreshing YouTube...")
        if ls_common.refresh_youtube_cache(config, cache, full=True):
            n = sum(1 for v in cache if v.get("platform") == "youtube")
            print(f"  ✔ YouTube: {n} VODs")
    if platform in ("all", "twitch"):
        print("  ⌛ Refreshing Twitch...")
        if ls_common.refresh_twitch_cache(config, cache, full=True):
            n = sum(1 for v in cache if v.get("platform") == "twitch")
            print(f"  ✔ Twitch: {n} VODs")
    ls_common.save_cache(cache)
    print("  ✔ Cache saved.")


def cmd_inject(config: dict, url: str | None = None):
    cache = ls_common.load_cache()
    if url:
        print(f"  ⌛ Fetching: {url}")
        data = ls_common.ytdlp_probe(config, url)
        if not data:
            print("  ⚠ Failed. Falling back to manual.")
            return _inject_manual(cache)

        platform = "twitch" if "twitch.tv" in url else "youtube"
        release_ts = data.get("release_timestamp")
        upload_date = data.get("upload_date", "")
        if release_ts:
            start = datetime.datetime.fromtimestamp(release_ts).isoformat()
        elif upload_date:
            start = datetime.datetime.strptime(upload_date, "%Y%m%d").isoformat()
        else:
            start = datetime.datetime.now().isoformat()

        vod = {
            "id":         data.get("id", "unknown"),
            "platform":   platform,
            "title":      data.get("title", "Unknown"),
            "start_time": start,
            "channel":    data.get("channel") or data.get("uploader") or "unknown",
            "duration":   data.get("duration"),
        }
    else:
        return _inject_manual(cache)

    _print_vod(vod)
    if input("\n  Add to cache? (y/n): ").strip().lower() == "y":
        ls_common.upsert_vod(cache, vod)
        ls_common.save_cache(cache)
        print("  ✔ Added.")


def _inject_manual(cache: list[dict]):
    """Interactive manual cache injection."""
    print("\n  Manual entry:")
    platform = input("  Platform (youtube/twitch): ").strip().lower()
    if platform not in ("youtube", "twitch"):
        print("  ✗ Invalid platform.")
        return
    vid_id = input("  Video ID: ").strip()
    if not vid_id:
        print("  ✗ ID required.")
        return
    title = input("  Title: ").strip() or "Unknown"
    date_str = input("  Start date (YYYY-MM-DD or ISO): ").strip()
    try:
        start = (date_str if "T" in date_str
                 else datetime.datetime.strptime(date_str, "%Y-%m-%d").isoformat())
    except ValueError:
        print("  ✗ Bad date format.")
        return
    dur = input("  Duration in seconds (Enter to skip): ").strip()
    channel = input("  Channel: ").strip() or "unknown"

    vod = {
        "id": vid_id, "platform": platform, "title": title,
        "start_time": start, "channel": channel,
        "duration": int(dur) if dur.isdigit() else None,
    }
    _print_vod(vod)
    if input("\n  Add to cache? (y/n): ").strip().lower() == "y":
        ls_common.upsert_vod(cache, vod)
        ls_common.save_cache(cache)
        print("  ✔ Added.")


def cmd_cache_info(vid_id: str):
    cache = ls_common.load_cache()
    vod = ls_common.find_vod(cache, vid_id)
    if vod:
        _print_vod(vod)
    else:
        print(f"  ✗ '{vid_id}' not in cache.")


def _print_vod(vod: dict):
    dur = vod.get("duration")
    if dur:
        dur_str = f"{dur}s ({dur // 3600}h{(dur % 3600) // 60:02d}m)"
    else:
        dur_str = "unknown"
    print(f"\n  Platform : {vod.get('platform')}")
    print(f"  ID       : {vod.get('id')}")
    print(f"  Title    : {vod.get('title')}")
    print(f"  Start    : {vod.get('start_time')}")
    print(f"  Duration : {dur_str}")
    print(f"  Channel  : {vod.get('channel', 'unknown')}")
    idx = vod.get("obsidian_index")
    if idx is not None:
        print(f"  Index    : #{idx}")


# ═══════════════════════════════════════════════════════════════════════════
#  TIMINGS SIDECAR
# ═══════════════════════════════════════════════════════════════════════════
#
#  Two instants per platform: when the broadcast started, and when we started
#  recording it. New recordings have both in the cache. Older ones are
#  reconstructed, best source first, and every value carries where it came
#  from and how accurate it is -- an unattributed timestamp is worse than a
#  missing one.

FILENAME_TS_RE = re.compile(r"@\s*(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})")


def _filename_epoch_ms(filename: str) -> int | None:
    """The `@ YYYY-MM-DD_HH-MM` stamp, to the minute."""
    m = FILENAME_TS_RE.search(filename or "")
    if not m:
        return None
    try:
        return int(datetime.datetime.strptime(
            m.group(1), "%Y-%m-%d_%H-%M").timestamp() * 1000)
    except ValueError:
        return None


def _log_record_start(config: dict, nas_file: str) -> int | None:
    """
    Scan the recorder log for this stream's first part.

    The log is written to the daemon's working directory, so it is only found
    if ls-audit runs from the same place. It also rolls over, covering the
    last few dozen recordings at most.
    """
    stem = re.sub(r"^\d+_", "", os.path.splitext(nas_file)[0])
    for cand in (config.get("log_file"),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "livestream_recorder.log"),
                 "livestream_recorder.log"):
        if not cand or not os.path.exists(cand):
            continue
        pat = re.compile(r"^([\d\-]{10} [\d:]{8}),\d+ .*Part 01 started: "
                         + re.escape(stem))
        try:
            with open(cand, encoding="utf-8", errors="replace") as f:
                for line in f:
                    m = pat.match(line)
                    if m:
                        return int(datetime.datetime.strptime(
                            m.group(1), "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
        except OSError:
            continue
    return None


def _iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.datetime.fromtimestamp(ms / 1000).isoformat(timespec="seconds")


def _kept_testimony(config: dict, index: int) -> dict:
    """{prefix: [witness, ...]} out of the entry's sidecar, per platform.

    One reader for the three places that need it. A sidecar written before
    testimony was a thing has no `testimony` key, so its history is taken off
    the settlement blocks it does carry -- `witnesses` plus `refused`, which
    between them are everything `settle` was given. Without that fallback the
    first sweep after this change would start every entry from empty, which
    is precisely the loss it exists to prevent.
    """
    prior = ls_common.read_meta(os.path.join(
        config.get("nas_path", ""), ls_common.entry_meta_name(index))) or {}
    out = {}
    for prefix, platform in (("yt", "youtube"), ("tw", "twitch")):
        old = prior.get(platform) or {}
        keep = old.get("testimony")
        if keep is None:
            keep = [w for st in (old.get("settled") or {}).values()
                    for w in ((st or {}).get("witnesses") or [])
                    + ((st or {}).get("refused") or [])]
        out[prefix] = keep
    return out


def _platform_timings(config: dict, cache: list[dict], nas: dict,
                      prefix: str, platform: str,
                      kept: list[dict] | None = None) -> dict | None:
    """Best-effort timings for one platform, with provenance on every value.

    `kept` is what this entry's sidecar already held for this platform. Fresh
    readings replace it per (claim, source) and the rest survives -- see
    `ls_witness.merge_testimony`. Absent, this behaves exactly as it did.
    """
    nas_root = config.get("nas_path", "")
    chat_file = nas.get(f"{prefix}_chat")
    video_file = nas.get(f"{prefix}_video")
    #  NOT "no files, nothing to say" any more. That line is why deleting one
    #  recording erased its platform's whole block: the sidecar was rebuilt
    #  from the files present this minute, and a platform with none was simply
    #  not in the new document. What a witness said once does not stop being
    #  true because the file it said it about has gone -- and for the two wall
    #  times, nothing can ever say it again.
    if not (chat_file or video_file or kept):
        return None

    vid = (ls_common.extract_video_id_from_filename(chat_file or video_file)
           if (chat_file or video_file) else None)
    vod = (ls_common.find_vod(cache, vid, platform) or {}) if vid else {}

    # ── GATHER, then settle. This was a fallback chain until C3 ─────────────
    #
    # Every witness is asked, every answer is kept, and `ls_witness.settle`
    # applies the authority table to pick one. What changed is not usually the
    # ANSWER — it is that the losers survive, so a caller can tell "the cache
    # said 13:02" from "four sources agreed on 13:02", which nothing could
    # before. The old chain stopped at the first hit and the rest were never
    # asked, so a wrong first answer was silent.
    #
    # Two behavioural corrections fall out of the table rather than out of
    # code here:
    #   · the recorder's naive local `start_time` is a witness to RECORD start
    #     and is refused as a broadcast start, which it had always been
    #     offered as — five hours adrift on 257 of 450 cached rows;
    #   · the filename's minute is offered to both clocks at the lowest rank
    #     either has, instead of being hand-gated against `stream_ms` by a
    #     60-second test that only existed because there was nowhere to record
    #     "this is coarse".
    ws = []
    #  The recorder's own sidecar, when one exists. Nothing has one yet — C1
    #  ships the writer and the back catalogue will never have them — so this
    #  is the witness that starts absent and gets denser over months.
    if video_file or chat_file:
        side = ls_common.read_meta(os.path.join(
            nas_root, ls_common.meta_name(
                os.path.splitext(video_file or chat_file)[0])))
        ws += ls_witness.read_sidecar(side)
    ws += ls_witness.read_cache(vod, config.get("tz_offset_min"))

    # The chat file itself — exact, and it works for the whole back catalogue.
    # What the zero MEANS depends on the format, which is why the routing
    # lives in `read_chat_zero` rather than in an `if` here.
    if chat_file:
        zero, zsrc = ls_chat.peek_zero(os.path.join(nas_root, chat_file))
        ws += ls_witness.read_chat_zero(zero, zsrc)

    # The recorder log — second accurate, but only the recent past: it is
    # written to the daemon's working directory and rolls over after a few
    # dozen recordings.
    if video_file or chat_file:
        ws += ls_witness.read_log(_log_record_start(config, video_file or chat_file))

    fname_ms = _filename_epoch_ms(chat_file or video_file)
    ws += ls_witness.read_filename(fname_ms)

    # Two different facts have always shared one field, and nothing said which
    # one you were holding. The cache's number is what the PLATFORM says the
    # broadcast ran for; ffprobe's is how long the FILE is. They disagree
    # legitimately — a VOD trimmed at the far end, a capture that started late
    # or died early — and the disagreement is a finding rather than noise.
    # They are two CLAIMS now, so nothing has to choose between them.
    measured = None
    if video_file:
        vp = os.path.join(nas_root, video_file)
        if os.path.exists(vp):
            measured = analyze_video_file(vp).get("duration_secs")
            ws += ls_witness.read_ffprobe(measured)

    #  Everything heard before, under everything heard now. The settlement
    #  runs over the merged set, so a claim whose only live witness has gone
    #  is still answered -- by the last source that answered it, marked stale.
    ws = ls_witness.merge_testimony(
        kept, ws, now_s=int(datetime.datetime.now().timestamp()))

    stream = ls_witness.settle(ls_witness.BROADCAST_START, ws)
    record = ls_witness.settle(ls_witness.RECORD_START, ws)
    on_disk = ls_witness.settle(ls_witness.FILE_DURATION, ws)
    broadcast = ls_witness.settle(ls_witness.BROADCAST_DURATION, ws)

    stream_ms, record_ms = stream["value"], record["value"]
    #  The labels the CLI and the archive have always displayed. The witness's
    #  `source` is the authority table's key; the label is what a person
    #  reads, and keeping them apart is what let the table be built without
    #  changing a word of the output.
    stream_src = (stream["best"] or {}).get("label")
    record_src = (record["best"] or {}).get("label")

    #  The file still wins for the number in `duration_secs`, because that is
    #  what every existing caller means by it. The platform's figure is kept
    #  beside it rather than instead of it.
    duration = on_disk["value"] if on_disk["value"] else broadcast["value"]
    duration_src = ((on_disk["best"] or {}).get("label") if on_disk["value"]
                    else (broadcast["best"] or {}).get("label"))

    def acc(src):
        if src is None:
            return None
        return "minute" if "filename" in src else "exact"

    return {
        #  What C3 adds, beside everything that was already here. The
        #  settlements carry every witness that answered, every one that was
        #  refused, who won and by how far anybody disagreed — which is the
        #  product of this whole round and is additive on purpose, so no
        #  existing reader has to change to keep working.
        #  THE STORE, beside the derived view of it. `settled` is what the
        #  witnesses add up to and is recomputed every run; this is the
        #  witnesses themselves, and it is the thing that persists.
        "testimony": ws,
        "settled": {"broadcast_start": stream, "record_start": record,
                    "file_duration": on_disk, "broadcast_duration": broadcast},
        "video_id": vid,
        "stream_start_epoch_ms": stream_ms,
        "stream_start_iso": _iso(stream_ms),
        "stream_start_source": stream_src,
        "stream_start_accuracy": acc(stream_src),
        "record_start_epoch_ms": record_ms,
        "record_start_iso": _iso(record_ms),
        "record_start_source": record_src,
        "record_start_accuracy": acc(record_src),
        "duration_secs": duration,
        # `ffprobe` means the file on disk; `cache` means the platform's own
        # number for the broadcast. See the note above the assignment.
        "duration_source": duration_src,
        # The raw ffprobe reading, kept beside the settlement. The
        # reconciler below compares PLATFORMS against each other and needs
        # what each file actually measured, not what each settled on.
        "measured_duration_s": measured,
        "filename_epoch_ms": fname_ms,
        "files": {"video": video_file, "chat": chat_file},
    }


def _length_findings(config: dict, nas: dict, timings: dict) -> list[dict]:
    """One recording's length denied by the other's.

    This is the loudest thing an audit can find about an entry and it had no
    voice at all. Entry #716: YouTube ffprobes at 6:25:11, Twitch at 2:08:10,
    and every check downstream believed the first — the chat looked 254
    minutes short, so the entry asked whether to download four hours of replay
    chat that has never existed.

    Suppressing that question without saying WHY would only move the problem:
    the entry would go quiet while still carrying a length that is wrong in
    the vault, on its card, and in the timeline. So the dispute is reported
    here, in the one sentence a person needs — both numbers, and what else
    agrees with which.

    `_reconcile_durations` deliberately does not act on this case: with the
    two recordings this far apart, neither is evidence about the other, and
    the archive cannot tell the bugged file from the capture that died early.
    A person can, in a second, from the numbers below.
    """
    got = {k: t.get("measured_duration_s") for k, t in (timings or {}).items()
           if t and t.get("measured_duration_s")}
    name = {"yt": "YouTube", "tw": "Twitch"}

    #  ── ONE RECORDING, so ask the source ─────────────────────────────
    #
    #  Two recordings check each other. One has nothing to check against, and
    #  this is the case that has gone wrong repeatedly: a file twice as long
    #  as the stream, believed by every number downstream.
    #
    #  Only when the entry is ALREADY suspect. The chat's last message is a
    #  lower bound on how long the broadcast ran — a chat can be cut off, so
    #  it can never prove a recording too long — but a recording that claims
    #  to be hours longer than its own chat is worth one metadata call to
    #  settle. A healthy solo entry makes no network call at all.
    if len(got) == 1:
        k = next(iter(got))
        t = (timings or {}).get(k) or {}
        chat_end = None
        f = nas.get(f"{k}_chat")
        if f and os.path.exists(os.path.join(config.get("nas_path", ""), f)):
            chat_end = analyze_chat_file(
                os.path.join(config.get("nas_path", ""), f)).get("last_secs")
        if not chat_end or got[k] - chat_end <= _TRUNCATION_FLOOR_S:
            return []
        aired = _ask_the_source(config, nas, k, t.get("video_id"))
        if not aired or abs(aired - got[k]) <= _TRUNCATION_FLOOR_S:
            #  Either the source will not say, or it agrees with the file —
            #  in which case the chat really is short and the coverage check
            #  is right to ask about it. Nothing to report here either way.
            return []
        return [_finding(
            "warn", "duration",
            f"{name.get(k, k)} measures {_seconds_to_hhmmss(got[k])} but "
            f"{name.get(k, k)} itself says the video is "
            f"{_seconds_to_hhmmss(aired)} — and its chat runs to "
            f"{_seconds_to_hhmmss(chat_end)}, which agrees with the source. "
            f"The recording on disk is the wrong one; no check is measured "
            f"against it.",
            platform=k, short="length disputed", aired_s=int(aired))]

    if len(got) < 2:
        return []
    lo_k = min(got, key=got.get)
    hi_k = max(got, key=got.get)
    if got[hi_k] - got[lo_k] <= _TRUNCATION_FLOOR_S:
        return []

    #  What else there is to go on, said in the same breath. A chat's last
    #  message is a LOWER bound on how long the broadcast ran — a chat can be
    #  cut off, so it can never prove a recording too long — but when it lands
    #  next to the shorter file it says which of the two to believe, and that
    #  is the whole decision.
    #  Read HERE and not in `_platform_timings`, which every audit runs: this
    #  is a second full pass over a chat file and only a disputed entry needs
    #  it. Rare by construction, so it costs nothing on a healthy sweep.
    corroborates = []
    for k in got:
        f = nas.get(f"{k}_chat")
        if not f:
            continue
        path = os.path.join(config.get("nas_path", ""), f)
        if not os.path.exists(path):
            continue
        chat_end = analyze_chat_file(path).get("last_secs")
        if chat_end and abs(chat_end - got[lo_k]) <= _TRUNCATION_FLOOR_S:
            corroborates.append(f"{name.get(k, k)}'s chat runs to "
                                f"{_seconds_to_hhmmss(chat_end)}")
    also = (" — " + ", and ".join(corroborates)) if corroborates else ""
    return [_finding(
        "warn", "duration",
        f"{name.get(hi_k, hi_k)} measures {_seconds_to_hhmmss(got[hi_k])} but "
        f"{name.get(lo_k, lo_k)} recorded the same broadcast in "
        f"{_seconds_to_hhmmss(got[lo_k])}{also}. One of these files is wrong "
        f"and nothing here can say which, so the length is left alone and no "
        f"check is measured against it.",
        platform=hi_k, short="length disputed")]


def _ask_the_source(config: dict, nas: dict, prefix: str, vid: str | None) -> float | None:
    """How long the platform says this video is. None if it will not say.

    THE ONLY WITNESS LEFT when a broadcast was recorded once. Two recordings
    check each other — a capture that died early looks complete from the
    inside, and only the other one says otherwise — but a solo entry has
    nothing to compare against, and a file that is twice as long as the
    stream is believed by everything downstream: the vault duration, the
    card, the timeline, and the chat-coverage check, which then reports hours
    missing from a complete log and offers to download them.

    Entry #747: the YouTube recording measures 6:19:06 and the chat stops at
    3:09:10, which is 2.004 times. Nothing local could tell which was wrong.
    The source can, in one metadata call.

    A PROBE, NOT A DOWNLOAD. The audit's rule against spending bandwidth
    unattended is about pulling files; `do_rescan` already reads this from
    every capture on a timer, and it is one request. Called only when it can
    decide something — see the caller — so a healthy sweep makes no network
    calls at all.

    Never raises: this is a courtesy, and an entry that cannot be checked
    against its source is exactly as well off as it was before.
    """
    if not vid:
        return None
    platform = "youtube" if prefix == "yt" else "twitch"
    try:
        url = ls_common.build_stream_url(config, platform, vid)
        data, why = ls_common.ytdlp_probe(config, url, with_reason=True)
        if why != "ok" or not data:
            return None
        d = data.get("duration")
        return float(d) if d else None
    except Exception as e:
        logging.getLogger(__name__).info(f"could not ask {platform} about {vid}: {e}")
        return None


def _reconcile_durations(config: dict, cache: list[dict], timings: dict) -> list[str]:
    """Correct a cached broadcast length that the recordings disprove.

    THE CACHE GOES STALE AND NOTHING EVER FIXED IT. The pre-C audit said so in
    one line — "the cache is written once at record time, so after a
    re-download or a manual repair it reports the old length forever" — and
    corrected it on the spot. C turned the file's length and the broadcast's
    length into two claims, which is right, and then had no rule connecting
    them: entry #716 carried a cached 6:25:11 against a two-hour recording and
    reported the disagreement on every single sweep, for ever.

    WHY THIS CANNOT BE DONE ONE PLATFORM AT A TIME, which is the mistake made
    first. A cached figure longer than the file looks identical in both of the
    cases that matter:

        the cache is wrong   — a bugged recording's length was cached, or the
                               file was re-downloaded and is now correct;
        the FILE is short    — the capture died early, and the cache is the
                               only thing that still knows how long the
                               broadcast actually ran.

    Correcting on the first reading would quietly overwrite the truth in the
    second. Nothing inside one platform can tell them apart — a capture that
    died early looks complete from the inside.

    THE OTHER RECORDING IS THE EVIDENCE. A broadcast recorded twice should
    produce two files of nearly the same length. When both platforms measured
    and AGREE, that agreement is what the broadcast ran for, and a cached
    figure that disagrees with it is simply wrong. When only one platform
    recorded, this declines and says nothing — there is no second witness, and
    a guess written into the cache is worse than a disagreement reported.

    The floor is `_TRUNCATION_FLOOR_S`, the same number the truncation finding
    uses, and deliberately the same: below it a difference is a trim at the far
    end of a VOD and the two-claim model is right to keep both. Above it,
    something is wrong.

    Returns the lines it wants said. Mutates `cache` and the settled
    broadcast_duration in `timings`, so the findings drawn afterwards read the
    corrected figure rather than the one this just replaced.
    """
    measured = {k: t.get("measured_duration_s") for k, t in timings.items()
                if t and t.get("measured_duration_s")}
    if len(measured) < 2:
        return []
    lo, hi = min(measured.values()), max(measured.values())
    if hi - lo > _TRUNCATION_FLOOR_S:
        # The two recordings do not agree either, so neither is evidence about
        # the other and there is nothing here to correct anything WITH.
        return []
    aired = max(measured.values())

    said = []
    for prefix, t in timings.items():
        if not t:
            continue
        settled = (t.get("settled") or {}).get("broadcast_duration") or {}
        cached = settled.get("value")
        if not cached or abs(float(cached) - float(aired)) <= _TRUNCATION_FLOOR_S:
            continue
        vid = t.get("video_id")
        platform = "youtube" if prefix == "yt" else "twitch"
        vod = ls_common.find_vod(cache, vid, platform) if vid else None
        if not vod or not vod.get("duration"):
            continue
        said.append(f"    duration corrected from cache "
                    f"{_seconds_to_hhmmss(vod['duration'])} → "
                    f"{_seconds_to_hhmmss(aired)} ({prefix}) — both recordings "
                    f"say {_seconds_to_hhmmss(aired)}")
        vod["duration"] = int(aired)
        settled["value"] = int(aired)
        if settled.get("best"):
            settled["best"]["value"] = int(aired)
            settled["best"]["label"] = "cache (corrected)"
    if said:
        try:
            ls_common.save_cache(cache)
        except Exception as e:
            logging.getLogger(__name__).warning(
                f"could not save the corrected cache: {e}")
    return said


def _old_entry_metas(nas_root: str, index: int, keep: str) -> list[str]:
    """Aggregates for this entry sitting under the old per-recording name.

    Identified POSITIVELY — by what the document says about itself — and not
    by ruling the recorder out. The glob `NNN_*.meta.json` matches the
    recorder's sidecars too, since a capture's filename starts with the same
    index, and `meta_is_recorder` answers False for a file it could not read.
    Negative identification would therefore rename a CORRUPT recorder sidecar,
    which is the one file in this tree that cannot be rebuilt.

    So: it must parse, it must carry the audit's own `schema`/`index` pair,
    and it must not be stamped as the recorder's. A recorder document has
    `platform`, `video_id` and `obsidian_index` and none of those.
    """
    if not nas_root or not os.path.isdir(nas_root):
        return []
    out = []
    for name in sorted(os.listdir(nas_root)):
        path = os.path.join(nas_root, name)
        if path == keep or not name.endswith(".meta.json"):
            continue
        if not name.startswith(f"{int(index):03d}_"):
            continue
        doc = ls_common.read_meta(path)
        if not doc or ls_common.meta_is_recorder(doc):
            continue
        try:
            if int(doc.get("index", -1)) != int(index) or "schema" not in doc:
                continue
        except (TypeError, ValueError):
            continue
        out.append(path)
    return out


def _migrate_entry_meta(nas_root: str, index: int, target: str) -> None:
    """Move this entry's aggregate onto `NNN_meta.json`, once.

    A rename rather than a rewrite-and-delete: it loses nothing, it is
    reversible by hand, and it means the `already current` check below
    compares against the content that was really there instead of rewriting
    every entry in the archive on one sweep just to change a filename.

    Refuses rather than guesses in both of the ways this can be ambiguous:

      · TWO candidates means the old name already flipped between platforms
        at some point, so there are two aggregates and no way from here to
        know which one is the entry's current truth. Picking would be picking
        at random.
      · The target already existing means something newer is there. Renaming
        over it with `os.replace` would silently destroy it.

    Both print and leave every file where it is. Nothing is deleted here,
    now or later — the worst case is litter somebody reads and removes.
    """
    olds = _old_entry_metas(nas_root, index, target)
    if not olds:
        return
    if len(olds) > 1:
        print(f"  ! {len(olds)} old-format sidecars for #{index}, so which one "
              f"is current cannot be known from here — left alone:")
        for p in olds:
            print(f"      {os.path.basename(p)}")
        return
    if os.path.exists(target):
        print(f"  ! {os.path.basename(olds[0])} is the old name for "
              f"{os.path.basename(target)}, which already exists — left alone")
        return
    try:
        os.replace(olds[0], target)
        print(f"  · renamed {os.path.basename(olds[0])} "
              f"→ {os.path.basename(target)}")
    except OSError as e:
        # Never fatal. The sidecar is about to be written under the new name
        # regardless, and a rename that failed leaves a readable file behind
        # rather than losing one.
        print(f"  ! could not rename {os.path.basename(olds[0])}: {e}")


def cmd_timings(config: dict, index: int, output: str | None = None,
                dry_run: bool = False):
    """Write a timings sidecar for one entry."""
    print(f"\n{'=' * 60}")
    print(f"  Timings for entry #{index}")
    print(f"{'=' * 60}")

    nas = scan_nas(config, index)
    cache = ls_common.load_cache()

    #  What this entry's sidecar already holds, read BEFORE anything is
    #  derived, because it is an input now rather than merely the thing about
    #  to be overwritten.
    prior_ws = _kept_testimony(config, index)

    doc = {"schema": 1, "index": int(index),
           "generated_at": datetime.datetime.now().isoformat(timespec="seconds")}
    any_found = False

    gathered = {}
    for prefix, platform in (("yt", "youtube"), ("tw", "twitch")):
        t = _platform_timings(config, cache, nas, prefix, platform,
                              prior_ws.get(prefix))
        if t:
            gathered[prefix] = t
    #  Before anything is printed or written down: a sidecar recording a
    #  broadcast length the recordings disprove would be a derivation of a
    #  stale number, kept for ever.
    for line in _reconcile_durations(config, cache, gathered):
        print(line)

    for prefix, platform in (("yt", "youtube"), ("tw", "twitch")):
        t = gathered.get(prefix)
        if not t:
            print(f"  {platform:<8} no files")
            continue
        any_found = True
        doc[platform] = t
        print(f"  {platform}")
        for label, key in (("stream start", "stream_start"),
                           ("record start", "record_start")):
            iso, src = t[f"{key}_iso"], t[f"{key}_source"]
            if iso:
                print(f"    {label}  {iso}  [{src}]")
            else:
                print(f"    {label}  UNKNOWN")
        if t["duration_secs"]:
            print(f"    duration      {_seconds_to_hhmmss(t['duration_secs'])}"
                  f"  [{t.get('duration_source') or 'unknown'}]")

    # False rather than a bare return, now that the caller reports whether a
    # sweep actually did anything. "Nothing to record" and "already current"
    # are both honest noes.
    if not any_found:
        print("\n  Nothing to record.\n")
        return False

    if dry_run:
        print("\n  --dry-run: nothing written.\n")
        return False

    if not output:
        output = os.path.join(config.get("nas_path", ""),
                              ls_common.entry_meta_name(index))
        # Whatever this entry's aggregate was called before it was called
        # that. Only on the default path: a caller that named a file meant
        # that file.
        _migrate_entry_meta(config.get("nas_path", ""), index, output)

    # A recorder-written sidecar is not ours to touch. It is a contemporaneous
    # claim by the only party present, and this one is a derivation from files
    # and logs — overwriting evidence with a reconstruction of it is the exact
    # direction the whole sidecar split exists to prevent.
    existing = ls_common.read_meta(output)
    if ls_common.meta_is_recorder(existing):
        print(f"\n  · {os.path.basename(output)} was written by the recorder "
              f"— left alone\n")
        return False

    # Otherwise it IS refreshable, and it needs to be: this used to be written
    # once and never again, so a sidecar written while the cache was wrong, or
    # before a chat backfill landed, could never be corrected. Rewritten only
    # when something actually changed, though — the stamps move on every run by
    # construction, and a sweep that reported work it did not do would be
    # noise of exactly the kind this tool is meant to remove.
    def _content(d):
        return {k: v for k, v in (d or {}).items()
                if k not in ("meta_version", "written_by", "written_at")}

    if existing is not None and _content(existing) == _content(doc):
        print(f"\n  · {os.path.basename(output)} is already current\n")
        return False

    ls_common.write_meta(output, doc, by="audit")
    print(f"\n  ✔ {output}{' (refreshed)' if existing else ''}\n")
    return True


# ═══════════════════════════════════════════════════════════════════════════
#  YOUTUBE CHAT COVERAGE
# ═══════════════════════════════════════════════════════════════════════════
#
#  A live_chat writer that dies mid-stream leaves a chat that just stops --
#  two hours of an eight hour VOD. Flagged when the shortfall exceeds an hour
#  or half the video, whichever is smaller, so short streams are judged
#  proportionally and long ones by an absolute bar.

CHAT_SHORTFALL_MAX_SECS = 3600
CHAT_SHORTFALL_FRACTION = 0.5

#  How far a capture's own clock may sit outside the broadcast before the
#  capture is suspected of belonging to a different stream. An hour, measured
#  against the MEDIAN message time so one stray row cannot raise it -- this
#  runs on every sweep, and a false alarm here offers a download every time.
TW_ASSIGN_WINDOW_SECS = 3600


def _yt_chat_shortfall(config: dict, cache: list[dict], nas: dict,
                       yt_id: str | None) -> dict | None:
    """Return details if the YouTube chat stops well short of the video."""
    chat_file = nas.get("yt_chat")
    if not chat_file:
        return None
    chat_path = os.path.join(config.get("nas_path", ""), chat_file)
    if not os.path.exists(chat_path):
        return None

    info = analyze_chat_file(chat_path)
    last = info.get("last_secs")
    if last is None or not isinstance(info["count"], int) or not info["count"]:
        return None

    # ffprobe first: it measures the file actually held, whereas the cache
    # holds the published length.
    duration = None
    video_file = nas.get("yt_video")
    if video_file:
        vp = os.path.join(config.get("nas_path", ""), video_file)
        if os.path.exists(vp):
            duration = analyze_video_file(vp).get("duration_secs")
    if not duration and yt_id:
        vod = ls_common.find_vod(cache, yt_id, "youtube") or {}
        duration = vod.get("duration")
    if not duration or duration <= 0:
        return None

    #  IS THAT LENGTH WORTH MEASURING AGAINST? Entry #716's YouTube file
    #  ffprobes at 6:25:11 and is simply wrong — the Twitch recording of the
    #  same broadcast is 2:08:10 and the YouTube chat's own last message is at
    #  2:10:22. Measured against the bogus figure the chat looks 254 minutes
    #  short, so this asked whether to download four hours of replay chat that
    #  has never existed, and answering it either way was a wrong answer to a
    #  question that should not have been asked.
    #
    #  So: when the OTHER platform recorded the same broadcast and its file
    #  disagrees by more than the truncation floor, this video's length is in
    #  dispute and a shortfall computed from it is not a fact. Declines, and
    #  the disputed length is reported instead by `_length_findings`.
    #
    #  The other recording only, never the chat's own span: a chat that was
    #  cut off IS short, and letting it vouch for the length it stops at would
    #  make this check unable to fire at all — which is the failure it exists
    #  to catch.
    other = _measured(config, nas, "tw")
    if other and abs(other - duration) > _TRUNCATION_FLOOR_S:
        return None

    #  AND WHEN THERE IS NO OTHER RECORDING, ask the source. Entry #747 had
    #  one YouTube file measuring 6:19:06 against a chat ending at 3:09:10,
    #  and no second capture to catch it — so this asked whether to download
    #  189 minutes of replay chat that has never existed.
    #
    #  Only once the chat already looks short by more than the floor, which
    #  is checked below — so the call is made at the moment it can change the
    #  answer and never on a healthy entry.
    if not other and (duration - last) > _TRUNCATION_FLOOR_S:
        aired = _ask_the_source(config, nas, "yt", yt_id)
        if aired and abs(aired - duration) > _TRUNCATION_FLOOR_S:
            return None

    shortfall = duration - last
    limit = min(CHAT_SHORTFALL_MAX_SECS, duration * CHAT_SHORTFALL_FRACTION)
    if shortfall <= limit:
        return None

    return {"platform": "youtube", "why": ["short"],
            "chat_file": chat_file, "chat_path": chat_path, "video_id": yt_id,
            "count": info["count"], "last_secs": last,
            "duration_secs": duration, "shortfall_secs": shortfall,
            "limit_secs": limit}


def _measured(config: dict, nas: dict, prefix: str) -> float | None:
    """How long one platform's video file actually is, or None.

    Its own function because two checks and the reconciler all need the same
    number and none of them should be the place that knows how to get it.
    """
    f = nas.get(f"{prefix}_video")
    if not f:
        return None
    path = os.path.join(config.get("nas_path", ""), f)
    if not os.path.exists(path):
        return None
    return analyze_video_file(path).get("duration_secs")


def _tw_chat_shortfall(config: dict, cache: list[dict], nas: dict,
                       tw_id: str | None, index: int) -> dict | None:
    """
    Return details if the Twitch capture is short, or is not this stream's, or
    cannot be placed in time at all.

    Not only a length check, and that is the point: the failures this has
    actually had are four shapes with one remedy, which is to pull the VOD's
    own chat and let the merge decide between them.

      unreadable it does not parse as any chat format we know
      short      chat that stops well before the video does — the same test
                 and the same bar as YouTube
      misplaced  a capture whose own clock sits an hour or more outside the
                 broadcast. One stream's Twitch capture landing on another
                 stream's row is a thing that has happened here.
      no clock   no message carries an absolute time and the cache holds no
                 start for this id either. This is the quiet one: the merge
                 drops every such message, writes a file that then reads as
                 finished, and the loss sticks. Every capture from before the
                 recorder wrote `tmi_sent_ts` is in this state.
      empty      a capture with no messages in it at all. Not the same as a
                 missing chat, which `_identify_missing` already offers to
                 fetch — this one exists, so nothing else notices it.

    Read through ls_chat rather than analyze_chat_file, which reads `timestamp`
    as a relative microsecond offset and is wrong about two of the three
    spellings a Twitch capture can arrive in.
    """
    chat_file = nas.get("tw_chat")
    if not chat_file:
        return None
    if chat_given_up(index, "twitch"):
        return None                        # said once, by a person. Enough.
    nas_root = config.get("nas_path", "")
    chat_path = os.path.join(nas_root, chat_file)
    if not os.path.exists(chat_path):
        return None

    try:
        conv = ls_chat.convert_file(chat_path)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        # Unreadable is not nothing, and answering None here is how an entry
        # created for a recording that never started stayed invisible: it HAS a
        # chat file, so `_identify_missing` is satisfied, and it has no format,
        # so everything downstream skipped it. Same remedy either way.
        return {"platform": "twitch", "why": ["unreadable"],
                "chat_file": chat_file, "chat_path": chat_path,
                "video_id": tw_id, "count": 0, "placed": 0,
                "last_secs": None, "duration_secs": None,
                "shortfall_secs": None, "limit_secs": None,
                "broadcast_ms": None, "median_ms": None, "error": str(e)}

    msgs = conv.messages
    placed = [m for m in msgs if m.abs_ms is not None]
    cache_zero = _cache_zeros(cache, None, tw_id).get("twitch")
    zero = conv.zero_ms or cache_zero
    why: list[str] = []

    if not msgs:
        why.append("empty")
    elif not placed and not cache_zero:
        why.append("no clock")

    # The recorder's own row is the best source for both of these: it wrote the
    # start when it attached and filled the duration in when the stream ended.
    # `find_vod`, not `find_confirmed_vod` — a broadcast row is exactly the row
    # that knows, and refusing it here would throw the answer away.
    vod = ls_common.find_vod(cache, tw_id, "twitch") if tw_id else None
    duration = None
    video_file = nas.get("tw_video")
    if video_file:
        vp = os.path.join(nas_root, video_file)
        if os.path.exists(vp):
            duration = analyze_video_file(vp).get("duration_secs")
    if not duration and vod:
        duration = vod.get("duration")

    # Offsets the file states itself are the ones to measure by. A capture
    # whose every offset is zero has none — it states absolute times only —
    # so measure from those against the zero instead, and if there is no zero
    # either then there is nothing to measure and "no clock" is the finding.
    last_secs = None
    if any(m.ts for m in msgs):
        last_secs = max(m.ts for m in msgs) / 1000.0
    elif placed and zero:
        last_secs = (max(m.abs_ms for m in placed) - zero) / 1000.0

    shortfall = limit = None
    #  The same guard the YouTube side carries, and for the same reason: a
    #  shortfall is measured AGAINST a length, so a length the other recording
    #  denies produces a shortfall that is not a fact. See `_yt_chat_shortfall`.
    other = _measured(config, nas, "yt")
    contested = bool(other and duration
                     and abs(other - duration) > _TRUNCATION_FLOOR_S)
    #  The solo case, as on the YouTube side: with no second recording the
    #  source is the only thing that can say whether this file's length is
    #  real. Asked only once the chat already looks short.
    if (not other and duration and last_secs is not None
            and (duration - last_secs) > _TRUNCATION_FLOOR_S):
        aired = _ask_the_source(config, nas, "tw", tw_id)
        if aired and abs(aired - duration) > _TRUNCATION_FLOOR_S:
            contested = True
    if last_secs is not None and duration and duration > 0 and not contested:
        shortfall = duration - last_secs
        limit = min(CHAT_SHORTFALL_MAX_SECS, duration * CHAT_SHORTFALL_FRACTION)
        if shortfall > limit:
            why.append("short")

    started = None
    if vod:
        started = (vod.get("record_start_epoch_ms")
                   or vod.get("stream_start_epoch_ms"))
    median_ms = None
    if placed and started and duration:
        mid = sorted(m.abs_ms for m in placed)
        median_ms = mid[len(mid) // 2]
        lo = started - TW_ASSIGN_WINDOW_SECS * 1000
        hi = started + int((duration + TW_ASSIGN_WINDOW_SECS) * 1000)
        if not lo <= median_ms <= hi:
            why.append("misplaced")

    if not why:
        return None
    return {"platform": "twitch", "why": why,
            "chat_file": chat_file, "chat_path": chat_path, "video_id": tw_id,
            "count": len(msgs), "placed": len(placed),
            "last_secs": last_secs, "duration_secs": duration,
            "shortfall_secs": shortfall, "limit_secs": limit,
            "broadcast_ms": started, "median_ms": median_ms}


def _backfill_yt_chat(config: dict, item: dict) -> bool:
    """
    Download the post-hoc chat and merge it into the live capture.

    The merge is refused if it would shrink the file, so a bad download
    cannot destroy the partial capture already held.
    """
    nas_path = config["nas_path"]
    if not item["video_id"]:
        print("  ✗ No YouTube ID; cannot backfill.")
        return False

    live = item["chat_path"]
    base = os.path.splitext(item["chat_file"])[0]
    posthoc = os.path.join(nas_path, f"{base}.posthoc.json")
    written = os.path.join(nas_path, f"{base}.posthoc.live_chat.json")

    print(f"\n  ↓ Post-hoc chat: {base}.posthoc.json")
    url = ls_common.build_stream_url(config, "youtube", item["video_id"])
    subprocess.run(ls_common.ytdlp_chat_cmd(
        config, url, f"{base}.posthoc.%(ext)s"), cwd=nas_path)

    if os.path.exists(written):
        os.rename(written, posthoc)
    if not os.path.exists(posthoc):
        print("  ✗ No post-hoc chat produced (replay chat may be disabled).")
        return False

    def _lines(path):
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                return sum(1 for ln in f if ln.strip())
        except OSError:
            return 0

    tmp = live + ".merging"
    r = subprocess.run([sys.executable,
                        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                     "merge_yt_chats.py"),
                        live, posthoc, "-o", tmp])

    if r.returncode == 0 and os.path.exists(tmp) and _lines(tmp) >= _lines(live):
        print(f"  merged {_lines(live):,} + {_lines(posthoc):,} "
              f"→ {_lines(tmp):,} lines")
        os.replace(tmp, live)
        os.remove(posthoc)
        print(f"  ✔ {item['chat_file']}")
        return True

    if os.path.exists(tmp):
        os.remove(tmp)
    print("  ✗ Merge failed or would shrink the file; both kept for manual merge.")
    return False


def _backfill_tw_chat(config: dict, item: dict) -> bool:
    """
    Pull the VOD's chat down BESIDE the capture already held.

    Beside, not over. What a live IRC capture has and a VOD download cannot —
    the messages that were deleted after being seen, and the record that they
    were — is exactly what replacing it would throw away: Twitch's GQL keeps
    the comments that survived, not the history of what was removed. So the
    pull lands as a second capture and `--merge-chat` unions them, keeping
    every message either source saw and the moderation history from the one
    that has it.

    Downloaded to a hidden temp first. The capture beside it may itself be an
    earlier pull, and a failed download that truncated it in place would cost
    more than it could possibly recover.
    """
    nas_path = config["nas_path"]
    if not item.get("video_id"):
        print("  ✗ No Twitch id on the capture; cannot pull.")
        return False

    tdl = config.get("twitch_downloader_cli")
    if not (tdl and os.path.exists(tdl)):
        print("  ✗ twitch_downloader_cli is not configured; cannot pull.")
        return False

    base = os.path.splitext(item["chat_file"])[0]
    # The capture may itself be a pull from an earlier attempt. One tag is
    # enough; two would read as a pull of a pull.
    if base.endswith(ls_common.OFFLINE_PULL_TAG):
        base = base[: -len(ls_common.OFFLINE_PULL_TAG)]
    out = os.path.join(nas_path, ls_common.offline_pull_name(base))
    tmp = os.path.join(nas_path, f".{base}.pull.tmp")

    print(f"\n  ↓ VOD chat: {os.path.basename(out)}")
    subprocess.run([tdl, "chatdownload", "--id", str(item["video_id"]),
                    "-o", tmp])

    if not os.path.exists(tmp) or os.path.getsize(tmp) == 0:
        if os.path.exists(tmp):
            os.remove(tmp)
        print("  ✗ Nothing came back. A broadcast id cannot be downloaded "
              "from —\n    check `ls-audit --tw-ids` if this id starts with 3.")
        return False

    try:
        got = ls_chat.convert_file(tmp)
    except (OSError, ValueError, json.JSONDecodeError) as e:
        os.remove(tmp)
        print(f"  ✗ The pull does not parse: {e}")
        return False
    if not got.messages:
        os.remove(tmp)
        print("  ✗ The pull has no messages in it; keeping what is held.")
        return False

    os.replace(tmp, out)
    print(f"  ✔ {os.path.basename(out)} — {len(got.messages):,} messages")
    print("    The merge will union it with the capture already held.")
    return True


# ── giving up on a capture ────────────────────────────────────────────────
#
# Some captures cannot be fixed. The VOD is years gone, there is no YouTube
# side, the file is half a crash. The checks above will keep finding them true
# every sweep for the rest of time, and the clock prompt will keep refusing to
# merge without an answer nobody has — so an unfixable capture stops being a
# gap in the archive and becomes a machine that prints.
#
# So there is a way to say "this one is lost, stop asking". It is by hand and
# it is per entry and platform, because a blanket rule would swallow the ones
# that are merely awkward along with the ones that are hopeless.
#
# Beside the cache, in the same shape as `.archive_pending_ids.json`: small,
# local, greppable, and editable with a text editor when a decision turns out
# to be wrong.
GIVEUP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           ".chat_giveup.json")


def _load_giveup() -> dict:
    try:
        with open(GIVEUP_PATH, encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def chat_given_up(index: int, platform: str) -> dict | None:
    """The note left when this entry's chat on this platform was let go."""
    return _load_giveup().get(f"{int(index)}:{platform}")


def cmd_give_up_chat(index: int, platform: str, why: str = "") -> None:
    """Record that this entry's chat on this platform is not coming back."""
    if platform not in ("twitch", "youtube"):
        print(f"  ✗ unknown platform '{platform}' (want twitch or youtube)")
        return
    data = _load_giveup()
    key = f"{int(index)}:{platform}"
    if key in data:
        print(f"  Already let go on {data[key].get('at', '?')}"
              + (f" — {data[key]['why']}" if data[key].get("why") else ""))
        return
    data[key] = {"at": datetime.datetime.now().isoformat(timespec="seconds"),
                 "why": why or "unrecoverable"}
    tmp = GIVEUP_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=1, sort_keys=True)
    os.replace(tmp, GIVEUP_PATH)
    print(f"  ✔ #{index} {platform} chat let go. It will not be offered or "
          f"waited for again.")
    print(f"    Undo by deleting the entry from "
          f"{os.path.basename(GIVEUP_PATH)}.")


#  What each finding is called when a person has to read it. Ordered worst
#  first, because a capture can be several of these at once and the first line
#  should be the one that matters.
_SHORTFALL_SAYS = {
    "unreadable": "the capture does not parse as any chat format we know",
    "empty":     "the capture is there but has no messages in it",
    "no clock":  "no message in it carries an absolute time, and the cache "
                 "has no start for this id either",
    "misplaced": "its own clock sits outside the broadcast — this may be "
                 "another stream's capture",
    "short":     "the chat stops well before the video does",
}


def _offer_chat_backfill(config: dict, item: dict,
                         interactive: bool = True) -> bool:
    plat = item.get("platform", "youtube")
    label = "YouTube" if plat == "youtube" else "Twitch"
    why = item.get("why") or ["short"]

    print(f"\n  ⚠ {label} chat wants a second look:")
    for w in [k for k in _SHORTFALL_SAYS if k in why]:
        print(f"      • {_SHORTFALL_SAYS[w]}")
    if item.get("count"):
        line = f"      {item['count']:,} messages"
        if item.get("placed") is not None and item["placed"] != item["count"]:
            line += f" ({item['placed']:,} of them placeable in time)"
        if item.get("last_secs") is not None and item.get("duration_secs"):
            line += (f", ending at {_seconds_to_hhmmss(item['last_secs'])}"
                     f" of {_seconds_to_hhmmss(item['duration_secs'])}")
        print(line)
    if item.get("shortfall_secs") and item.get("limit_secs"):
        print(f"      short by {_seconds_to_hhmmss(item['shortfall_secs'])} "
              f"(flags above {_seconds_to_hhmmss(item['limit_secs'])})")
    if "misplaced" in why and item.get("median_ms") and item.get("broadcast_ms"):
        print(f"      messages centre on {_clock(item['median_ms'])}, "
              f"broadcast began {_clock(item['broadcast_ms'])}")

    run = _backfill_yt_chat if plat == "youtube" else _backfill_tw_chat
    ask = ("Download post-hoc chat and merge?" if plat == "youtube"
           else "Pull the VOD's chat beside it?")
    #  Unattended, this does not download. That is the rule step 6 of `audit()`
    #  already states one screen up about a missing VOD — "never pull one
    #  unattended" — and this branch was the only thing in the file breaking
    #  it. The size difference is not the distinction: a caller with nobody
    #  watching may be a timer OR a website button, and a button that quietly
    #  fetches from YouTube and Twitch is bandwidth spent by nobody.
    #
    #  Declining is not the end of the repair. The shortfall was just printed
    #  in full, and `_pipeline` collects it for whoever asked, so it can be
    #  offered somewhere a person can say yes.
    if not interactive:
        print(f"    unattended — not downloading. To repair: {ask.lower()} "
              "run ls-audit on this entry from a terminal.")
        return False
    if input(f"\n  {ask} [y/N]: ").strip().lower() not in ("y", "yes"):
        print("  Skipped.")
        return False
    return run(config, item)


# ═══════════════════════════════════════════════════════════════════════════
#  MERGE CHAT
# ═══════════════════════════════════════════════════════════════════════════

def _cache_zeros(cache: list[dict], yt_id: str | None,
                 tw_id: str | None) -> dict:
    """
    Known zeros for captures that carry none, from the recording cache.

    Twitch IRC offsets are relative to when the recorder attached, YouTube's
    videoOffsetTimeMsec to the broadcast start. TDC dumps and post-tmi_sent_ts
    captures have their own zero, so these are only ever a fallback.
    """
    out = {}
    if yt_id:
        vod = ls_common.find_vod(cache, yt_id, "youtube") or {}
        if vod.get("stream_start_epoch_ms"):
            out["youtube"] = vod["stream_start_epoch_ms"]
    if tw_id:
        vod = ls_common.find_vod(cache, tw_id, "twitch") or {}
        if vod.get("record_start_epoch_ms"):
            out["twitch"] = vod["record_start_epoch_ms"]
    return out


# ── the clock a capture does not carry ────────────────────────────────────
#
# `merge` places every message by its absolute time and nothing else. A source
# that has none contributes nothing: its messages are counted as `unplaced` and
# omitted. That used to happen quietly, after which the merged file existed —
# and `_pipeline` treats an entry with a merged file as finished, so the
# omission was permanent and invisible.
#
# Every Twitch capture from before the recorder wrote `tmi_sent_ts` is in this
# state, which is most of the back catalogue. So the answer cannot be to refuse
# them: it has to be possible to say what the clock was.

#  What the zero MEANS, which differs by platform and is the thing somebody
#  supplying one has to get right.
_ZERO_MEANS = {
    "twitch": ("the moment the recorder attached to chat — a Twitch capture's "
               "offsets\n      are measured from there, not from the "
               "broadcast start"),
    "youtube": ("the broadcast start — videoOffsetTimeMsec is measured from "
                "there"),
}

#  Where to go and look for it, in the order they are worth trying.
_ZERO_WHERE = (
    "the vault entry's own start time, if the entry has one",
    "the recording cache: `record_start_epoch_ms` on the Twitch row, "
    "`stream_start_epoch_ms` on the YouTube one",
    "the video file's own start — `ls-audit N --timings` prints it",
)


def _sources_without_zero(res: dict) -> list[dict]:
    """Sources that carried messages and no clock to place them by."""
    return [s for s in res["metadata"]["sources"]
            if not s.get("zero_ms") and s.get("messages")]


def _source_platforms(paths: list[str]) -> dict:
    """platform -> filenames, from a bounded probe rather than a parse.

    Needed for the case where the merge REFUSED outright: nothing has an
    absolute reference, so there is no metadata to read the platforms off.
    """
    out: dict[str, list[str]] = {}
    for p in paths:
        cls = ls_chat.CONVERTERS.get(ls_chat.detect_format(p))
        if cls:
            out.setdefault(cls.platform, []).append(os.path.basename(p))
    return out


def _explain_missing_zero(groups: dict, fallback: dict) -> None:
    """Say what is wrong, what it costs, and what would fix it."""
    print("\n  ⚠ A capture has no clock, so its messages cannot be placed.")
    for plat in sorted(groups):
        files, dropped = groups[plat]
        print(f"\n      {plat}: " + ", ".join(files))
        if dropped:
            print(f"      {dropped:,} messages would be dropped from the "
                  f"merge and lost from the archive's view of this stream.")
        print(f"      Its zero is {_ZERO_MEANS.get(plat, 'the reference')}.")
        if fallback.get(plat):
            print(f"      The cache offers {fallback[plat]} "
                  f"({_clock(fallback[plat])}) — already tried, and the "
                  f"capture still has none.")
    print("\n      Accepted: epoch ms, epoch seconds, an ISO datetime "
          "(local unless\n      it carries an offset), or +/-seconds from "
          "the merge reference.")
    print("      Where to find it:")
    for w in _ZERO_WHERE:
        print(f"        · {w}")


def _ask_for_zeros(groups: dict) -> dict:
    """Prompt per platform. Empty input skips that one; bad input re-asks."""
    given: dict[str, str] = {}
    for plat in sorted(groups):
        while True:
            raw = input(f"\n  Zero for {plat} (Enter to skip): ").strip()
            if not raw:
                break
            try:
                ms = ls_chat.parse_zero(raw) if raw[0] not in "+-" else None
            except ValueError as e:
                print(f"    ✗ {e}")
                continue
            if ms is not None:
                print(f"    → {_clock(ms)} on "
                      f"{datetime.datetime.fromtimestamp(ms / 1000):%Y-%m-%d}")
            given[plat] = raw
            break
    return given


MERGED_SUFFIXES = (".json.gz", ".json")


def merged_chat_name(index: int) -> str:
    return f"{int(index):03d}_merged-chat.json"


def find_merged_chat(nas_root: str, index: int) -> str | None:
    """The merged chat for this entry, compressed for preference.

    Both spellings exist in the wild: everything merged from now on is written
    compressed and only compressed, and everything merged before that is a
    plain .json sitting on the NAS. Neither is wrong and the archive reads
    both, so this looks for the good one and settles for the old one.
    """
    base = merged_chat_name(index)
    for name in (base + ".gz", base):
        if os.path.exists(os.path.join(nas_root, name)):
            return name
    return None


def _write_merged(path: str, res: dict) -> tuple[str, float] | None:
    """Write the merged chat, compressed, and return (path, MB).

    Compressed and NOT also plain. The archive serves this file and nothing in
    its stack compresses — express with one dependency and no proxy of its own
    — and a merged chat is the most compressible thing in the whole archive: a
    megabyte of repeated names and repeated words, about an eighth of that
    gzipped. Keeping the plain copy as well would be eight times the bytes on
    a 21 TB NAS for a file nothing reads, and a second thing to keep in step.

    Doing it here follows the rule the rest of the media follows: the recorder
    writes under the media root, the container only ever reads.

    Written to a .part and renamed, because rename is atomic and a half-written
    file served as a complete one is a truncated parse in somebody's browser
    with nothing on either side saying why.
    """
    dest = path if path.endswith(".gz") else path + ".gz"
    tmp = dest + ".part"
    try:
        with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=9) as f:
            json.dump(res, f, ensure_ascii=False, indent=1)
        os.replace(tmp, dest)
    except OSError as e:
        print(f"  ✗ could not write {os.path.basename(dest)} ({e})")
        try:
            os.remove(tmp)
        except OSError:
            pass
        return None
    # The plain twin from a previous merge is now stale — it describes an
    # earlier merge of the same entry — and a stale chat served as the current
    # one is worse than no chat. Only ever the twin of what was just written.
    plain = dest[:-3]
    if plain != dest and os.path.exists(plain):
        try:
            os.remove(plain)
            print(f"  · removed {os.path.basename(plain)}, "
                  f"superseded by the compressed one")
        except OSError as e:
            print(f"  ⚠ {os.path.basename(plain)} is stale and would not delete ({e})")
    return dest, os.path.getsize(dest) / (1024 * 1024)


def _write_gz(path: str) -> float | None:
    """Compress an existing merged chat that was written plain. Kept for the
    entries that already are."""
    tmp, dest = path + ".gz.part", path + ".gz"
    # Dropped FIRST, not overwritten. This only ever runs straight after the
    # merge rewrote the json, so any .gz already here describes the previous
    # merge — and a failure below that left it in place would have the archive
    # serving last week's chat to anyone whose browser asked for gzip, with
    # the correct file sitting right beside it. No .gz is a slower page; a
    # stale one is a wrong one.
    try:
        os.remove(dest)
    except OSError:
        pass
    try:
        with open(path, "rb") as src, gzip.open(tmp, "wb", compresslevel=9) as dst:
            shutil.copyfileobj(src, dst, 1024 * 1024)
        os.replace(tmp, dest)
        return os.path.getsize(dest) / (1024 * 1024)
    except OSError as e:
        print(f"  ⚠ could not write {os.path.basename(path)}.gz ({e})")
        for leftover in (tmp, dest):
            try:
                os.remove(leftover)
            except OSError:
                pass
        # Only reachable if the filesystem refused the delete as well, which is
        # the one state worth shouting about: the archive would serve this to
        # every browser that asks for gzip, and it is not the file beside it.
        if os.path.exists(dest):
            print(f"  ✗ {os.path.basename(dest)} is STALE and could not be removed — "
                  f"delete it by hand before the archive serves it")
        return None


def _archive_raw_chats(config: dict, sources: list[str], res: dict) -> set[str]:
    """
    Move raw captures to deep storage once the merge holds them.

    A raw is only moved if its platform actually landed messages in the
    output and the source had a real zero -- otherwise the merge dropped
    content and the raw is the only copy of it.

    Returns the platforms whose raw left, so the caller can tell the archive
    to stop pointing at a file that is no longer where it says.
    """
    moved: set[str] = set()
    dest = config.get("chat_archive_path")
    if not dest:
        print("  Raw chats left in place (set chat_archive_path to archive).")
        return moved
    # A relative path would resolve against the caller's CWD and quietly put
    # the archive outside the media root, where scan_nas cannot see it and the
    # server reads every archived chat as lost. join() leaves absolute alone.
    dest = os.path.join(config.get("nas_path", ""), dest)

    placed: dict[str, int] = {}
    for m in res["messages"]:
        placed[m["origin"]] = placed.get(m["origin"], 0) + 1
    by_name = {os.path.basename(p): p for p in sources}

    os.makedirs(dest, exist_ok=True)
    for src in res["metadata"]["sources"]:
        path = by_name.get(src["file"])
        if not path:
            continue
        if src["zero_ms"] is None:
            print(f"  ⚠ {src['file']}: no zero, messages unplaced — keeping")
            continue
        if not placed.get(src["platform"]):
            print(f"  ⚠ {src['file']}: contributed nothing — keeping")
            continue
        target = os.path.join(dest, src["file"])
        # A re-merge now feeds the archived raws back in, so most of them ARE
        # the target. Nothing to move and nothing wrong, so nothing said.
        if os.path.abspath(path) == os.path.abspath(target):
            continue
        if os.path.exists(target):
            print(f"  ⚠ {src['file']}: already in archive — keeping")
            continue
        try:
            shutil.move(path, target)
            moved.add(src["platform"])
            print(f"  → archived {src['file']}")
        except OSError as e:
            print(f"  ✗ archive failed for {src['file']}: {e}")
    return moved


def cmd_merge_chat(config: dict, index: int, ref="youtube",
                   zeros: list | None = None, output: str | None = None,
                   dry_run: bool = False, keep_raw: bool = False,
                   assets: bool = True, interactive: bool = True):
    """Merge this entry's chat captures into one origin-tagged file."""
    nas_root = config.get("nas_path", "")
    print(f"\n{'=' * 60}")
    print(f"  Merging chat for entry #{index}")
    print(f"{'=' * 60}")

    nas = scan_nas(config, index)
    sources = []
    for key, label in (("yt_chats", "YT"), ("tw_chats", "TW")):
        names = nas.get(key) or []
        if not names:
            print(f"  {label} chat : not found")
            continue
        for filename in names:
            path = os.path.join(nas_root, filename)
            if os.path.exists(path):
                print(f"  {label} chat : {filename}")
                sources.append(path)

    # Raws already in deep storage count too, and used not to. The merge is
    # what put them there, so leaving them out is only ever invisible on an
    # entry nobody re-merges -- but a REPAIR is exactly that: an offline pull
    # dropped beside an entry whose original capture was archived months ago.
    # Merging the drop alone and writing it over the good file would trade the
    # whole chat for the piece that was missing.
    #
    # Appended rather than interleaved, so on every entry that already worked
    # the order the duplicates are seen in -- and so which copy's fields win --
    # is exactly what it was.
    arch_dir = config.get("chat_archive_path")
    if arch_dir:
        arch_dir = os.path.join(nas_root, arch_dir)
        for key, label in (("yt_chats_archived", "YT"),
                           ("tw_chats_archived", "TW")):
            for filename in nas.get(key) or []:
                path = os.path.join(arch_dir, filename)
                if os.path.exists(path):
                    print(f"  {label} chat : {filename}  (archived)")
                    sources.append(path)

    if not sources:
        print("\n  No chat files to merge.\n")
        return

    # IDs come straight off the filenames being merged, so the cache lookup
    # is guaranteed to describe these exact captures.
    cache = ls_common.load_cache()
    fallback = _cache_zeros(
        cache,
        ls_common.extract_video_id_from_filename(nas["yt_chat"]) if nas.get("yt_chat") else None,
        ls_common.extract_video_id_from_filename(nas["tw_chat"]) if nas.get("tw_chat") else None)
    if fallback:
        print("  cache zeros: " + ", ".join(
            f"{k}={v}" for k, v in sorted(fallback.items())))
    print()

    try:
        given = ls_chat.parse_zero_args(zeros)
    except ValueError as e:
        print(f"  ✗ {e}\n")
        return

    #  A source with no clock is not a merge that fails, it is a merge that
    #  quietly holds less than it was given — and then the merged file exists,
    #  so `_pipeline` calls the entry finished and the omission sticks. So the
    #  clock gets asked for, and unattended the merge is not written at all.
    res = None
    while True:
        try:
            res = ls_chat.merge(sources, ref=ref, zeros=given,
                                fallback_zeros=fallback)
        except (ValueError, json.JSONDecodeError) as e:
            print(f"  ✗ {e}\n")
            # "no source has an absolute reference" is the same problem as one
            # source missing one, and wants the same question. There is no
            # metadata to read the platforms off, so probe the files.
            groups = {p: (f, None) for p, f in _source_platforms(sources).items()}
            if not groups or not interactive:
                if groups:
                    _explain_missing_zero(groups, fallback)
                    print("\n  Nothing written. Re-run with "
                          "--zero PLATFORM=<epoch|ISO> from the terminal.\n")
                return
            _explain_missing_zero(groups, fallback)
            more = _ask_for_zeros(groups)
            if not more:
                print("  Nothing written.\n")
                return
            given.update(more)
            continue

        orphans = _sources_without_zero(res)
        if not orphans:
            break
        groups = {}
        for s in orphans:
            files, dropped = groups.get(s["platform"], ([], 0))
            groups[s["platform"]] = (files + [s["file"]],
                                     dropped + s["messages"])
        # A platform somebody has already given up on is not asked about
        # again, and it does not hold the rest of the entry hostage: the merge
        # goes ahead without it, which is the whole point of having said so.
        letgo = {p for p in groups if chat_given_up(index, p)}
        for p in sorted(letgo):
            print(f"  ⚠ {p}: {groups[p][1]:,} messages left out — this chat "
                  f"was let go ({os.path.basename(GIVEUP_PATH)})")
        if letgo == set(groups):
            break
        for p in letgo:
            groups.pop(p)

        _explain_missing_zero(groups, fallback)
        if not interactive:
            print("\n  Nothing written. Re-run with "
                  "--zero PLATFORM=<epoch|ISO> from the terminal,\n"
                  "  or let this capture go: ls-audit "
                  f"{index} --give-up-chat {sorted(groups)[0]}\n")
            return
        more = _ask_for_zeros(groups)
        if more:
            given.update(more)
            continue
        if input("\n  Write the merge without them anyway? [y/N]: ") \
                .strip().lower() in ("y", "yes"):
            break
        print("  Nothing written.\n")
        return

    md = res["metadata"]
    for src in md["sources"]:
        when = (datetime.datetime.fromtimestamp(src["zero_ms"] / 1000)
                .strftime("%Y-%m-%d %H:%M:%S") if src["zero_ms"] else "UNKNOWN")
        print(f"  {src['platform']:<8} {src['messages']:>7,}  "
              f"zero {when} ({src['zero_source']})")
    print(f"  merged   {md['messages']:,} from "
          f"{datetime.datetime.fromtimestamp(md['zero_epoch_ms'] / 1000):%Y-%m-%d %H:%M:%S}")
    if md["duplicates_removed"]:
        print(f"  dupes    {md['duplicates_removed']:,}")
    if md["unplaced_no_abs"]:
        print(f"  ⚠ {md['unplaced_no_abs']:,} messages had no absolute time and "
              f"were omitted.\n    Supply a zero with --zero PLATFORM=<epoch|ISO|+secs>")

    if dry_run:
        print("\n  --dry-run: nothing written.\n")
        return

    if not output:
        output = os.path.join(nas_root, f"{int(index):03d}_merged-chat.json")
    wrote = _write_merged(output, res)
    if wrote is None:
        return
    output, size = wrote
    print(f"\n  ✔ {os.path.basename(output)}  ({size:.1f}MB)")

    # The pictures this file only names. Straight from the metadata in hand
    # rather than by re-reading what was just written, and after the file is on
    # disk rather than before: a failure here is a slower-looking chat, and a
    # merge that did not get written is a lost one.
    if assets:
        ls_assets.harvest(config, res["metadata"])

    if keep_raw:
        print("  Raw chats kept (--keep-raw).\n")
        return {"merged": output, "moved": set()}
    moved = _archive_raw_chats(config, sources, res)
    print()
    return {"merged": output, "moved": moved}


# ═══════════════════════════════════════════════════════════════════════════
#  POST-STREAM PIPELINE
# ═══════════════════════════════════════════════════════════════════════════

def _recent_indices(config: dict, n: int = 5) -> list[int]:
    """Highest n entry indices present on the NAS."""
    nas = config.get("nas_path", "")
    if not os.path.exists(nas):
        return []
    idxs = set()
    for name in os.listdir(nas):
        m = re.match(r"^(\d+)_", name)
        if m:
            idxs.add(int(m.group(1)))
    return sorted(idxs)[-n:]


def _deferred_repair(item: dict, platform: str) -> dict:
    """A repair that was not run because nobody was there to say yes.

    Not a finding and not a question: a finding says what is true and a
    question asks what nobody can see, and this is neither — it is work the
    entry is waiting on, which somebody has to authorise because it spends
    somebody else's bandwidth. It travels as its own shape so the caller can
    decide what to do with it; `finish()` is the only thing that collects one.
    """
    return {"kind": "chat_backfill", "platform": platform,
            "why": list(item.get("why") or []),
            "count": item.get("count"),
            "shortfall_secs": item.get("shortfall_secs")}


def _late_captures(config: dict, nas_root: str, merged_name: str,
                   nas: dict) -> list[str]:
    """Raws on the NAS that the merged file does not name as a source.

    The merge is written once and then `_pipeline` calls the chat side
    finished, which was right for every capture that exists before the merge
    and wrong for the one shape that arrives after it: a PULL. An offline chat
    dropped beside an entry merged months ago would land, be skipped by the
    very next sweep, and sit on the NAS forever while the entry reported a
    merged chat that did not contain it.

    `cmd_merge_chat` has known how to do this properly since archived raws
    became merge sources -- it reads deep storage and the NAS root together
    and unions the lot. Nothing ever called it a second time.

    Read off the merged file's own header, which names every source that went
    into it and costs a few kilobytes to read. Two ways of saying "do not":

      no header, or a header naming no sources -- a v1 file, or one this
      cannot parse. Not knowing what is in it is not a reason to rewrite it.

      a source the merge names that is no longer findable, on the NAS or in
      deep storage. A re-merge REPLACES the merged file from its sources, so
      one that has gone would be dropped out of it, and the merged copy is
      the only place it still exists. Better a stale merge than a shorter one.
    """
    meta = ls_chat.read_header(os.path.join(nas_root, merged_name),
                               MERGED_CHAT_HEAD_BYTES)
    named = {str(src.get("file") or "")
             for src in ((meta or {}).get("sources") or [])}
    named.discard("")
    if not named:
        return []

    arch = config.get("chat_archive_path")
    arch_dir = os.path.join(nas_root, arch) if arch else None
    here = set()
    for k in ("yt_chats", "tw_chats"):
        here |= {n for n in (nas.get(k) or [])
                 if os.path.exists(os.path.join(nas_root, n))}
    stored = set()
    if arch_dir:
        for k in ("yt_chats_archived", "tw_chats_archived"):
            stored |= {n for n in (nas.get(k) or [])
                       if os.path.exists(os.path.join(arch_dir, n))}
    lost = named - here - stored
    if lost:
        print(f"    {len(lost)} merged source(s) are no longer on disk "
              f"— not re-merging: " + ", ".join(sorted(lost)[:3]))
        return []

    return sorted(here - named)


def _pipeline(config: dict, cache: list[dict], index: int,
              interactive: bool = True,
              deferred: list | None = None) -> bool:
    """
    Bring one entry to its finished shape: meta sidecar, merged chat, raws
    archived.

    Order is not negotiable: the meta sidecar derives exact zeros by reading
    the raw captures, so it must be written while they are still in place.

    Anything not ready yet is left alone and picked up by a later run, so
    this is safe to call repeatedly.

    `deferred`, when a list is passed, collects the repairs this run would
    have offered and did not run because it is unattended. An out-parameter
    rather than a second return value because every existing caller wants the
    bool and only one caller wants this — changing the return type would make
    three call sites learn about a case two of them cannot reach.
    """
    nas_root = config.get("nas_path", "")
    nas = scan_nas(config, index)
    # THROUGH `find_merged_chat`, which knows both spellings. This line used to
    # build the plain `.json` name by hand, and everything merged since the
    # merge started compressing is `.json.gz` — so the "already merged" check
    # below missed every recent entry. On #736 it fell through to "no chats
    # present" about an entry that HAS a merged chat, and with
    # `chat_archive_path` unset — raws left in place rather than moved — it
    # would have re-merged an already-merged entry on every single run.
    merged_name = find_merged_chat(nas_root, index)
    changed = False

    # 1. Meta sidecar, ensured first and independently of the merge: it reads
    #    the raw captures for exact zeros, and an entry merged before this
    #    step existed would otherwise never get one.
    # Not "only when absent" any more. Write-once was right for getting the
    # first sidecar onto every entry and wrong forever after: one written while
    # the cache held a bad start, or before a chat backfill landed, could never
    # be corrected — and C's whole design reads these back. `cmd_timings` now
    # decides for itself, leaving a recorder-written sidecar alone and
    # rewriting its own only when the content actually changed, so this can
    # simply ask every sweep and still report honestly.
    changed = bool(cmd_timings(config, index))

    if merged_name:
        late = _late_captures(config, nas_root, merged_name, nas)
        if not late:
            return changed                 # chat side already finished
        #  A capture that arrived AFTER the merge, which is what a pull is.
        #  Falling through re-runs the whole chat side, and `cmd_merge_chat`
        #  writes over the merged file atomically from every source including
        #  the ones in deep storage -- so this widens the merge, it does not
        #  replace it with the drop.
        print(f"    {len(late)} capture(s) arrived after the merge "
              f"— re-merging: " + ", ".join(late))

    chats = [k for k in ("yt_chats", "tw_chats") if nas.get(k)]
    if not chats:
        print("    no chats present — nothing to merge")
        return changed

    # Everything here has been let go, so there is nothing left to try and
    # nothing to say about it beyond one line. Without this an entry whose
    # chat can never be placed re-runs the whole merge, and re-prints the
    # whole clock prompt, on every sweep for the rest of time.
    _plat = {"yt_chats": "youtube", "tw_chats": "twitch"}
    if all(chat_given_up(index, _plat[k]) for k in chats):
        print("    chat let go on every platform here — nothing to merge")
        return changed

    # A truncated YouTube chat blocks the merge: merging now would archive
    # the raws with hours of chat still missing.
    yt_id = (ls_common.extract_video_id_from_filename(nas["yt_chat"])
             if nas.get("yt_chat") else None)
    short = _yt_chat_shortfall(config, cache, nas, yt_id)
    if short:
        if not _offer_chat_backfill(config, short, interactive=interactive):
            # Two different reasons to be standing here, and they are not the
            # same news. Interactively it means the replay chat could not be
            # had — a retry is the answer. Unattended it means nobody was
            # asked, which is a repair waiting on a person rather than on the
            # platform, and saying "will retry" about it would promise a retry
            # that is going to decline again every single time.
            if interactive:
                print("    repair unavailable (replay chat not ready?) — will retry")
            elif deferred is not None:
                deferred.append(_deferred_repair(short, "youtube"))
            return changed
        nas = scan_nas(config, index)
        if _yt_chat_shortfall(config, cache, nas, yt_id):
            print("    still short after repair — will retry")
            return changed

    # Twitch does not block, and the asymmetry is deliberate. A YouTube repair
    # merges INTO the live file, so re-checking the shortfall is a meaningful
    # test of whether it worked. A Twitch pull lands beside the capture, so the
    # capture is exactly as short as it was and re-checking would always say so
    # — the union happens in the merge, not here. And a pull that is declined
    # or unavailable is no reason to hold the merge back: archived raws are
    # merge sources now, so a later pull re-merges to the full thing.
    tw_id = (ls_common.extract_video_id_from_filename(nas["tw_chat"])
             if nas.get("tw_chat") else None)
    tw_short = _tw_chat_shortfall(config, cache, nas, tw_id, index)
    if tw_short:
        if _offer_chat_backfill(config, tw_short, interactive=interactive):
            nas = scan_nas(config, index)  # the pull is a second capture now
        elif not interactive and deferred is not None:
            # Deferred only when nobody was asked. A person who said no said
            # no, and putting that on a list for somebody to be asked again is
            # how a queue fills with decisions already made.
            deferred.append(_deferred_repair(tw_short, "twitch"))

    # `True` only if a merge was actually written. The meta-sidecar step above
    # already learned this lesson -- "only count it if a file actually
    # appeared" -- and returning True unconditionally here made every sweep
    # report work it had not done. It matters beyond the tidiness: `audit()`
    # decides whether to rewrite the vault entry on this answer, and an
    # unattended sweep over an entry whose chat can never be placed would
    # otherwise claim progress on it forever.
    return bool(cmd_merge_chat(config, index, interactive=interactive))


def cmd_tw_ids(config: dict, apply_entries: bool = False):
    """Which stored Twitch ids are broadcast ids, and what VOD each became.

    Read-only by default, because the archive is the other half of this and
    nothing here can reach it: the capture rows are keyed on the id they were
    written with, and ls-archive refuses to repoint a capture from a sweep on
    purpose. So this prints the mapping and lets a person carry it across.

    `--apply` rewrites the Obsidian entries only, which is the half that is
    safely rewritable — the entry is a derived document and ls-audit rebuilds
    it anyway.
    """
    cache = ls_common.load_cache()
    print("\n  ⌛ Refreshing twitch cache...")
    if not ls_common.refresh_twitch_cache(config, cache, full=True):
        print("  ✗ Could not reach Helix. Nothing was checked.")
        return
    ls_common.save_cache(cache)
    known = sum(1 for v in cache
                if v.get("platform") == "twitch" and v.get("stream_id"))
    total = sum(1 for v in cache if v.get("platform") == "twitch")
    print(f"  ✔ {total} VODs cached, {known} with a broadcast id\n")
    if not known:
        print("  ⚠ No VOD carries a stream_id. Twitch keeps these for a while,\n"
              "    not forever — anything older than the window it serves can\n"
              "    only be matched by date, which this deliberately will not do.\n")

    top = ls_common.obsidian_next_index(config) - 1
    rows, unknown = [], []
    for idx in range(1, top + 1):
        entry = ls_common.obsidian_parse_entry(config, idx)
        if not entry.get("found") or not entry.get("tw_id"):
            continue
        stored = entry["tw_id"]
        fixed, corrected = ls_common.twitch_correct_id(cache, stored)
        if corrected:
            vod = ls_common.find_vod(cache, fixed, "twitch") or {}
            rows.append((idx, stored, fixed, vod.get("title") or "",
                         (vod.get("start_time") or "")[:10]))
        elif not ls_common.find_vod(cache, stored, "twitch"):
            unknown.append((idx, stored, entry.get("date_str") or ""))

    print(f"{'=' * 78}")
    print("  Twitch ids that are broadcast ids, not videos")
    print(f"{'=' * 78}")
    if not rows:
        print("  ✔ none — every entry holds a real VOD id.")
    for idx, stored, fixed, title, date in rows:
        print(f"  #{idx:03d}  {stored:>13}  →  {fixed:>11}   {date}  {title[:34]}")
    print(f"\n  {len(rows)} to correct.")
    if rows:
        print("  In the archive, for each: set the TW capture's remote_id to the\n"
              "  right-hand id and its url to https://www.twitch.tv/videos/<id>.")
    if unknown:
        print(f"\n  {len(unknown)} id{'' if len(unknown) == 1 else 's'} neither "
              "a cached VOD nor a cached broadcast —")
        print("  too old for the window Helix serves, or the VOD is gone:")
        for idx, stored, date in unknown[:40]:
            print(f"  #{idx:03d}  {stored:>13}   {date}")
        if len(unknown) > 40:
            print(f"  … and {len(unknown) - 40} more")
    print()

    if apply_entries and rows:
        print("  Rewriting the Obsidian entries…")
        done = 0
        for idx, stored, fixed, _t, _d in rows:
            if _rewrite_entry_tw_id(config, idx, stored, fixed):
                done += 1
        print(f"  ✔ {done} entr{'y' if done == 1 else 'ies'} rewritten. "
              "The archive is untouched.\n")


def _rewrite_entry_tw_id(config: dict, index: int,
                         old_id: str, new_id: str) -> bool:
    """Swap one id inside one entry's TW line, leaving everything else alone.

    A targeted substitution rather than a rebuild: the entry holds notes and
    hand edits, and this is a correction to one number in it, not a reason to
    regenerate the whole block.
    """
    path = config["obsidian"]
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return False
    span = ls_common._find_entry_block(lines, index)
    if not span:
        return False
    start, end = span
    hit = False
    for i in range(start, end):
        if not re.match(r"^\t`TW`", lines[i]):
            continue
        # Any of the three spellings in, the canonical one out.
        def _sub(m):
            return f"https://www.twitch.tv/videos/{new_id}"
        fixed = re.sub(rf"https?://(?:www\.)?twitch\.tv/(?:[^/)\s]+/)?"
                       rf"videos?/v?{re.escape(str(old_id))}", _sub, lines[i])
        if fixed != lines[i]:
            lines[i] = fixed
            hit = True
    if not hit:
        return False
    ls_common._write_lines_atomic(path, lines)
    return True


def cmd_sweep(config: dict, count: int = 5, interactive: bool = False):
    """Run the full audit over the most recent entries. Safe to run hourly."""
    idxs = _recent_indices(config, count)
    print(f"\n{'=' * 60}")
    print(f"  Sweep: {len(idxs)} most recent entries — "
          f"{', '.join(str(i) for i in idxs) or 'none'}")
    print(f"{'=' * 60}")
    for idx in idxs:
        try:
            audit(config, idx, interactive=interactive)
        except Exception as e:
            print(f"  ✗ #{idx:03d} {type(e).__name__}: {e}")
    print()


# ═══════════════════════════════════════════════════════════════════════════
#  THE CORE — what an audit KNOWS, with nothing printed and nothing asked
# ═══════════════════════════════════════════════════════════════════════════
#
# This used to be the top half of `audit()`, interleaved with prints. Pulling
# it out is what lets the same three checks answer a terminal, a job report and
# a test without three copies of them — and it is what makes the tool a
# VERIFIER rather than a builder. Rebuilding the Obsidian line was never the
# point; it was the cheapest way to deal with a markdown file whose formatting
# could not be trusted, and the block below is now one output among several
# rather than the purpose.
#
# Three checks, and only the first one is about this machine:
#
#   disk        is the file here, and is it the one the record names
#   platform    does the id name something that exists, and does it still
#   assignment  is this capture attached to the RIGHT broadcast
#
# The third is the one nothing has ever asked, and it is the one that bites:
# a Twitch capture landing on the wrong stream's row is invisible to every
# other check, because every file is present and every link resolves.

# level     meaning
# 'ok'      checked, and it is fine — kept so a quiet run can say what it read
# 'note'    true and worth knowing, not a problem (a copy declined on purpose)
# 'warn'    probably wrong, and a person should look
# 'bad'     wrong, or unanswerable when it should be answerable
LEVELS = ("ok", "note", "warn", "bad")
_RANK = {lvl: i for i, lvl in enumerate(LEVELS)}

# How far a capture's own start may sit from the entry's before the assignment
# is in doubt. Generous on purpose: a vault date is typed to the minute by a
# person, and the two platforms genuinely start minutes apart. Past an hour,
# though, they are not the same broadcast.
ASSIGN_WINDOW_S = 3600


def _finding(level: str, check: str, message: str, *,
             platform: str | None = None, short: str | None = None,
             **detail) -> dict:
    """One thing that was checked.

    `message` is the sentence, `short` the two or three words that go on the
    per-platform summary line. Both, rather than one derived from the other:
    the summary has to stay short enough to sit four-across, and a problem has
    to stay long enough to say what to do about it — "could not ask Twitch:
    twitch_user_id is not set" is the whole value of that finding and there is
    no shortening of it that keeps that.
    """
    # A `question=None` is how a caller says "no question here" when whether
    # there is one is conditional. Dropped rather than carried, so `detail`
    # never holds a key whose value means the key is not there -- a panel
    # drawing answer buttons off `detail.question` would draw an empty row.
    if "question" in detail and detail["question"] is None:
        del detail["question"]
    return {"level": level, "check": check, "platform": platform,
            "message": message, "short": short or message, "detail": detail}


#  What the archive has been told that no file can show.
#
#  The vault has always carried two of these — `.×` for "I did not keep this"
#  and `no_yt`/`no_tw` for "there was no stream here" — and they are the only
#  reason `_disk_findings` can tell a decision from a loss. The vault is
#  retiring, and the archive now holds the same two facts as `claim` rows, so
#  this is how they reach the checks that already know what to do with them.
#
#  Keyed on (platform, assertion) with the platform lowercased and `None` kept
#  as itself: a claim about the broadcast as a whole carries no platform, and
#  collapsing that to a string would make it match one.

_PREFIX_PLATFORM = {"yt": "youtube", "tw": "twitch"}


def _claims_index(claims: list[dict] | None) -> dict:
    """(platform, assertion) -> the claim, newest first wins.

    The archive hands them back newest-first already, so the first of each key
    is the answer and the rest are the history behind it.
    """
    out: dict = {}
    for c in claims or []:
        if c.get("withdrawn_at"):
            continue
        # Lowercased, and an absent platform kept as `None` rather than
        # becoming the string "none". The archive writes them lowercase today,
        # so the casing only matters the first time a row arrives from
        # somewhere else — and it would show up as a lookup that silently
        # misses and a question asked forever.
        key = ((c.get("platform") or "").lower() or None, c.get("assertion"))
        out.setdefault(key, c)
    return out


def _asked(kind: str, prefix: str | None, message: str, answers: list[str]) -> dict:
    """A question the evidence cannot settle, and what would settle it.

    Carried on the finding that raised it rather than as a level of its own:
    an unanswered question is not more or less severe than a missing file, it
    is the same fact with somewhere to go. Adding it to LEVELS would have made
    `worst()` claim a severity nobody meant.
    """
    return {"kind": kind, "platform": _PREFIX_PLATFORM.get(prefix or ""),
            "message": message, "answers": answers}


def _disk_findings(config: dict, nas: dict, entry: dict,
                   claims: dict | None = None,
                   ids: dict | None = None) -> list[dict]:
    """Is the file here, and if not, is that a decision or a loss?

    `ids` is the resolution's {platform: (id, source)}, and all it decides is
    whether `fetch` is among the answers offered. A question that offers to go
    and get something must know there is somewhere to get it FROM: the only
    answers a missing file used to have were "there was no broadcast" and "I
    did not keep it", and on an entry whose VOD link is sitting right there
    both of them are false.
    """
    out = []
    claims = claims or {}
    ids = ids or {}
    # A claim carrying NO platform is about the broadcast as a whole, so it
    # answers for both. "There was no stream that day" and "I kept none of it"
    # are things somebody says once rather than twice, and the archive has
    # always accepted them — this is where they finally mean something. Without
    # it they were stored, served to this worker, and silently ignored.
    none_at_all = (None, "no_broadcast") in claims
    all_declined = (None, "declined") in claims
    for prefix, label in (("yt", "YouTube"), ("tw", "Twitch")):
        platform = _PREFIX_PLATFORM[prefix]
        # The vault's `no_yt` / `no_tw`, or the archive's claim saying the same
        # thing. A platform that never broadcast has nothing to be missing.
        if entry.get(f"no_{prefix}") or none_at_all or (platform, "no_broadcast") in claims:
            continue
        # And the vault's `.×`, or the archive's claim saying the same thing.
        # Checked once here so both the video and the chat branch below can ask
        # the same question of either source.
        declined = all_declined or (platform, "declined") in claims
        #  AND THE ANSWER THAT SAYS WHERE IT IS. `identified` has been offered
        #  since the questions existed, accepted by the archive, stored — and
        #  read by nothing. It appeared exactly once in this whole file: the
        #  line that offered it. So answering it deleted the question, changed
        #  no finding, and the next sweep asked again, for ever.
        #
        #  What it means is not "I have this file". It means the broadcast
        #  happened, this machine never recorded it, and THAT is the video: a
        #  stream watched on one platform and only asked for on the other,
        #  which is the ordinary case when `ls-rec watch` is given one link.
        #  Absent locally and accounted for, which is a note and not a warning.
        located = claims.get((platform, "identified")) or {}
        located_id = (located.get("value") or "").strip() or None

        #  NOTHING AT ALL from this platform, which is one fact and was being
        #  reported as two. A platform nobody recorded has no video AND no
        #  chat, and asking separately about each produced two questions on
        #  one entry whose only honest answers were the same answer — and
        #  neither of them offered the one a person actually has.
        nothing_here = not (nas.get(f"{prefix}_video") or nas.get(f"{prefix}_chat")
                            or _chat_accounted(nas, prefix))
        ask_once = (nothing_here and not declined and not located_id
                    and not entry.get(f"{prefix}_video_x")
                    and not entry.get(f"{prefix}_chat_x"))
        #  Whether there is anywhere to fetch FROM. An id and a platform is
        #  the whole requirement -- the URL is built from them -- and it is
        #  the difference between a question a person can act on and one they
        #  can only file a fact against.
        can_fetch = bool((ids.get(platform) or (None,))[0])
        if ask_once:
            out.append(_finding(
                "warn", "disk", f"nothing from {label} was recorded for this entry",
                platform=prefix, short="not recorded", state="lost",
                question=_asked(
                    "nothing_recorded", prefix,
                    f"Nothing from {label} was recorded for this entry. Was "
                    f"there a broadcast on {label}?",
                    #  Order is the order a person meets them in: the fetches
                    #  first when there is something to fetch, because they
                    #  END the question rather than describing it, then the
                    #  common answer, then the one that needs a link, then the
                    #  one that says a file existed and was let go.
                    #
                    #  TWO of them, never one that gets both. A VOD is hours
                    #  of video and its chat is a few megabytes; wanting the
                    #  second and not the first is the ordinary case, not an
                    #  edge, and a single button spends the difference without
                    #  asking.
                    (["fetch_video", "fetch_chat"] if can_fetch else [])
                    + ["no_broadcast", "identified", "declined"])))
            continue
        # VIDEO. The `.×` in the vault is a person saying "I did not keep
        # this" — both platforms get recorded and one master is usually
        # enough — and until the archive had a word for it, every audit
        # reported it as missing. It is a note, not a warning.
        if nas.get(f"{prefix}_video"):
            out.append(_finding("ok", "disk", f"{label} video on disk",
                                platform=prefix, short="video on disk",
                                file=nas[f"{prefix}_video"]))
        elif entry.get(f"{prefix}_video_x") or declined:
            out.append(_finding("note", "disk", f"{label} video deliberately not kept",
                                platform=prefix, short="video not kept",
                                state="declined"))
        elif located_id:
            #  Somebody said where it is. Absent and accounted for — a note,
            #  and it carries the id so the entry can link out to the video
            #  rather than merely stop complaining about it.
            #
            #  And now an OFFER, because "the video is over there" stopped
            #  being the end of the sentence the moment a pull could be
            #  queued from a panel. Still a note: nothing is wrong with this
            #  entry, there is simply something that could be done about it.
            #  `declined` is what ends the asking, which is why it is now
            #  checked ABOVE this rather than below — answering it under the
            #  old order filed a fact that changed nothing and the question
            #  came back on the next sweep.
            out.append(_finding("note", "disk",
                                f"{label} was not recorded; the video is {located_id}",
                                platform=prefix, short="not recorded",
                                state="lost", remote_id=located_id,
                                question=_asked(
                                    "located_not_here", prefix,
                                    f"The {label} broadcast is {located_id} and "
                                    f"nothing from it is here. Pull the video, "
                                    f"the chat, or neither?",
                                    ["fetch_video", "fetch_chat", "declined"])
                                if can_fetch else None))
        else:
            # Missing, and nothing anywhere says why. That is the one case
            # worth asking about rather than reporting every sweep: deleting
            # the lower-quality copy is ordinary here, and an audit that calls
            # it a loss every week is an audit nobody reads.
            out.append(_finding("warn", "disk", f"{label} video missing",
                                platform=prefix, short="video MISSING",
                                state="lost",
                                question=_asked(
                                    "video_missing", prefix,
                                    f"The {label} video is not here. Was it "
                                    f"kept and lost, or deliberately not kept?"
                                    + (" It can be pulled." if can_fetch else ""),
                                    (["fetch_video"] if can_fetch else [])
                                    + ["declined", "no_broadcast", "identified"])))

        # CHAT. Deep storage counts as present: the merge folded it in and
        # moved it on purpose, and calling that missing is the single most
        # alarming line the old output produced for the one outcome that went
        # entirely right.
        if nas.get(f"{prefix}_chat"):
            out.append(_finding("ok", "disk", f"{label} chat on disk",
                                platform=prefix, short="chat on disk",
                                file=nas[f"{prefix}_chat"]))
        elif _chat_accounted(nas, prefix):
            out.append(_finding("ok", "disk", f"{label} chat folded into the merged file",
                                platform=prefix, short="chat merged", state="kept"))
        elif entry.get(f"{prefix}_chat_x") or declined:
            out.append(_finding("note", "disk", f"{label} chat deliberately not kept",
                                platform=prefix, short="chat not kept",
                                state="declined"))
        elif located_id:
            #  The same answer covers the chat. A broadcast nobody recorded
            #  has no chat log either, and asking about it separately is the
            #  second question this change exists to stop asking — so the
            #  offer above covers both, and this stays the quiet note it was.
            out.append(_finding("note", "disk", f"{label} chat was not recorded",
                                platform=prefix, short="not recorded", state="lost"))
        else:
            out.append(_finding("warn", "disk", f"{label} chat missing",
                                platform=prefix, short="chat MISSING",
                                state="lost",
                                question=_asked(
                                    "chat_missing", prefix,
                                    f"The {label} chat is not here and was not "
                                    f"folded into a merged file. Was there a "
                                    f"broadcast on {label} at all?"
                                    + (" The VOD's chat can be pulled."
                                       if can_fetch else ""),
                                    (["fetch_chat"] if can_fetch else [])
                                    + ["declined", "no_broadcast"])))
    return out


#  What each claim is called when a person has to read about it.
_CLAIM_SAYS = {
    ls_witness.BROADCAST_START: "the broadcast start",
    ls_witness.RECORD_START: "the recording start",
    ls_witness.FILE_DURATION: "the file's length",
    ls_witness.BROADCAST_DURATION: "the broadcast's length",
}

#  Below this, a file shorter than the broadcast is the ordinary tail — a
#  capture joined late or stopped at the end card. Above it, something was
#  truncated and that is worth a person's time. Five minutes is a guess made
#  once and written down rather than tuned; the number that matters is that
#  there IS one, because without a floor this fires on every single entry.
_TRUNCATION_FLOOR_S = 300


def _witness_findings(timings: dict) -> list[dict]:
    """Where the witnesses disagreed, and where there was nobody to ask.

    The part of C3 a person actually sees. Until now a timing was a number
    with a source label and no way to tell a corroborated answer from a lone
    one — so this reports the three states worth a person's attention and
    stays silent on the fourth, which is the common one.

    Silent on a plain single-sourced exact answer, deliberately. 363 of 450
    cached rows have exactly one clock witness; a note on each would be a
    report that flags four fifths of the archive and is therefore read by
    nobody. Single-sourced is recorded in the settlement for anyone who wants
    it and is not news.
    """
    out = []
    for prefix, t in sorted((timings or {}).items()):
        #  A platform with no files at all yields None from
        #  `_platform_timings`, and `inspect` does not store those — but a
        #  caller assembling its own dict may, and a report is not worth a
        #  traceback.
        if not t:
            continue
        label = "YouTube" if prefix == "yt" else "Twitch"
        settled = t.get("settled") or {}

        for claim, s in sorted(settled.items()):
            said = _CLAIM_SAYS.get(claim, claim)
            if s.get("agreement") == ls_witness.DISAGREE:
                #  Two independent witnesses to one question, far enough apart
                #  that their own precision cannot explain it. This is the
                #  thing the inversion exists to surface: before C3 the first
                #  one answered and the second was never asked.
                #  Only the witnesses that VOUCHED. The spread is computed
                #  over those alone, so listing the others put numbers in the
                #  sentence that its own figure does not describe — #716 read
                #  "disagree by 00:00:02" and then named a filename four and a
                #  half minutes away, which is a record-start proxy that
                #  cannot answer this question at all. A reader trying to
                #  reconcile the two is being sent after nothing.
                who = ", ".join(
                    f"{w.get('label') or w['source']} {_iso(w['value'])}"
                    if claim in (ls_witness.BROADCAST_START,
                                 ls_witness.RECORD_START)
                    else f"{w.get('label') or w['source']} "
                         f"{_seconds_to_hhmmss(w['value'])}"
                    for w in s.get("witnesses", [])
                    if w.get("corroborates", True))
                out.append(_finding(
                    "warn", "assignment",
                    f"{label}: witnesses disagree about {said} by "
                    f"{_seconds_to_hhmmss(s.get('spread_s') or 0)} — {who}",
                    platform=prefix, short=f"{said.split()[-1]} disputed",
                    claim=claim, spread_s=s.get("spread_s")))
            elif (claim == ls_witness.BROADCAST_START
                  and s.get("agreement") == ls_witness.SINGLE
                  and (s.get("precision_s") or 0) > ls_witness.PRECISE):
                #  The only answer is a rounded one. Worth saying because the
                #  number will be quietly used as though it were measured.
                #
                #  Scoped to the BROADCAST start and no other claim. The
                #  recording start is bookkeeping — knowing it to the minute
                #  changes nothing anybody does — and noting it fired on a
                #  clean fixture, which is the noise this whole function is
                #  trying not to be. The broadcast start is the one number
                #  everything else is measured from.
                out.append(_finding(
                    "note", "assignment",
                    f"{label}: {said} is only known to the minute, from "
                    f"{(s.get('best') or {}).get('label') or s.get('source')}",
                    platform=prefix, short=f"{said.split()[-1]} to the minute",
                    claim=claim))

        #  Nobody could say when it began. On Twitch that number cannot be
        #  rebuilt once the stream ends, which is why it is worth a line
        #  rather than an absence.
        bs = settled.get(ls_witness.BROADCAST_START) or {}
        if bs.get("agreement") == ls_witness.NONE:
            out.append(_finding(
                "note", "assignment",
                f"{label}: nothing on hand says when the broadcast began"
                + (" — on Twitch that is not recoverable later"
                   if prefix == "tw" else ""),
                platform=prefix, short="no broadcast start",
                claim=ls_witness.BROADCAST_START))

        #  The two durations are separate claims so neither overwrites the
        #  other — which is exactly what makes comparing them meaningful. A
        #  file much shorter than the broadcast is truncation, and the
        #  authority table names this as the check the platform's figure is
        #  FOR.
        on_disk = (settled.get(ls_witness.FILE_DURATION) or {}).get("value")
        aired = (settled.get(ls_witness.BROADCAST_DURATION) or {}).get("value")
        if on_disk and aired and aired - on_disk > _TRUNCATION_FLOOR_S:
            out.append(_finding(
                "warn", "disk",
                f"{label}: the file is {_seconds_to_hhmmss(aired - on_disk)} "
                f"shorter than the broadcast "
                f"({_seconds_to_hhmmss(on_disk)} of {_seconds_to_hhmmss(aired)})",
                platform=prefix, short="capture short",
                missing_s=aired - on_disk))
    return out


def _platform_findings(cache: list[dict], ids: dict, entry: dict) -> list[dict]:
    """Does this id name something that exists?

    Answered from the cache rather than from the network. That is not a
    shortcut — the cache is fed by Helix and yt-dlp and is refreshed by the id
    resolution that just ran, so it holds the platform's own answer. What it
    buys is an audit that can be run over two hundred entries without two
    hundred round trips, and an offline one that still says something useful.
    `--probe` is the door to a live check when the cached answer is doubted.
    """
    out = []
    for prefix, platform, label in (("yt", "youtube", "YouTube"),
                                    ("tw", "twitch", "Twitch")):
        if entry.get(f"no_{prefix}"):
            continue
        vid = (ids.get(platform) or (None, None))[0]
        if not vid:
            out.append(_finding("warn", "platform", f"no {label} id could be resolved",
                                platform=prefix, short="no id"))
            continue
        row = ls_common.find_vod(cache, vid, platform)
        if row is None:
            # Not necessarily wrong: Helix lists a bounded window and an old
            # entry falls out of it. Worth a word, not an alarm.
            out.append(_finding("note", "platform",
                                f"{label} {vid} is not in the cache — too old to list, "
                                f"or never confirmed", platform=prefix, id=vid,
                                short=f"{vid} unconfirmed"))
        elif ls_common.is_broadcast_row(row):
            # The whole #736 failure in one line: this id is the BROADCAST,
            # and a watch link built from it is a 404.
            #
            # And WHY, which is the part that was missing. `resolve_id` asks
            # Helix for the VOD; every way that ask can fail used to return an
            # empty list in silence, so "Twitch has not published it yet" and
            # "this archive has never once been able to reach Twitch" produced
            # the same line. The second is the likelier one — nothing in the
            # live cache carries a `stream_id` — and it is the one with an
            # answer a person can act on.
            why = ls_common.twitch_last_error
            out.append(_finding("bad", "platform",
                                f"{label} {vid} is a broadcast id, not a video — "
                                + (f"could not ask Twitch: {why}" if why
                                   else "no VOD published for it yet"),
                                platform=prefix, id=vid, reason=why,
                                short=f"{vid} is a BROADCAST id"))
        else:
            out.append(_finding("ok", "platform", f"{label} {vid} is a known video",
                                platform=prefix, id=vid, short=f"{vid} exists",
                                title=row.get("title")))
    return out


def _assignment_findings(cache: list[dict], ids: dict, entry: dict) -> list[dict]:
    """Is this capture on the RIGHT broadcast?

    The check nothing has performed, and the one that catches the failure that
    actually happened: one stream's Twitch capture filed against another
    stream's row. Every other check passes in that state — the file is there,
    the link resolves — because nothing compares the video's OWN start against
    the entry's.
    """
    out = []
    want = entry.get("date_obj")
    if not want:
        return [_finding("bad", "assignment", "the entry has no parseable date, so "
                         "nothing can be checked against it")]
    # THROUGH `_vault_epoch`, and the first version of this did not: the vault
    # writes a wall-clock reading with its offset beside it (`GMT-5`), while
    # Helix answers in UTC. Comparing the naive datetime directly made every
    # Twitch capture look five hours out of place — a check that fires on every
    # row is worse than no check, because it is the one people turn off.
    want_ms = (_vault_epoch(want, _tz_offset_min(entry.get("tz_str"))) or 0) * 1000
    for prefix, platform, label in (("yt", "youtube", "YouTube"),
                                    ("tw", "twitch", "Twitch")):
        if entry.get(f"no_{prefix}"):
            continue
        vid = (ids.get(platform) or (None, None))[0]
        if not vid:
            continue
        row = ls_common.find_vod(cache, vid, platform) or {}
        got_ms = row.get("stream_start_epoch_ms") or row.get("record_start_epoch_ms")
        if not got_ms:
            raw = str(row.get("start_time") or "")
            try:
                dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.astimezone()
                got_ms = int(dt.timestamp() * 1000)
            except ValueError:
                got_ms = None
        if not got_ms:
            out.append(_finding("note", "assignment",
                                f"nothing says when {label} {vid} started, so its "
                                f"assignment cannot be checked", platform=prefix, id=vid,
                                short="offset unknown"))
            continue
        off = abs(got_ms - want_ms) // 1000
        if off <= ASSIGN_WINDOW_S:
            out.append(_finding("ok", "assignment",
                                f"{label} starts within {off // 60}m of the entry",
                                platform=prefix, id=vid, off_s=off,
                                short=f"offset={off // 60}m"))
        else:
            out.append(_finding("bad", "assignment",
                                f"{label} {vid} starts {off // 3600}h{(off % 3600) // 60:02d}m "
                                f"from this entry — it may belong to another stream",
                                platform=prefix, id=vid, off_s=off,
                                short=f"offset={off // 3600}h{(off % 3600) // 60:02d}m"))
    return out


_MARK = {"ok": "✓", "note": "·", "warn": "!", "bad": "✗"}


def _clock(ms) -> str:
    """Wall time, to the second, in whatever zone this machine is in."""
    if not ms:
        return "—"
    return datetime.datetime.fromtimestamp(int(ms) / 1000).strftime("%H:%M:%S")


def media_report(config: dict, got: dict) -> dict:
    """The measurements an audit confirms, as DATA rather than as columns.

    Pulled out from under `render_media` for the same reason `inspect()` was
    pulled out from under `audit()`: two surfaces now show these numbers — a
    terminal and a panel on the website — and two implementations of "what is
    the broadcast's zero" would eventually disagree about a stream's start
    time, which is the one number in this whole system that everything else is
    measured from.

    It carries NUMBERS, not rendered strings, and that is deliberate. `_clock`
    formats in the Pi's local timezone; the archive renders in the viewer's
    browser using the stream's own `tz_offset_min`, and a report full of
    pre-formatted Pi-local times would disagree with the timeline drawn six
    inches above it. Each surface formats for itself; the measuring happens
    once, here.

    Reading a chat file costs a pass over it, so this is not free — and it was
    not free before either, since `render_media` did exactly the same reads.
    """
    nas_root = config.get("nas_path", "")
    nas, timings = got["nas"], got.get("timings") or {}

    # The broadcast's own zero, which is what every offset is measured from.
    # Earliest measured platform start, matching how `_archive_inputs` decides
    # the stream's `started_at` — so the audit and the archive are reading the
    # same instant.
    zeros = [t["stream_start_epoch_ms"] for t in timings.values()
             if t.get("stream_start_epoch_ms")]
    zero_ms = min(zeros) if zeros else None

    platforms = []
    for prefix in ("yt", "tw"):
        vid = (got["ids"].get("youtube" if prefix == "yt" else "twitch")
               or (None, None))[0]
        t = timings.get(prefix) or {}
        # Neither an id nor a measurement means this platform is not part of
        # this broadcast, and a row of dashes for it is noise.
        if not vid and not timings.get(prefix):
            continue
        row = {
            "platform": prefix,
            "id": vid,
            # The link, built by the one builder — an id is not something a
            # person can click, and the panel had them retyping it into a
            # browser bar to check a recording against its source.
            "url": (ls_common.build_stream_url(
                config, "youtube" if prefix == "yt" else "twitch", vid)
                if vid else None),
            "stream_start_ms": t.get("stream_start_epoch_ms"),
            "record_start_ms": t.get("record_start_epoch_ms"),
            "duration_s": t.get("duration_secs"),
            "duration_source": t.get("duration_source"),
        }
        raw = nas.get(f"{prefix}_chat")
        if raw:
            info = analyze_chat_file(os.path.join(nas_root, raw))
            row["chat"] = {
                "state": "raw", "file": raw,
                # `analyze_chat_file` answers the string "UNKNOWN" for a file
                # it could not parse. Kept as None here rather than passed on:
                # a reader should not have to know that one field is sometimes
                # a number and sometimes a word meaning "no number".
                "count": info["count"] if isinstance(info["count"], int) else None,
                "first": None if info["first_ts"] == "UNKNOWN" else info["first_ts"],
                "last": None if info["last_ts"] == "UNKNOWN" else info["last_ts"],
            }
        elif _chat_accounted(nas, prefix):
            row["chat"] = {"state": "merged"}
        else:
            row["chat"] = {"state": "none"}
        platforms.append(row)

    # ── how long the BROADCAST ran, which is a different question from how
    # long either recording is ─────────────────────────────────────────
    #
    # The longest recording, and that is the pre-C rule restored verbatim
    # — `dur = max(durations)`. It is also the check that a single platform
    # cannot do for itself: a capture that died early looks complete from the
    # inside, and only the OTHER platform's length says otherwise. Entry #716
    # is the case — a bugged YouTube recording against a Twitch one that ran
    # the real two hours.
    #
    # `proposed` rather than `duration`, because this is what the audit would
    # write into the vault, and naming it after the decision keeps it from
    # being read as a fifth measurement.
    lengths = [p["duration_s"] for p in platforms if p.get("duration_s")]
    out = {"zero_ms": zero_ms, "platforms": platforms, "merged": None,
           "proposed_duration_s": max(lengths) if lengths else None,
           # How far apart the recordings are. A broadcast recorded twice
           # should produce two files of nearly the same length; a gap here is
           # one of them being wrong, and it is the only evidence that says so.
           "duration_spread_s": (int(round(max(lengths) - min(lengths)))
                                 if len(lengths) > 1 else None)}

    merged = got.get("merged")
    if not merged:
        return out
    meta = _merged_chat_meta(os.path.join(nas_root, merged)) or {}
    out["merged"] = {
        "file": merged,
        "sources": meta.get("chat_sources"),
        "messages": meta.get("chat_messages"),
        "first_ms": meta.get("chat_first_ms"),
        "last_ms": meta.get("chat_last_ms"),
        "archived_raws": (len(nas.get("yt_chats_archived", []))
                          + len(nas.get("tw_chats_archived", []))),
    }
    return out


def render_media(config: dict, got: dict, *, verbose: bool = False) -> None:
    """The measurements, aligned into columns, two lines per platform.

    These are the numbers an audit exists to confirm, and the first version of
    the quiet output dropped all of them — which traded one problem for the
    opposite one. A run that says "8 checks passed" and nothing else is not a
    report, it is a receipt: nothing in it shows that a duration is plausible,
    that the two platforms started together, or that a chat covers the
    broadcast.

    `-v` adds where each clock came from and how well it is known, which is
    genuinely only interesting once a number looks wrong.

    The numbers come from `media_report`; this is only the columns. Provenance
    under `-v` is read from `timings` directly, because it is the one thing
    here the website's report deliberately does not carry.
    """
    timings = got.get("timings") or {}
    rep = media_report(config, got)
    zero_ms = rep["zero_ms"]

    for row in rep["platforms"]:
        prefix = row["platform"]
        label = prefix.upper()
        # `—` rather than `_seconds_to_hhmmss`'s "UNKNOWN": the other columns
        # on this line already spell an absent number that way, and one row
        # reading `start — | rec — | UNKNOWN` says the same thing three ways.
        dur = (_seconds_to_hhmmss(row["duration_s"]) if row["duration_s"] else "—")
        print(f"  {label}  {(row['id'] or '—'):<14} |  "
              f"start {_clock(row['stream_start_ms']):<8} |  "
              f"rec {_clock(row['record_start_ms']):<8} |  {dur}")
        t = timings.get(prefix)
        if verbose and t:
            # Provenance, which matters exactly when a clock looks wrong: a
            # number off a filename is a minute-accurate guess, one off a chat
            # file is the real thing.
            for which in ("stream_start", "record_start"):
                src = t.get(f"{which}_source")
                if src:
                    print(f"      {which.replace('_', ' '):<13}"
                          f"{src}  ±{t.get(f'{which}_accuracy') or '?'}")
        chat = row["chat"]
        if chat["state"] == "raw":
            span = (f"   ({chat['first']} → {chat['last']})"
                    if chat["first"] else "")
            print("      chat  "
                  + (f"{chat['count']:,} msgs" if chat["count"] is not None
                     else "unreadable")
                  + span)
            if verbose:
                print(f"            {chat['file']}")
        elif chat["state"] == "merged":
            print("      chat log merged")
        else:
            print("      chat  —")

    # The merged file, which is the one the site actually plays, on its own
    # block: it is a property of the broadcast rather than of either platform,
    # and hanging it off the last platform's lines read as if it belonged to
    # that one.
    print()
    m = rep["merged"]
    if not m:
        print("CHAT  not merged yet")
        print()
        return
    print(f"CHAT merged  {m['file']}"
          + (f"   {m['sources']}" if m.get("sources") else ""))
    line2 = []
    if m.get("messages"):
        line2.append(f"{m['messages']:,} msgs")
    # OFFSETS from the broadcast start, not wall clock. The stored numbers are
    # absolute — they have to be, holding two platforms whose zeros differ —
    # and rendering them as wall times made a correct 11h36m span read as
    # nonsense on a two-hour stream. It is not nonsense: YouTube's chat opens
    # with the waiting room hours before she goes live, so the span really does
    # begin nine hours early. Said as an offset it reads as what it is, and it
    # matches the raw chat lines above rather than inventing a second
    # convention. Wall clock is under -v for anyone who wants the clock.
    if m.get("first_ms") and m.get("last_ms"):
        if zero_ms:
            line2.append(
                f"({_seconds_to_hhmmss((m['first_ms'] - zero_ms) // 1000)}"
                f" → {_seconds_to_hhmmss((m['last_ms'] - zero_ms) // 1000)})")
        else:
            line2.append(f"({_clock(m['first_ms'])} → {_clock(m['last_ms'])})")
    if line2:
        print(f"             {'   '.join(line2)}")
    if verbose and m.get("first_ms"):
        print(f"             wall {_clock(m['first_ms'])}"
              f" → {_clock(m['last_ms'])}")
    if m["archived_raws"]:
        n = m["archived_raws"]
        print(f"             {n} raw{'s' if n != 1 else ''} in deep storage")
    print()


def render_findings(findings: list[dict], *, verbose: bool = False) -> None:
    """One line per platform, then the full sentence for anything not ok.

    Two passes rather than one, and both are needed. The summary line is what
    makes a clean entry readable at a glance — four short verdicts across,
    aligned with its neighbour so the two platforms can be compared by eye.
    But a problem's whole value is in its sentence: "could not ask Twitch:
    twitch_user_id is not set" tells you what to do and `318648037478 is a
    BROADCAST id` does not.

    So: everything on the summary, and then the ones that need saying said
    properly underneath. A collapsed count was tried and dropped — it told you
    that something had been checked without telling you what, so a check that
    silently stopped running looked exactly like one that passed.
    """
    if not findings:
        print("  · nothing to check")
        print()
        return
    # Padded per COLUMN, across the platform rows, so the separators line up
    # down the page. The verdicts are naturally different lengths — a YouTube
    # id is eleven characters and a Twitch one is ten — and one character of
    # drift is enough to stop two rows reading as a comparison, which is the
    # only reason to put them one above the other.
    rows = []
    for prefix in ("yt", "tw"):
        mine = [f for f in findings if f.get("platform") == prefix]
        if mine:
            rows.append((prefix, [f"{_MARK.get(f['level'], ' ')} {f['short']}"
                                  for f in mine]))
    widths: dict[int, int] = {}
    for _, cells in rows:
        for i, c in enumerate(cells):
            widths[i] = max(widths.get(i, 0), len(c))
    for prefix, cells in rows:
        line = " | ".join(c.ljust(widths[i]) for i, c in enumerate(cells))
        # rstrip, or the last column pads into trailing whitespace on the
        # shorter row and every line ends somewhere different.
        print(f"[{prefix.upper()}] {line}".rstrip())
    loose = [f for f in findings if not f.get("platform")]
    for f in loose:
        print(f"     {_MARK.get(f['level'], ' ')} {f['message']}")
    # And the detail, for whatever is not fine.
    bad = [f for f in findings if f["level"] in ("warn", "bad")
           and f.get("platform")]
    if bad:
        print()
        for f in bad:
            print(f"  {_MARK.get(f['level'], ' ')} [{f['platform'].upper()}] "
                  f"{f['message']}")
    print()


def worst(findings: list[dict]) -> str:
    """The loudest level present, or 'ok' for nothing at all."""
    return max((f["level"] for f in findings), key=lambda l: _RANK.get(l, 0),
               default="ok")


def inspect(config: dict, index: int, *,
            yt_override: str | None = None,
            tw_override: str | None = None,
            cache: list[dict] | None = None,
            claims: list[dict] | None = None,
            archive: dict | None = None) -> dict:
    """Everything an audit knows about one entry. Prints nothing, asks nothing.

    The return value is the whole of what the CLI renders, what a job report
    would carry, and what a test asserts against — which is the point of it
    existing separately.
    """
    out = {"index": int(index), "ok": False, "reason": None, "findings": [],
           "entry": None, "nas": None, "ids": {}, "block": None,
           "stream_fields": None, "captures": None, "questions": []}

    # What the archive has already been told, if the caller fetched it. Kept
    # optional so `inspect` still works from a terminal with no archive
    # reachable — the same reason it takes `cache` rather than loading one.
    known = _claims_index(claims)
    # And what it holds for the captures, which is where an id entered on the
    # website lives. Fetched here only when the caller did not already have a
    # lookup in hand; the worker does, and a second round trip for the same
    # answer is a second thing to keep in step.
    if archive is None and ls_archive.enabled(config):
        archive = ls_archive.lookup(config, idx=int(index))
    arch_ids = archive_ids(archive)

    entry = ls_common.obsidian_parse_entry(config, index)
    if not entry["found"]:
        out["reason"] = f"entry #{index} not found"
        return out
    entry["_index"] = index
    out["entry"] = entry
    if not entry["date_obj"]:
        out["reason"] = f"cannot parse the date for #{index}"
        out["findings"].append(_finding("bad", "assignment", out["reason"],
                                        raw=entry.get("date_str")))
        return out

    cache = ls_common.load_cache() if cache is None else cache
    nas = scan_nas(config, index)
    out["nas"] = nas

    ids = {}
    if not entry.get("no_yt"):
        ids["youtube"] = resolve_id(config, cache, "youtube", entry, nas,
                                    yt_override, arch_ids)
    if not entry.get("no_tw"):
        ids["twitch"] = resolve_id(config, cache, "twitch", entry, nas,
                                   tw_override, arch_ids)
    out["ids"] = ids

    out["findings"] = (_disk_findings(config, nas, entry, known, ids)
                       + _platform_findings(cache, ids, entry)
                       + _assignment_findings(cache, ids, entry))
    # Lifted out flat for a caller that wants the questions and not the whole
    # report — the plan that goes home, and eventually the panel. They stay on
    # their findings too, because the finding is where the context is.
    out["questions"] = [f["detail"]["question"] for f in out["findings"]
                        if f.get("detail", {}).get("question")]

    # The measurements, kept so the renderer does not have to re-derive them.
    # `_platform_timings` reads chat files and shells out to ffprobe; asking it
    # twice for one entry would double the slowest part of an audit.
    #  The same history `cmd_timings` writes with. Without this the report
    #  would describe a platform as having nothing while the sidecar beside it
    #  holds its broadcast start -- two answers to one question, from one run.
    prior_ws = _kept_testimony(config, index)
    out["timings"] = {}
    for prefix, platform in (("yt", "youtube"), ("tw", "twitch")):
        if entry.get(f"no_{prefix}"):
            continue
        t = _platform_timings(config, cache, nas, prefix, platform,
                              prior_ws.get(prefix))
        if t:
            out["timings"][prefix] = t
    #  A cached broadcast length the two recordings disprove, corrected before
    #  anything reads it. It needs both platforms in hand, which is why it is
    #  out here and not inside `_platform_timings` — and why putting it in
    #  there first was wrong: one recording cannot tell "the cache is stale"
    #  from "this capture died early".
    for line in _reconcile_durations(config, cache, out["timings"]):
        out["findings"].append({
            "level": "ok", "check": "duration", "platform": None,
            "short": "cache corrected", "message": line.strip()})
    #  What the witnesses could not agree on, which is the product of C3 and
    #  is computed here rather than in `_platform_timings` because it needs
    #  both platforms' settlements in hand. Appended after the three checks
    #  and before `worst`, so a disagreement can raise an entry's level.
    out["findings"] += _length_findings(config, nas, out["timings"])
    out["findings"] += _witness_findings(out["timings"])
    out["merged"] = find_merged_chat(config.get("nas_path", ""), index)

    yt_id = (ids.get("youtube") or (None, None))[0]
    tw_id = (ids.get("twitch") or (None, None))[0]
    out["block"] = build_entry(config, cache, index, entry, nas, yt_id, tw_id)
    out["stream_fields"], out["captures"] = _archive_inputs(
        config, cache, entry, nas, index, yt_id, tw_id)
    out["cache"] = cache
    out["ok"] = True
    out["worst"] = worst(out["findings"])
    return out


def finish(config: dict, index: int,
           cache: list[dict] | None = None) -> dict:
    """The deterministic half of an audit: sidecar, merged chat, raws archived.

    What `inspect()` is to the checks, this is to the work: the door a caller
    with nobody sitting at it comes in by. `audit()` still calls `_pipeline`
    directly, because it is the interactive one and has a terminal to ask at.

    Deterministic is the whole claim and it is worth being precise about.
    `cmd_timings` derives the entry's clocks from files already on disk.
    `ls_chat.merge` UNIONS every source rather than preferring one, so there
    is no wrong side for it to come down on — two captures of one chat merge
    to the same file whichever order they arrive in. Neither asks anything and
    neither has an editorial opinion, which is why they can run without a
    person the way a rescan can and an id resolution cannot.

    Nothing is downloaded. A chat short enough to want repairing comes back in
    `deferred` instead, because spending somebody's bandwidth is a decision
    and this function has nobody to take it.

    Returns {"changed": bool, "deferred": [...]}. `changed` means a file
    actually appeared or was rewritten — the caller re-reads the entry on it,
    so claiming work that did not happen costs a wasted second pass.
    """
    deferred: list = []
    cache = ls_common.load_cache() if cache is None else cache
    changed = _pipeline(config, cache, index, interactive=False,
                        deferred=deferred)
    return {"changed": bool(changed), "deferred": deferred}


def repair(config: dict, index: int, platform: str,
           cache: list[dict] | None = None) -> dict:
    """Run the one repair `finish()` declined, because somebody said yes.

    The other half of the rule. `finish()` refuses to download because nobody
    authorised it; this runs because somebody did, and it is the only door in
    this file that pulls with no terminal attached. Everything that makes that
    safe is upstream of here — the work arrives as a job, one platform at a
    time, queued by a click — so this stays a small thing that does exactly
    what it was told and then lets the deterministic half finish.

    The shortfall is checked AGAIN rather than taken from whoever asked. A
    question can sit in a panel for a week; by the time it is answered the
    chat may have been repaired from a terminal, and downloading a second copy
    of something already merged is the kind of work nobody notices is wasted.

    Returns {"ran", "changed", "deferred", "why"} — `ran` False with a `why`
    is the honest no-op, not a failure.
    """
    if platform not in ("youtube", "twitch"):
        return {"ran": False, "changed": False, "deferred": [],
                "why": f"unknown platform {platform!r}"}
    cache = ls_common.load_cache() if cache is None else cache
    nas = scan_nas(config, index)
    if platform == "youtube":
        yt_id = (ls_common.extract_video_id_from_filename(nas["yt_chat"])
                 if nas.get("yt_chat") else None)
        short = _yt_chat_shortfall(config, cache, nas, yt_id)
    else:
        tw_id = (ls_common.extract_video_id_from_filename(nas["tw_chat"])
                 if nas.get("tw_chat") else None)
        short = _tw_chat_shortfall(config, cache, nas, tw_id, index)
    if not short:
        # Not a failure. The entry is in the state the asker wanted it in, and
        # the merge below may still have something to do.
        done = finish(config, index, cache=cache)
        return {"ran": False, "changed": done["changed"],
                "deferred": done["deferred"],
                "why": f"the {platform} chat is no longer short"}

    run = _backfill_yt_chat if platform == "youtube" else _backfill_tw_chat
    ok = bool(run(config, short))
    # Either way, finish what can be finished. A Twitch pull that failed does
    # not hold the merge back — the union happens in the merge and a later
    # pull re-merges to the full thing — and a YouTube one that failed will
    # block it there, which is `_pipeline`'s call and not this function's.
    done = finish(config, index, cache=cache)
    return {"ran": ok, "changed": done["changed"],
            "deferred": done["deferred"],
            "why": None if ok else f"the {platform} repair did not come back"}


def give_up_chat(index: int, platform: str) -> dict:
    """Stop waiting for this entry's chat on this platform.

    The other answer to a short chat, and the one that costs nothing: a chat
    that can never be placed is not repaired by being asked about every sweep
    for the rest of time. Wraps `cmd_give_up_chat` rather than replacing it —
    the terminal has offered this since the merge learned to ask, the ledger
    is the same file, and undoing it is still deleting a line from it.
    """
    if platform not in ("youtube", "twitch"):
        return {"ok": False, "why": f"unknown platform {platform!r}"}
    cmd_give_up_chat(index, platform, why="let go from the archive")
    # Read back rather than trusting the write: `cmd_give_up_chat` prints its
    # refusals and returns None either way, so this is the only way to answer
    # the caller honestly.
    return {"ok": bool(chat_given_up(index, platform)), "why": None}


# ═══════════════════════════════════════════════════════════════════════════
#  AUDIT
# ═══════════════════════════════════════════════════════════════════════════

def audit(config: dict, index: int,
          yt_override: str | None = None,
          tw_override: str | None = None,
          push_archive: bool = True,
          interactive: bool = True,
          verbose: bool = False,
          show_block: bool = False):
    """
    Verify entry #index, and repair the vault line while it is here.

    1. Parse Obsidian entry → checkbox, date, notes, existing IDs
    2. Scan NAS → existing files
    3. Resolve IDs (override → entry → NAS → cache)
    4. Build reconstructed entry
    5. Write to Obsidian
    6. Offer downloads for missing files
    """
    # The banner is printed AFTER the inspection rather than before, so the
    # date can go in it. One heading instead of a heading and a line under it.
    # 1-4. Everything the audit knows, in one call and with nothing printed.
    got = inspect(config, index, yt_override=yt_override, tw_override=tw_override)
    when = ((got.get("entry") or {}).get("date_str") or "").strip()
    tz = ((got.get("entry") or {}).get("tz_str") or "").strip()
    head = f"  Auditing entry #{index}" + (f" - {when} {tz}".rstrip() if when else "")
    print(f"\n{'=' * 60}")
    print(head)
    print(f"{'=' * 60}\n")

    if not got["ok"]:
        print(f"  ✗ {got['reason']}")
        if got.get("entry") and got["entry"].get("date_str"):
            print(f"    Raw: {got['entry']['date_str']}")
        return
    entry, nas, cache = got["entry"], got["nas"], got["cache"]
    yt_id = (got["ids"].get("youtube") or (None, None))[0]
    tw_id = (got["ids"].get("twitch") or (None, None))[0]
    block = got["block"]

    # The date is in the banner and the ids are in the media block, beside the
    # numbers they belong to. Only the checkbox is left, and it goes with the
    # thing it is about rather than on a line of its own.
    render_media(config, got, verbose=verbose)
    render_findings(got["findings"], verbose=verbose)

    # 5. Write
    # Headless runs write without asking. The reconstruction is deterministic
    # and the vault is in git; a timer that stops to ask a question nobody is
    # there to answer just never runs at all, which is what --sweep did.
    #
    # The BLOCK is not printed by default. It is one markdown line per platform
    # and most of it is percent-encoded obsidian:// URLs six hundred characters
    # long — reading it was never how anybody checked anything, the findings
    # above are. `--show` prints it; `-v` implies it.
    if show_block or verbose:
        print("  ┌─ Reconstructed ────────────────────")
        for line in block:
            print(f"  │ {line}")
        print("  └───────────────────────────────────\n")

    # ONE question, asked here and ANSWERED here — but the write itself waits
    # until the end of the run. It used to ask twice, once now and once after
    # the merge, about the same file, with the second answer quietly
    # overwriting the first; the only way to say "yes, but not that one" was
    # to not notice you had been asked twice. Deferring the write means the
    # line that lands is built from what the run actually did, which is what
    # the second prompt was really for.
    may_write = (not interactive
                 or input("  Write to Obsidian? (y/n): ").strip().lower() == "y")
    if not may_write:
        print("  Obsidian left alone.")

    def _write_vault(lines):
        if not may_write:
            return
        if ls_common.obsidian_write_entry(config, index, lines):
            print("  ✓ Obsidian entry written.")
        else:
            print("  ✗ Obsidian write failed.")
    print()

    # 6. Missing files → download
    absent = {
        "yt_video": entry.get("yt_video_x", False),
        "yt_chat":  entry.get("yt_chat_x", False),
        "tw_video": entry.get("tw_video_x", False),
        "tw_chat":  entry.get("tw_chat_x", False),
    }

    missing = _identify_missing(config, nas, yt_id, tw_id, absent)
    changed = False

    if missing:
        if not interactive:
            # A missing VOD can be many GB; never pull one unattended.
            for m in missing:
                print(f"  ✗ missing: {m['label']}")
        elif _download_files(config, missing, index):
            changed = True
            nas = scan_nas(config, index)   # coverage needs the new files
    else:
        print("  ✔ All files present.\n")

    # 7. meta sidecar, merged chat, archive raws
    if _pipeline(config, cache, index, interactive=interactive):
        changed = True
        nas = scan_nas(config, index)

    if not changed:
        _write_vault(block)
        # Save cache (may have been updated by title lookups)
        ls_common.save_cache(cache)
        if push_archive:
            cmd_archive(config, index, entry=entry, nas=nas, cache=cache,
                        yt_id=yt_id, tw_id=tw_id, interactive=interactive)
        return

    # Re-scan and rebuild after download
    print("\n  Re-scanning NAS...")
    nas = scan_nas(config, index)
    for key in ("yt_video", "yt_chat", "tw_video", "tw_chat"):
        if nas[key]:
            print(f"    ✔ {nas[key]}")
            continue
        # A raw chat the merge just folded in and moved to deep storage is
        # NOT missing, and saying so about a file this very run archived on
        # purpose is the most alarming line in the output for the one outcome
        # that went right. `_chat_accounted` has always known the difference;
        # this loop was the one place that did not ask it.
        if key.endswith("_chat") and _chat_accounted(nas, key[:2]):
            print("    ✔ raw chat in deep storage — the merged file holds it")
        else:
            print("    ✗ still missing")
    print()

    # Re-run media analysis on freshly downloaded files
    _print_media_analysis(config, nas)

    block = build_entry(config, cache, index, entry, nas, yt_id, tw_id)
    if show_block or verbose:
        print("  ┌─ Updated ──────────────────────────────────────────")
        for line in block:
            print(f"  │ {line}")
        print("  └────────────────────────────────────────────────────\n")

    # The rebuilt line, using the answer already given.
    _write_vault(block)

    ls_common.save_cache(cache)

    # Last, and only after the vault write: the archive learns what this run
    # settled, not what it was still deciding.
    if push_archive:
        cmd_archive(config, index, entry=entry, nas=nas, cache=cache,
                    yt_id=yt_id, tw_id=tw_id, interactive=interactive)
    print()


# ═══════════════════════════════════════════════════════════════════════════
#  ARCHIVE
# ═══════════════════════════════════════════════════════════════════════════
#
#  Push what this audit reconstructed to the tenma archive: create the stream
#  if it has never seen this broadcast, otherwise fill in blanks and ask about
#  anything that collides. One direction only. Nothing in the archive is ever
#  read back into the vault, because two systems writing to each other is how
#  both end up wrong.

TZ_RE = re.compile(r"GMT\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?", re.I)


def _tz_offset_min(tz_str: str | None) -> int | None:
    """`(GMT-6)` → -360. Bare `(GMT)` → 0. Anything else → None."""
    if not tz_str:
        return None
    m = TZ_RE.search(tz_str)
    if not m:
        return 0 if "gmt" in tz_str.lower() else None
    sign = -1 if m.group(1) == "-" else 1
    return sign * (int(m.group(2)) * 60 + int(m.group(3) or 0))


def _vault_epoch(date_obj, tz_min: int | None) -> int | None:
    """The vault writes a wall-clock reading; the offset says where."""
    if not date_obj:
        return None
    return int(calendar.timegm(date_obj.timetuple()) - (tz_min or 0) * 60)


MERGED_CHAT_HEAD_BYTES = 8 * 1024 * 1024


# Whole numbers the archive will accept, and the smallest each may be. The
# server enforces the same floors and 400s on anything else; sending a value
# it will refuse is a sweep that fails every time it runs.
_CHAT_META_INTS = (("version", "chat_version", 1),
                   ("messages", "chat_messages", 0),
                   ("first_abs_ms", "chat_first_ms", 0),
                   ("last_abs_ms", "chat_last_ms", 0))
_MODERATION = ("complete", "none", "unknown")


def _merged_chat_meta(path: str) -> dict | None:
    """What a merged chat says about itself, without reading the messages.

    The header read itself lives in ls_chat, which owns the format. This turns
    it into the archive's vocabulary.

    Returns the archive's own field names, ready to merge into stream_fields —
    the caller should not have to know that `first_abs_ms` in the file is
    `chat_first_ms` in the database.

    Deliberately best-effort, and per field rather than all-or-nothing: a v1
    file has no `moderation` and no span, and the right outcome there is four
    fields sent and two left alone, not a stream the archive knows nothing
    about. If the format changes past recognition this returns None and the
    caller sends nothing, which is a gap rather than a lie.
    """
    meta = ls_chat.read_header(path, MERGED_CHAT_HEAD_BYTES)
    if meta is None:
        return None
    try:
        out: dict = {}

        # ls_chat names platforms 'youtube', 'twitch' and 'unknown'. Map by
        # name, never by truncation: 'youtube'[:2] is 'YO', which silently
        # dropped YouTube from the list and left the archive recording that a
        # dual-platform stream had Twitch chat only.
        plats = {ls_archive.PLATFORM.get(str(src.get("platform", "")).strip().lower())
                 for src in meta.get("sources", [])
                 if src.get("messages") and src.get("zero_ms") is not None}
        # 'unknown' maps to None and is dropped: a source ls_chat could not
        # identify is not evidence that either platform had chat.
        sources = ",".join(sorted(p for p in plats if p))
        if sources:
            out["chat_sources"] = sources

        for key, col, floor in _CHAT_META_INTS:
            v = meta.get(key)
            # `not isinstance(v, bool)` because True is an int in Python and
            # would sail through as a message count of 1.
            if isinstance(v, int) and not isinstance(v, bool) and v >= floor:
                out[col] = v

        mod = {}
        for plat, how in (meta.get("moderation") or {}).items():
            p = ls_archive.PLATFORM.get(str(plat).strip().lower())
            if p and how in _MODERATION:
                mod[p] = how
        if mod:
            # Sorted and space-free, matching the archive's canonical form
            # exactly. The two are string-compared on every sweep to decide
            # whether anything changed, so a second spelling of the same fact
            # would collide forever and ask a human about it every run.
            out["chat_moderation"] = json.dumps(mod, sort_keys=True,
                                                separators=(",", ":"))
        return out or None
    except Exception:
        return None


def _archive_inputs(config: dict, cache: list[dict], entry: dict, nas: dict,
                    index: int, yt_id: str | None, tw_id: str | None):
    """Everything the archive could learn from this audit."""
    caps, timings = [], {}

    # The merged chat is a property of the broadcast, not of one platform, so
    # it lives on the stream. It is also the only chat the site serves.
    nas_root = config.get("nas_path", "")
    merged_name = find_merged_chat(nas_root, index)
    merged_rel = ls_archive.archive_path(config, merged_name) if merged_name else None

    for prefix, platform, vid, no_it in (
            ("yt", "youtube", yt_id, entry.get("no_yt")),
            ("tw", "twitch",  tw_id, entry.get("no_tw"))):
        # A platform the vault says had no stream gets no capture. Absence is
        # how the archive already spells "there was nothing here".
        if no_it or not vid:
            continue
        t = _platform_timings(config, cache, nas, prefix, platform,
                              _kept_testimony(config, index).get(prefix)) or {}
        timings[platform] = t
        cap = {
            "platform": platform,
            "remote_id": vid,
            "title": _get_title(config, cache, vid, platform, nas[f"{prefix}_video"]),
            "video_path": ls_archive.archive_path(config, nas[f"{prefix}_video"]),
            "chat_path": ls_archive.archive_path(config, nas[f"{prefix}_chat"]),
        }
        # The url is OMITTED rather than guessed when the id is a broadcast
        # whose VOD has not appeared. `remote_id` still goes — it is how the
        # packet addresses the capture and the archive already holds it — but
        # a watch link built from a broadcast id is a 404, and sending one
        # replaces whatever the archive had with something worse. Leaving the
        # field out leaves the archive's own value alone; the next run, once
        # Twitch has published, sends the real one.
        if unpublished_vod(cache, platform, vid):
            print(f"  ⏳ {prefix.upper()} VOD not published yet — leaving its "
                  f"link alone (re-run later)")
        else:
            cap["url"] = ls_common.build_stream_url(config, platform, vid)
        # Once the merge holds this platform's messages the raw goes to deep
        # storage, and the archive is told to stop pointing at it rather than
        # left holding a path that now reads `lost`. Only when the raw is
        # actually gone: with chat_archive_path unset the pipeline leaves them
        # in place and there is nothing to forget.
        if merged_rel and not nas[f"{prefix}_chat"]:
            cap["clear"] = ["chat_path"]

        # ── why there is no local copy ───────────────────────────────────
        # The `.×` in an Obsidian line is a person saying "I did not keep
        # this", and it is the only record of that decision anywhere. The
        # archive was never told, so it read every deliberate deletion as a
        # loss — which is most of them, because both platforms get recorded
        # and one master is usually enough. Sending it here is what carries
        # that decision out of the markdown before the markdown goes away.
        if nas[f"{prefix}_video"]:
            cap["video_state"] = "kept"
        elif entry.get(f"{prefix}_video_x"):
            cap["video_state"] = "declined"
        if nas[f"{prefix}_chat"] or _chat_accounted(nas, prefix):
            cap["chat_state"] = "kept"
        elif entry.get(f"{prefix}_chat_x"):
            cap["chat_state"] = "declined"
        if t.get("duration_secs"):
            cap["duration_s"] = int(t["duration_secs"])
        if t.get("stream_start_epoch_ms"):
            cap["broadcast_started_at"] = t["stream_start_epoch_ms"] // 1000
        if t.get("record_start_epoch_ms"):
            cap["record_started_at"] = t["record_start_epoch_ms"] // 1000
            # ls-audit's own word for how well it knows this, carried through
            # so the archive can refuse to let a filename guess overwrite a
            # measurement — and so the theater can draw the difference.
            cap["local_start_precision_s"] = (
                60 if t.get("record_start_accuracy") == "minute" else 1)
        caps.append(cap)

    tz_min = _tz_offset_min(entry.get("tz_str"))
    # The broadcast start, when something measured it, beats the vault's
    # hand-typed minute. Both are offered; the archive decides nothing, the
    # human answering the prompt does.
    measured = [t["stream_start_epoch_ms"] // 1000 for t in timings.values()
                if t.get("stream_start_epoch_ms")]
    started = min(measured) if measured else _vault_epoch(entry.get("date_obj"), tz_min)

    stream_fields = {}
    title = next((c["title"] for c in caps if c.get("title")), None)
    if title:
        stream_fields["title"] = title
    if started:
        stream_fields["started_at"] = started
    if tz_min is not None:
        stream_fields["tz_offset_min"] = tz_min
    if merged_rel:
        stream_fields["chat_path"] = merged_rel
        # Everything the file says about itself, in the archive's field names.
        # Sent WITH the path, never separately: the archive records which file
        # a description belongs to by looking at the path in the same packet,
        # and a description arriving on its own would be filed against
        # whatever happened to be there.
        stream_fields.update(
            _merged_chat_meta(os.path.join(nas_root, merged_name)) or {})
    return stream_fields, caps


def cmd_archive(config: dict, index: int, *, entry: dict | None = None,
                nas: dict | None = None, cache: list[dict] | None = None,
                yt_id: str | None = None, tw_id: str | None = None,
                interactive: bool = True) -> bool:
    """Reconcile entry #index with the archive. Returns True if anything was
    sent. Never raises: the audit is the point, this is a courtesy."""
    if not ls_archive.enabled(config):
        return False
    try:
        if entry is None:
            entry = ls_common.obsidian_parse_entry(config, index)
            if not entry["found"]:
                print(f"  ✗ Entry #{index} not found.")
                return False
            entry["_index"] = index
        if nas is None:
            nas = scan_nas(config, index)
        if cache is None:
            cache = ls_common.load_cache()
        # Only reached by `--archive` on its own; the audit resolves first and
        # hands both ids down. Enabled is already checked above, so the lookup
        # is one call on a path that is about to make several.
        arch_ids = ({} if (yt_id or tw_id)
                    else archive_ids(ls_archive.lookup(config, idx=index)))
        if yt_id is None and not entry.get("no_yt"):
            yt_id, _ = resolve_id(config, cache, "youtube", entry, nas, None, arch_ids)
        if tw_id is None and not entry.get("no_tw"):
            tw_id, _ = resolve_id(config, cache, "twitch", entry, nas, None, arch_ids)

        stream_fields, caps = _archive_inputs(
            config, cache, entry, nas, index, yt_id, tw_id)
        if not caps and not stream_fields:
            return False

        print("\n  ┌─ Archive ──────────────────────────────────────────")
        plan = ls_archive.build_plan(config, idx=index,
                                     stream_fields=stream_fields, captures=caps)
        print(ls_archive.render_plan(plan))
        print("  └────────────────────────────────────────────────────")
        if not plan.get("ok") or not plan["items"]:
            return False

        plan = ls_archive.confirm_plan(plan, interactive=interactive)
        if not any(i.get("accepted") for i in plan["items"]):
            print("  Nothing sent.\n")
            return False
        if interactive and input("\n  Send to archive? (y/n): ").strip().lower() != "y":
            print("  Skipped.\n")
            return False

        results = ls_archive.push_plan(config, plan)
        # ONE line. `push_plan` sends a packet per capture and every packet
        # comes back with the same STREAM-level answer, so printing per result
        # said the identical sentence twice and read like a double push. The
        # count is what actually differs between them, so that is what is
        # added. The last result is the one read because the first may be the
        # one that created the stream, and only after it is there an index.
        if results:
            last = results[-1]
            print(f"  ✔ #{last.get('index')} "
                  f"vod={last.get('vod_state')} chat={last.get('chat_state')}"
                  + ("  (created)" if any(r.get("created") for r in results) else "")
                  + (f"  · {len(results)} captures" if len(results) > 1 else ""))
        print()
        return bool(results)
    except Exception as e:
        print(f"  ✗ Archive step failed: {e}")
        logging.getLogger(__name__).warning(f"archive reconcile failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Audit and reconstruct Obsidian livestream entries.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  ls-audit 515                        Reconstruct entry #515
  ls-audit 515 --yt-id dQw4w9WgXcQ   Override YouTube ID
  ls-audit 515 --tw-id 2345678901     Override Twitch ID
  ls-audit --refresh                  Refresh all caches
  ls-audit --refresh youtube          Refresh YouTube only
  ls-audit --inject URL               Inject video from URL
  ls-audit --inject --manual          Manual cache injection
  ls-audit --cache-info dQw4w9WgXcQ   Look up cached video
  ls-audit 515 --no-archive           Audit without touching the archive
  ls-audit --sweep [N]                Audit N recent entries (hourly timer)
  ls-audit 515 --archive              Reconcile with the archive only
  ls-audit 515 --archive --yes        ...filling blanks, asking nothing
  ls-audit --tw-ids                   List Twitch ids that are broadcast ids
  ls-audit --tw-ids --apply           ...and fix the Obsidian entries
        """,
    )
    parser.add_argument("index", nargs="?", type=int,
                        help="Entry index to audit")
    parser.add_argument("--yt-id", help="Override YouTube video ID")
    parser.add_argument("--tw-id", help="Override Twitch video ID")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Every check and every file, as it used to print")
    parser.add_argument("--show", action="store_true",
                        help="Print the reconstructed Obsidian line as well")
    parser.add_argument("--refresh", nargs="?", const="all",
                        choices=["all", "youtube", "twitch"],
                        help="Refresh VOD cache")
    parser.add_argument("--inject", nargs="?", const="__prompt__",
                        metavar="URL",
                        help="Inject video into cache")
    parser.add_argument("--manual", action="store_true",
                        help="Use manual input for --inject")
    parser.add_argument("--cache-info", metavar="ID",
                        help="Look up a video ID in the cache")
    parser.add_argument("--timings", action="store_true",
                        help="Write a timings sidecar for this entry")
    parser.add_argument("--merge-chat", action="store_true",
                        help="Merge this entry's chats into one tagged file")
    parser.add_argument("--give-up-chat", metavar="PLATFORM",
                        choices=("twitch", "youtube"),
                        help="this entry's chat on PLATFORM is unrecoverable: "
                             "stop offering to fix it and merge without it")
    parser.add_argument("--why", default="",
                        help="--give-up-chat: a note to your future self")
    parser.add_argument("--ref", default="youtube",
                        help="--merge-chat: reference timeline "
                             "(youtube | twitch | epoch ms)")
    parser.add_argument("--zero", action="append", metavar="PLATFORM=VALUE",
                        help="--merge-chat: define a source's zero "
                             "(epoch, ISO, or +/-seconds). Repeatable.")
    parser.add_argument("-o", "--output", metavar="PATH",
                        help="--merge-chat: output path")
    parser.add_argument("--dry-run", action="store_true",
                        help="--merge-chat: report without writing")
    parser.add_argument("--no-assets", action="store_true",
                        help="--merge-chat: skip fetching emote and badge "
                             "pictures")
    parser.add_argument("--archive", action="store_true",
                        help="Reconcile with the archive and do nothing else")
    parser.add_argument("--no-archive", action="store_true",
                        help="Audit without touching the archive")
    parser.add_argument("--sweep", nargs="?", const=5, type=int, metavar="N",
                        help="Audit the N most recent entries (default 5), "
                             "non-interactive. Intended for an hourly timer.")
    parser.add_argument("--yes", action="store_true",
                        help="--archive: accept new values, leave collisions "
                             "alone, ask nothing")
    parser.add_argument("--tw-ids", action="store_true",
                        help="Report Twitch ids that are broadcast ids rather "
                             "than videos, and what each should be")
    parser.add_argument("--apply", action="store_true",
                        help="--tw-ids: rewrite the Obsidian entries "
                             "(the archive is never touched)")

    args = parser.parse_args()
    config = ls_common.load_config()

    if args.tw_ids:
        cmd_tw_ids(config, apply_entries=args.apply)
        return
    if args.sweep is not None:
        cmd_sweep(config, args.sweep)
        return
    if args.refresh is not None:
        cmd_refresh(config, args.refresh)
        return
    if args.cache_info:
        cmd_cache_info(args.cache_info)
        return
    if args.inject is not None:
        url = None if args.manual or args.inject == "__prompt__" else args.inject
        cmd_inject(config, url)
        return
    if args.index is None:
        parser.print_help()
        return

    if args.give_up_chat:
        cmd_give_up_chat(args.index, args.give_up_chat, args.why)
        return

    if args.timings:
        cmd_timings(config, args.index, output=args.output,
                    dry_run=args.dry_run)
        return

    if args.merge_chat:
        ref = int(args.ref) if args.ref.lstrip("-").isdigit() else args.ref
        cmd_merge_chat(config, args.index, ref=ref, zeros=args.zero,
                       output=args.output, dry_run=args.dry_run,
                       assets=not args.no_assets)
        return

    if args.archive:
        if not ls_archive.enabled(config):
            print("  Archive is off — set archive_url and archive_token.")
            return
        cmd_archive(config, args.index, yt_id=args.yt_id, tw_id=args.tw_id,
                    interactive=not args.yes)
        return

    audit(config, args.index, yt_override=args.yt_id, tw_override=args.tw_id,
          push_archive=not args.no_archive, interactive=not args.yes,
          verbose=args.verbose, show_block=args.show)


if __name__ == "__main__":
    main()
