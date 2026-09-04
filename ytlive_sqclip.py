#!/usr/bin/env python3
"""
ytlive_sqclip.py -- pull an arbitrary time range out of an ONGOING YouTube
livestream, including streams with DVR disabled.

Why this exists
---------------
Because fuck you for charging $5/mo ClipsCutter.
yt-dlp's --download-sections is implemented only in FFmpegFD, whose
SUPPORTED_PROTOCOLS omits 'http_dash_segments_generator' -- the protocol the
youtube extractor assigns to live formats under --live-from-start. The
combination aborts with "This format cannot be partially downloaded".

The underlying capability is fine. YouTube live DASH segments are addressed by
a monotonic &sq=N on the googlevideo base URL and stay fetchable for ~120h
regardless of the DVR toggle -- DVR is a player-side seek-bar restriction, not
a CDN retention policy. This script uses yt-dlp purely as an extractor (base
URL, signature, PO token) and does its own sq math.

Three things this gets right that are easy to get wrong
-------------------------------------------------------
1. SEGMENT DURATION IS MEASURED, NOT ASSUMED. Streams run 1s, 2s, or 5s
   segments. Guessing 5s on a 1s stream puts you 5x too deep into the stream
   with no error -- you just silently get the wrong content.

2. NO sq=0 PREPEND. YouTube live segments are self-initializing (each carries
   its own ftyp/moov as well as moof/mdat), so sq=0 is unnecessary. Including
   it injects stream-opening content and a huge timestamp gap.

3. BYTE CONCAT + make_zero, NOT THE CONCAT DEMUXER. ffmpeg skips the duplicate
   moov of each segment but still reads every moof, so plain byte
   concatenation works. The concat demuxer does NOT: it reads each segment's
   absolute end timestamp as that file's duration and accumulates them, so a
   7-segment clip comes out hours long with the content scattered.

Usage
-----
    ./ytlive_sqclip.py URL --probe
    ./ytlive_sqclip.py URL --start 2:48:00 --end 2:48:30 -o clip.mp4
    ./ytlive_sqclip.py URL --last 10m -o clip.mp4
    ./ytlive_sqclip.py URL -F

--start/--end are offsets from the START OF THE STREAM. --last is measured
back from the live edge.
"""

import argparse, re, subprocess, sys, tempfile
from pathlib import Path

try:
    from yt_dlp import YoutubeDL
    from yt_dlp.networking import HEADRequest
except ImportError:
    sys.exit("pip install yt-dlp")


def hms(seconds):
    s = int(seconds)
    return f"{s // 3600}:{(s % 3600) // 60:02d}:{s % 60:02d}"

def parse_duration(s):
    """Accept 90, 1:30, 1:02:03, 10m, 90s, 2h."""
    s = str(s).strip()
    if m := re.fullmatch(r"(\d+(?:\.\d+)?)\s*([hms])", s, re.I):
        n, unit = float(m.group(1)), m.group(2).lower()
        return n * {"h": 3600, "m": 60, "s": 1}[unit]
    parts = s.split(":")
    if not all(p.replace(".", "").isdigit() for p in parts):
        raise argparse.ArgumentTypeError(f"cannot parse time: {s!r}")
    total = 0.0
    for p in parts:
        total = total * 60 + float(p)
    return total


def ffprobe_duration(path):
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", str(path)],
            capture_output=True, text=True, check=True)
        return float(out.stdout.strip())
    except (subprocess.CalledProcessError, ValueError, FileNotFoundError):
        return None


def seg_url(base_url, sq):
    return f"{base_url}{'&' if '?' in base_url else '?'}sq={sq}"


def list_formats(url):
    opts = {"quiet": True, "no_warnings": True, "live_from_start": True}
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False, process=False)
        info = ydl.process_ie_result(info, download=False)

    print(f"{'id':<14} {'res':<12} {'codec':<14} {'target':<8} {'sq?':<5} protocol")
    print("-" * 80)
    for f in info.get("formats", []):
        ok = "yes" if callable(f.get("fragments")) else "-"
        res = f.get("resolution") or f.get("format_note") or ""
        codec = (f.get("vcodec") if f.get("vcodec", "none") != "none"
                 else f.get("acodec")) or ""
        tgt = f.get("target_duration")
        print(f"{f.get('format_id',''):<14} {res:<12} {codec[:13]:<14} "
              f"{str(tgt or '-'):<8} {ok:<5} {f.get('protocol','')}")
    print("\nOnly rows marked 'yes' can be clipped by sq range.")
    print("An empty 'target' column is why segment duration must be measured.")


def extract_streams(url, video_fmt, audio_fmt):
    """
    Return [(kind, base_url, target_duration_or_None), ...].

    With live_from_start the format's 'fragments' key is a generator factory.
    Calling it and taking only the FIRST yield gives sq=0's URL immediately --
    no download, no waiting -- from which we strip sq to recover the base URL.
    """
    opts = {
        "quiet": True,
        "no_warnings": True,
        "live_from_start": True,
        "format": f"{video_fmt}+{audio_fmt}",
    }
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)

    if not info.get("is_live"):
        sys.exit(
            "Stream is not live. Once it ends the protocol becomes plain\n"
            "http_dash_segments and stock yt-dlp handles it directly:\n"
            "  yt-dlp --live-from-start --download-sections '*START-END' URL")

    out = []
    for f in (info.get("requested_formats") or [info]):
        gen = f.get("fragments")
        if not callable(gen):
            sys.exit(
                f"format {f.get('format_id')} ({f.get('protocol')}) has no\n"
                "fragment generator and cannot be sq-addressed. Use bestvideo\n"
                "(not best) -- only adaptive video-only/audio-only DASH formats\n"
                "get is_from_start. Run with -F to see what qualifies.")
        try:
            first = next(iter(gen({})))
        except StopIteration:
            sys.exit(f"format {f.get('format_id')} produced no fragments")
        base = re.sub(r"[?&]sq=\d+", "", first["url"])
        kind = "video" if f.get("vcodec", "none") != "none" else "audio"
        tgt = f.get("target_duration")
        out.append((kind, base, float(tgt) if tgt else None))
    return out


def head_seqnum(ydl, base_url):
    """Current tip. The bare base URL returns an empty body but real headers."""
    resp = ydl.urlopen(HEADRequest(base_url))
    val = resp.headers.get("X-Head-Seqnum")
    resp.close()
    if val is None:
        sys.exit("No X-Head-Seqnum header -- base URL may have expired.")
    return int(val)


def _probe_sq(ydl, base_url, sq, workdir, label):
    """Fetch one segment and return whatever ffprobe calls its duration."""
    dest = workdir / f"probe_{label}_{sq}.mp4"
    resp = ydl.urlopen(seg_url(base_url, sq))
    dest.write_bytes(resp.read())
    resp.close()
    val = ffprobe_duration(dest)
    dest.unlink(missing_ok=True)
    if val is None:
        sys.exit(f"ffprobe failed on {label} sq={sq} (is ffprobe installed?)")
    return val


def measure_segment_duration(ydl, base_url, tip, workdir, label, gap=20):
    """
    Determine seconds-per-segment by DIFFERENTIAL probing.

    ffprobe on a single live segment does not report its media duration. The
    fragment's timestamps are absolute from stream start and the container
    declares start_time 0, so ffprobe reports the segment's absolute END:
    roughly (sq + 1) * seg_dur. Probe sq=11736 on a 1s stream and you get
    11737, not 1 -- a number that looks like a plausible duration and is off
    by four orders of magnitude.

    Probing two segments a known distance apart cancels the offset:
        probe(b) - probe(a) == (b - a) * seg_dur
    """
    b = max(1, tip - 5)
    a = max(0, b - gap)
    if a == b:
        sys.exit("stream too short to measure segment duration")
    da = _probe_sq(ydl, base_url, a, workdir, label)
    db = _probe_sq(ydl, base_url, b, workdir, label)
    dur = (db - da) / (b - a)
    if not 0.1 <= dur <= 30.0:
        sys.exit(
            f"implausible {label} segment duration {dur:.3f}s from probes "
            f"sq={a}:{da:.3f} sq={b}:{db:.3f}.\n"
            "Pass --segment-duration to override.")
    return dur, (a, da, b, db)


def fetch_concat(ydl, base_url, sq_start, sq_end, dest, label):
    """
    Byte-concatenate the sq range into one file.

    Each segment is self-contained; ffmpeg skips the duplicate moov of each
    but reads every moof, so this produces a continuous stream. Timestamps
    stay absolute -- the caller rebases them with -avoid_negative_ts make_zero.
    """
    total = sq_end - sq_start + 1
    got = 0
    with open(dest, "wb") as fh:
        for i, sq in enumerate(range(sq_start, sq_end + 1), 1):
            try:
                resp = ydl.urlopen(seg_url(base_url, sq))
                fh.write(resp.read())
                resp.close()
                got += 1
            except Exception as exc:
                print(f"  sq={sq} failed: {exc}", file=sys.stderr)
            if i % 50 == 0 or i == total:
                print(f"  {label}: {i}/{total} segments", file=sys.stderr)
    if got == 0:
        sys.exit(f"no {label} segments fetched")
    return dest


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("--start", type=parse_duration, help="offset from stream start")
    ap.add_argument("--end", type=parse_duration, help="offset from stream start")
    ap.add_argument("--last", type=parse_duration, help="duration back from live edge")
    ap.add_argument("--probe", action="store_true", help="report tip and timing, then exit")
    ap.add_argument("-o", "--output", default="clip.mp4")
    # bestVIDEO, not best: only adaptive formats carry targetDurationSec and
    # therefore only they get is_from_start and a fragment generator.
    ap.add_argument("--video-format", default="bestvideo[height<=1080]")
    ap.add_argument("--audio-format", default="bestaudio")
    ap.add_argument("-F", "--list-formats", action="store_true",
                    help="show which formats support sq addressing, then exit")
    ap.add_argument("--segment-duration", type=float,
                    help="override measured segment duration (seconds)")
    args = ap.parse_args()

    if args.list_formats:
        list_formats(args.url)
        return

    streams = extract_streams(args.url, args.video_format, args.audio_format)

    with YoutubeDL({"quiet": True, "no_warnings": True}) as ydl, \
            tempfile.TemporaryDirectory() as tmp:
        tmpdir = Path(tmp)
        vkind, vbase, vtarget = streams[0]
        tip = head_seqnum(ydl, vbase)

        if args.segment_duration:
            dur, raw = args.segment_duration, None
            source = "override"
        else:
            dur, raw = measure_segment_duration(ydl, vbase, tip, tmpdir, vkind)
            source = "measured"

        if vtarget and abs(vtarget - dur) > 0.1:
            print(f"note: target_duration says {vtarget}s but segments measure "
                  f"{dur:.3f}s; trusting the measurement", file=sys.stderr)

        if args.probe:
            print(f"segment duration : {dur:.3f}s ({source})")
            if raw:
                a, da, b, db = raw
                print(f"  from probes    : sq={a} -> {da:.3f}, sq={b} -> {db:.3f}")
                print(f"  differential   : ({db:.3f} - {da:.3f}) / {b - a} = {dur:.3f}")
                print("  (a single probe returns absolute END time, not duration)")
            if vtarget:
                print(f"target_duration  : {vtarget}s")
            else:
                print("target_duration  : absent (this is why we measure)")
            print(f"current tip      : sq={tip}")
            print(f"implied elapsed  : {hms(tip * dur)}")
            print("                   ^ compare against the YouTube player clock;")
            print("                     a mismatch means the duration is wrong")
            print(f"reachable window : sq=0..{tip}")
            for kind, base, _ in streams[1:]:
                adur, _ = measure_segment_duration(ydl, base, tip, tmpdir, kind)
                flag = "" if abs(adur - dur) < 0.1 else "   <-- DIFFERS from video"
                print(f"{kind + ' segment':<17}: {adur:.3f}s{flag}")
            return

        if args.last is not None:
            sq_end, sq_start = tip, max(0, tip - int(args.last / dur))
        elif args.start is not None:
            sq_start = int(args.start / dur)
            sq_end = int(args.end / dur) if args.end else tip
        else:
            sys.exit("need --start/--end, or --last, or --probe")

        sq_end = min(sq_end, tip)
        if sq_start > sq_end:
            sys.exit(f"empty range: sq {sq_start}..{sq_end} (tip is {tip})")
        # A requested span of many seconds that resolves to a single segment
        # means the duration is wrong -- the failure mode that silently
        # produced 1-second clips from 30-second requests.
        requested = (args.last if args.last is not None
                     else (args.end - args.start if args.end else None))
        if requested and requested > 2 * dur and sq_end == sq_start:
            sys.exit(
                f"requested {requested:.0f}s but that resolves to a single "
                f"segment at {dur:.3f}s/segment.\nThe segment duration is "
                "almost certainly wrong. Run --probe, or pass "
                "--segment-duration.")
        print(f"sq {sq_start}..{sq_end} of {tip} at {dur:.3f}s/segment "
              f"= {hms(sq_start * dur)} to {hms((sq_end + 1) * dur)} "
              f"(~{(sq_end - sq_start + 1) * dur:.0f}s)", file=sys.stderr)

        parts = []
        for kind, base, _ in streams:
            parts.append(fetch_concat(ydl, base, sq_start, sq_end,
                                      tmpdir / f"{kind}.bin", kind))

        cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "warning"]
        for p in parts:
            cmd += ["-i", str(p)]
        for idx, (kind, _, _) in enumerate(streams):
            cmd += ["-map", f"{idx}:{'v' if kind == 'video' else 'a'}:0"]
        # Segment timestamps are absolute from stream start; make_zero rebases
        # the clip to begin at t=0 instead of t=sq*duration.
        cmd += ["-c", "copy", "-avoid_negative_ts", "make_zero",
                "-movflags", "+faststart", args.output]
        subprocess.run(cmd, check=True)

    out_dur = ffprobe_duration(args.output)
    print(f"wrote {args.output}" + (f" ({out_dur:.1f}s)" if out_dur else ""),
          file=sys.stderr)

if __name__ == "__main__":
    main()
