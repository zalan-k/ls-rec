#!/usr/bin/env python3
"""Where one recording sits inside another, measured from the sound.

    delta = sync(local_path, archive_path, local_duration_s)

An archiver's re-upload of a stream is the same broadcast with an unknown
amount taken off the front. Nothing free says how much: the filename is
theirs, not ours, and a re-upload carries no chat of its own, so the ladder in
CHATCLOCK.md has no rung here. The audio is the only witness both files share.

THE STATISTIC IS `z` AND NOT `r`, which is the finding that file paid for:
coarser bins correlate better and locate worse, so anything tuned on peak
correlation tunes itself in the wrong direction. `z` is how far the peak
stands above every other lag, and it is what separates an alignment from a
coincidence. Believed at 5, the same threshold the chat bench settled on for
the same reason -- a degraded true fit and a forced false pair land in the
same range of `r`, and one threshold refuses both.

TWO PROBES, FAR APART, and that is where the error bar comes from. One probe
gives an offset and no way to know whether it is right. Two give an offset
AND their disagreement, which is a measurement of the thing that would
otherwise corrupt every timestamp downstream: a cut, an ad break, or encoder
drift between the two files. A constant `error_s` would be a guess; this is
not.

NOTHING HERE WRITES OR DOWNLOADS. It reads two files that already exist and
returns numbers. Fetching the re-upload is the caller's business, which keeps
the only outbound socket in the job where the rest of them are.
"""
from __future__ import annotations

import json
import math
import os
import subprocess

#  numpy is now a pipeline dependency, which it was not before. CHATCLOCK.md
#  counted it as a cost against a gain that was not needed; here it is the
#  difference between an answer and none, because no cheaper rung exists for a
#  re-upload. Loaded on use so that a recorder which has not been updated says
#  a sentence rather than failing to import the whole job module.
np = None


def _numpy():
    global np
    if np is None:
        import numpy
        np = numpy
    return np


#  16 kHz mono is plenty: what is being matched is the SHAPE of the loudness
#  over time, not the sound, so everything above a few kHz is weight without
#  information. Frames of 25 ms every 10 ms are the usual speech-analysis
#  pair, and 10 ms is finer than any consumer of this number can use.
SR = 16_000
FRAME = 400
HOP = 160
ENV_HZ = SR / HOP

#  Measured by the chat bench on one stream: true pairs 4.7-8.8, false pairs
#  2.5-3.6. Five refuses both a coincidence and a true fit too degraded to
#  trust, which is the behaviour wanted -- a wrong offset is worse than none,
#  because everything downstream is measured against it.
GOOD_Z = 5.0

#  How far apart the two probes sit, as a fraction of the local file. Far
#  enough that drift between them is visible; inside the file at both ends so
#  neither lands on a trimmed head or a cut-off tail.
PROBE_AT = (0.25, 0.75)
PROBE_S = 60.0

#  Beyond this the two probes are not measuring one constant offset, so there
#  is a cut or a drift between them and a single number would be a lie.
MAX_DRIFT_S = 2.0


def envelope(path: str, start_s: float | None = None,
             dur_s: float | None = None) -> "np.ndarray":
    """Short-time log energy, one value every 10 ms.

    LOG, because a stream's loud moments are tens of dB above its quiet ones
    and a linear envelope is then a record of where the loudest passage was
    rather than of the shape around it. The log turns a ratio into a distance,
    which is what correlation measures.
    """
    n = _numpy()
    cmd = ["ffmpeg", "-nostdin", "-v", "error"]
    #  BEFORE -i, so ffmpeg seeks instead of decoding up to the start. On a
    #  three-hour file that is the difference between instant and minutes.
    if start_s is not None:
        cmd += ["-ss", f"{max(0.0, float(start_s)):.3f}"]
    cmd += ["-i", path]
    if dur_s is not None:
        cmd += ["-t", f"{float(dur_s):.3f}"]
    cmd += ["-vn", "-ac", "1", "-ar", str(SR), "-f", "s16le", "-"]
    out = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         check=True).stdout
    x = n.frombuffer(out, dtype="<i2").astype(n.float32)
    if x.size < FRAME:
        return n.zeros(0, dtype=n.float32)
    #  Frames as a strided view rather than a copy: a three-hour file is 170M
    #  samples and the copy would be a gigabyte for no reason.
    count = 1 + (x.size - FRAME) // HOP
    frames = n.lib.stride_tricks.as_strided(
        x, shape=(count, FRAME), strides=(x.strides[0] * HOP, x.strides[0]))
    return n.log1p(n.einsum("ij,ij->i", frames, frames) / FRAME).astype(n.float32)


def locate(probe: "np.ndarray", ref: "np.ndarray") -> tuple[float, float]:
    """Where `probe` sits inside `ref`, in seconds, and how sure that is.

    NORMALISED PER WINDOW, which is the part that is easy to leave out and
    wrong to. A plain sliding dot product is largest where `ref` is loudest,
    so without dividing by each window's own spread the answer is "the noisiest
    minute of the upload" on every input.

    By FFT because the reference is the whole upload: a three-hour file is a
    million envelope samples and a direct correlation over every lag is an
    afternoon.
    """
    n = _numpy()
    m, N = probe.size, ref.size
    if m == 0 or N < m:
        return (float("nan"), 0.0)

    p = probe - probe.mean()
    ps = float(n.sqrt((p * p).sum()))
    if ps == 0:
        return (float("nan"), 0.0)
    p = p / ps

    #  Window means and spreads for every lag at once, from running sums.
    c = n.concatenate(([0.0], n.cumsum(ref, dtype=n.float64)))
    c2 = n.concatenate(([0.0], n.cumsum(ref.astype(n.float64) ** 2)))
    s = c[m:] - c[:-m]
    s2 = c2[m:] - c2[:-m]
    var = n.maximum(s2 - s * s / m, 0.0)
    rs = n.sqrt(var)

    size = 1 << int(N + m - 1).bit_length()
    dot = n.fft.irfft(n.fft.rfft(ref.astype(n.float64), size)
                      * n.fft.rfft(p[::-1].astype(n.float64), size), size)[m - 1:N]
    #  The probe is already mean-zero, so the window mean cancels out of the
    #  numerator and only the window's spread has to divide.
    corr = n.where(rs > 1e-9, dot / n.maximum(rs, 1e-9), 0.0)

    k = int(n.argmax(corr))
    peak = float(corr[k])
    #  z against every OTHER lag, so a peak is only impressive relative to the
    #  rest of the surface it stands on.
    mu, sd = float(corr.mean()), float(corr.std())
    z = 0.0 if sd <= 0 else (peak - mu) / sd
    return (k / ENV_HZ, z)


def sync(local_path: str, archive_path: str, local_dur_s: float,
         probe_s: float = PROBE_S) -> dict:
    """Measure `archive_t = local_t + delta`.

    `delta` is what the archive needs and the sign is the one that reads
    naturally: a re-upload with eight minutes trimmed off its head answers
    about -480, because a moment eight minutes into the local file is at the
    very start of the upload.
    """
    n = _numpy()
    ref = envelope(archive_path)
    if ref.size == 0:
        return {"ok": False, "why": "that upload has no sound to match against"}

    seen = []
    for frac in PROBE_AT:
        at = max(0.0, float(local_dur_s) * frac - probe_s / 2)
        pr = envelope(local_path, start_s=at, dur_s=probe_s)
        if pr.size < ENV_HZ * 5:
            return {"ok": False, "why": "the recording is too short to match on"}
        where, z = locate(pr, ref)
        if math.isnan(where):
            return {"ok": False, "why": "that part of the recording is silent"}
        seen.append({"local_s": round(at, 3), "archive_s": round(where, 3),
                     "delta_s": round(where - at, 3), "z": round(z, 2)})

    deltas = [s["delta_s"] for s in seen]
    drift = round(max(deltas) - min(deltas), 3)
    worst_z = min(s["z"] for s in seen)
    delta = round(float(n.median(deltas)), 3)

    out = {"ok": True, "delta_s": delta, "drift_s": drift,
           "z": round(worst_z, 2), "probes": seen,
           #  THE DISAGREEMENT IS THE ERROR BAR. Half the spread between two
           #  probes an hour apart is a measurement of how much this offset
           #  can be wrong; a constant here would have been a guess wearing a
           #  number's clothes.
           "error_s": round(max(drift / 2, 1 / ENV_HZ), 3)}

    good = [s for s in seen if s["z"] >= GOOD_Z]
    if not good:
        out.update(ok=False, why=(
            f"the sound does not match well enough to trust (z {worst_z:.1f}, "
            f"wanted {GOOD_Z:.0f}) — this may not be the same broadcast"))
    elif len(good) < len(seen):
        #  ONE END MATCHES AND THE OTHER DOES NOT, which is a different fact
        #  from a bad match and deserves its own sentence: the usual cause is
        #  an upload trimmed past the probe, so that stretch of the recording
        #  simply is not in it. An offset IS available here and it is handed
        #  back — unverified, because the second probe is what would have
        #  verified it, and the whole design is that one probe is a guess.
        out.update(ok=False, delta_s=good[0]["delta_s"], error_s=None, why=(
            "only one end of the recording is in that upload, so the offset "
            "below is a single measurement with nothing to check it against "
            "— the upload is probably trimmed past it, or ends early"))
    elif drift > MAX_DRIFT_S:
        #  Not a failure of the method: this is the method REPORTING the thing
        #  that would otherwise have corrupted every timestamp on the entry.
        out.update(ok=False, why=(
            f"the two ends disagree by {drift:.1f}s, so the upload is cut or "
            f"re-timed somewhere in the middle rather than simply trimmed"))
    return out


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Where does our recording sit inside theirs?")
    ap.add_argument("local")
    ap.add_argument("archive")
    ap.add_argument("--duration", type=float, default=None,
                    help="length of the local file in seconds; probed when omitted")
    a = ap.parse_args()
    dur = a.duration
    if dur is None:
        dur = float(json.loads(subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "json", a.local],
            stdout=subprocess.PIPE, check=True).stdout)["format"]["duration"])
    print(json.dumps(sync(a.local, a.archive, dur), indent=1))
