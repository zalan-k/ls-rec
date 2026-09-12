#!/usr/bin/env python3
"""
resplit.py - recover an interrupted yt-dlp android_vr/SABR .part file.

Such a .part is a raw concatenation of many self-contained MP4 segments
(each ftyp+moov+mdat), which ffmpeg can't stream-copy directly ("Found
duplicated MOOV Atom"). This walks the top-level ISO-BMFF boxes, splits the
file back into its segments, and writes a concat list ffmpeg can re-stitch.

Usage:
    python resplit.py INPUT.part inspect            # show box structure, do nothing
    python resplit.py INPUT.part split  segs        # write segs/ + segs/list.txt
then:
    ffmpeg -f concat -safe 0 -i segs/list.txt -c copy -movflags +faststart video_recovered.mp4
"""
import struct, sys, os
from collections import Counter

src    = sys.argv[1]
mode   = sys.argv[2] if len(sys.argv) > 2 else "split"
outdir = sys.argv[3] if len(sys.argv) > 3 else "segs"
total  = os.path.getsize(src)

# --- walk top-level boxes; a truncated final box is dropped cleanly ---
boxes = []
with open(src, "rb") as f:
    pos = 0
    while pos + 8 <= total:
        f.seek(pos); hdr = f.read(8)
        if len(hdr) < 8:
            break
        sz, typ = struct.unpack(">I", hdr[:4])[0], hdr[4:8]
        if sz == 1:                                  # 64-bit largesize
            ext = f.read(8)
            if len(ext) < 8:
                break
            sz = struct.unpack(">Q", ext)[0]
        elif sz == 0:                                # extends to EOF
            sz = total - pos
        if sz < 8 or pos + sz > total:               # incomplete tail -> stop
            break
        boxes.append((pos, sz, typ.decode("latin1")))
        pos += sz

if mode == "inspect":
    seq = [t for _, _, t in boxes]
    print("total boxes:", len(seq))
    print("first 24  :", " ".join(seq[:24]))
    print("counts    :", dict(Counter(seq)))
    sys.exit(0)

# --- group boxes into segments: a new segment begins at ftyp/moov/styp
#     once the current one already holds BOTH a moov and an mdat ---
segs, cur, seen = [], [], set()
BND = {"ftyp", "moov", "styp"}
for (p, s, t) in boxes:
    if t in BND and {"moov", "mdat"} <= seen:
        segs.append(cur); cur, seen = [], set()
    cur.append((p, s, t)); seen.add(t)
if {"moov", "mdat"} <= seen:
    segs.append(cur)

if not segs:
    sys.exit("No complete segments found - is this the right file?")

os.makedirs(outdir, exist_ok=True)
listp = os.path.join(outdir, "list.txt")
with open(src, "rb") as f, open(listp, "w") as lst:
    for i, seg in enumerate(segs):
        a, b = seg[0][0], seg[-1][0] + seg[-1][1]
        f.seek(a)
        with open(os.path.join(outdir, f"s{i:05d}.mp4"), "wb") as o:
            o.write(f.read(b - a))
        lst.write("file '%s'\n" % os.path.abspath(
            os.path.join(outdir, f"s{i:05d}.mp4")).replace(os.sep, "/"))

print(f"segments={len(segs)}  list={listp}")
print("next: ffmpeg -f concat -safe 0 -i "
      f"{listp} -c copy -movflags +faststart video_recovered.mp4")
