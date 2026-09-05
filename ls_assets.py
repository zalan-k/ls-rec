#!/usr/bin/env python3
"""ls_assets — the pictures a merged chat only names.

A merged chat says `:tenmaWow:` and `subscriber_12`. The pictures live on
Twitch's and YouTube's CDNs, and the archive serves files, not links: it is
offline-shaped on purpose, and an emote that 404s in three years is a message
nobody can read. So each one is fetched once into the media root beside the
videos, and a lookup records everything already tried — so a re-merge asks the
network about the three emotes that are new and nothing else.

Layout, relative to the archive's media root, which is also how the archive
addresses them (`/media/thumb/emotes/twitch/25.png`):

    emotes/index.json
    emotes/twitch/<id>.png            and .gif beside it when it animates
    emotes/youtube/<channel>/<hash>.png
    badges/index.json
    badges/twitch/<set>_<version>.png
    badges/youtube/<key>.png

Everything the renderer reads is a `.png` at a path it can derive from the
merged file alone, which is the whole reason for that rule: the merged file
names an emote and the renderer must be able to turn that name into a URL
without asking anything else. Twitch's animated emotes are fetched as both —
the still for today, the .gif sitting there for whenever the renderer wants it.

Credentials: Twitch EMOTES need none, because the id is the address. Twitch
BADGES do — their images live behind Helix, keyed by set and version — so
without twitch_client_id/secret the badges are skipped, loudly, and the emotes
still land. YouTube needs none for either: the URL was in the dump and ls_chat
kept it.
"""
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

import ls_archive
import ls_chat
import ls_common

# The lookup's own shape, so a later change can be told from a corrupt file.
INDEX_VERSION = 1

# How long a failure is believed. Twitch deletes emotes and YouTube's URLs
# expire, and neither is worth re-asking on every merge — but neither is
# permanent either: a channel that lost its affiliate status and got it back
# has its emotes again. A week is short enough to recover from and long enough
# that a sweep over two hundred entries does not become a sweep over two
# hundred 404s.
RETRY_AFTER_S = 7 * 86400

SLEEP_S = 0.04          # courtesy between downloads, not a rate limit
TIMEOUT_S = 20
MAX_BYTES = 8 * 1024 * 1024

TWITCH_EMOTE = "https://static-cdn.jtvnw.net/emoticons/v2/{id}/{fmt}/dark/3.0"

# Google's image hosts take a size on the end of the URL — `=w48-h48-c-k-nd` —
# and the dump offers 24 and 48. Cutting it off asks for the original, which is
# what a 4x screenshot needs and what the 48px one cannot be scaled up to.
_GOOGLE_IMG = ("ggpht.com", "googleusercontent.com")


def _full_size(url: str) -> str | None:
    """The same picture at whatever size it was uploaded, or None if this is
    not a URL that can be asked."""
    if "=" not in url or not any(h in url for h in _GOOGLE_IMG):
        return None
    bare = url.rsplit("=", 1)[0]
    return bare if bare and bare != url else None

# What a fetched file is allowed to be. An emote endpoint that answers with an
# HTML error page is not an emote, and writing it out under a .png would make a
# broken image that looks exactly like a real one on disk.
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
GIF_MAGIC = (b"GIF87a", b"GIF89a")


# ── the lookup ─────────────────────────────────────────────────────────────

def _index_path(root: str, tree: str) -> str:
    return os.path.join(root, tree, "index.json")


def load_index(root: str, tree: str) -> dict:
    """What has already been tried. A missing or unreadable one starts empty:
    the worst that costs is one wasted sweep, and refusing to run because a
    cache is unreadable would be worse."""
    try:
        with open(_index_path(root, tree), encoding="utf-8") as f:
            got = json.load(f)
        if got.get("version") == INDEX_VERSION and isinstance(got.get("entries"), dict):
            return got
    except (OSError, ValueError):
        pass
    return {"version": INDEX_VERSION, "updated_at": None, "entries": {}}


def save_index(root: str, tree: str, idx: dict) -> None:
    idx["updated_at"] = int(time.time())
    path = _index_path(root, tree)
    tmp = path + ".part"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(idx, f, ensure_ascii=False, indent=1, sort_keys=True)
        os.replace(tmp, path)
    except OSError as e:
        print(f"  ⚠ could not write {tree}/index.json ({e})")
        try:
            os.remove(tmp)
        except OSError:
            pass


def _fresh_failure(entry: dict | None, now: int) -> bool:
    return bool(entry and entry.get("gone") and now - (entry.get("at") or 0) < RETRY_AFTER_S)


# ── the network ────────────────────────────────────────────────────────────

def _get(url: str, headers: dict | None = None) -> tuple[str, object]:
    """('ok', body) | ('http', code) | ('net', reason).

    The three are kept apart because only one of them means what it looks
    like. A 404 is Twitch saying the emote is gone, and worth remembering for
    a week. A refused connection is this machine having no network, and
    remembering THAT as "gone" would write six hundred tombstones for emotes
    that are all still there — a wrong answer that then takes a week to expire.
    """
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
            body = resp.read(MAX_BYTES + 1)
            if len(body) > MAX_BYTES:
                return "http", 413
            return "ok", body
    except urllib.error.HTTPError as e:
        return "http", e.code
    except (urllib.error.URLError, OSError, ValueError) as e:
        return "net", str(e)


def _looks_like(body: bytes, kind: str) -> bool:
    if kind == "png":
        return body.startswith(PNG_MAGIC)
    return any(body.startswith(m) for m in GIF_MAGIC)


def _write(root: str, rel: str, body: bytes) -> bool:
    """Write one asset. `.part` then rename, so nothing ever serves half a
    picture — the same rule the merged chat's .gz follows."""
    dest = os.path.join(root, rel)
    tmp = dest + ".part"
    try:
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(tmp, "wb") as f:
            f.write(body)
        os.replace(tmp, dest)
        return True
    except OSError as e:
        print(f"  ⚠ could not write {rel} ({e})")
        try:
            os.remove(tmp)
        except OSError:
            pass
        return False


# ── Twitch badge definitions, which are the only thing needing credentials ──

def _helix_badges(config: dict) -> dict[str, str] | None:
    """`set/version` -> image url, global badges plus this channel's.

    None means "could not ask", which is different from an empty answer and is
    why badges are skipped rather than recorded as gone: a missing token is a
    configuration gap, and writing `gone` for six hundred badges because of one
    would take a week to expire.
    """
    token = ls_common.twitch_get_token(config)
    cid = config.get("twitch_client_id")
    if not (token and cid):
        return None
    headers = {"Client-ID": cid, "Authorization": f"Bearer {token}"}
    urls: dict[str, str] = {}
    endpoints = ["https://api.twitch.tv/helix/chat/badges/global"]
    if config.get("twitch_user_id"):
        endpoints.append("https://api.twitch.tv/helix/chat/badges"
                         f"?broadcaster_id={config['twitch_user_id']}")
    got_any = False
    for url in endpoints:
        state, payload = _get(url, headers)
        if state != "ok":
            continue
        try:
            data = json.loads(payload)
        except ValueError:
            continue
        got_any = True
        for badge in data.get("data") or []:
            for v in badge.get("versions") or []:
                key = f"{badge.get('set_id')}/{v.get('id')}"
                # 4x where it exists: the archive keeps the file once, and a
                # badge that has to be drawn at 2x on a retina row cannot be
                # scaled up afterwards.
                urls[key] = (v.get("image_url_4x") or v.get("image_url_2x")
                             or v.get("image_url_1x") or "")
    return urls if got_any else None


# ── what a record turns into on disk ───────────────────────────────────────

def _emote_targets(platform: str, rec: dict) -> tuple[str, str, list[tuple[str, str]]] | None:
    """(lookup key, path under the media root, [(url, kind), ...]) for one emote.

    The path is root-relative and includes the tree, because that is what the
    archive serves it as — `/media/thumb/emotes/twitch/25.png` — and what the
    lookup records. A path relative to the tree instead would put emotes and
    badges in the same two directories, which is a collision nobody would see
    until a badge and an emote shared an id.

    The urls are tried in order and the first that answers with the right kind
    of bytes wins.
    """
    eid = str(rec.get("id") or "")
    if not eid:
        return None
    if platform == "twitch":
        # `static` rather than `default`: default follows the emote's own
        # nature and hands back a GIF for the animated ones, and the renderer
        # gets to derive one path from the merged file or none at all.
        return (f"twitch/{eid}", f"emotes/twitch/{eid}.png",
                [(TWITCH_EMOTE.format(id=eid, fmt="static"), "png")])
    if platform == "youtube":
        # `<channel>/<hash>`, which is already a path, and the only address
        # this picture has anywhere once the raw dump is archived.
        url = rec.get("url")
        if not url or "/" not in eid:
            return None
        # Original first, the offered thumbnail second: if Google ever stops
        # honouring a bare URL this still fetches something rather than
        # recording the emote as gone.
        big = _full_size(url)
        return (f"youtube/{eid}", f"emotes/youtube/{eid}.png",
                ([(big, "png")] if big else []) + [(url, "png")])
    return None


def _badge_targets(platform: str, key: str, rec: dict,
                   helix: dict | None) -> tuple[str, str, list[tuple[str, str]]] | None:
    if platform == "twitch":
        if helix is None:
            return None
        url = helix.get(f"{rec.get('set')}/{rec.get('version')}")
        if not url:
            return (f"twitch/{key}", f"badges/twitch/{key}.png", [])
        return (f"twitch/{key}", f"badges/twitch/{key}.png", [(url, "png")])
    if platform == "youtube":
        # A built-in badge is one of three pictures YouTube draws on every
        # channel and has no address; the renderer draws those itself.
        if rec.get("icon") or not rec.get("url"):
            return None
        big = _full_size(rec["url"])
        return (f"youtube/{key}", f"badges/youtube/{key}.png",
                ([(big, "png")] if big else []) + [(rec["url"], "png")])
    return None


# ── the sweep ──────────────────────────────────────────────────────────────

def _fetch_one(root: str, rel: str, attempts: list[tuple[str, str]]) -> str:
    """'ok' | 'gone' | 'down'."""
    reachable = False
    for url, kind in attempts:
        state, payload = _get(url)
        time.sleep(SLEEP_S)
        if state == "net":
            continue
        reachable = True
        if state != "ok":
            continue
        # Checked by magic bytes rather than by Content-Type, because a CDN
        # error page arrives as 200 text/html often enough to matter and would
        # otherwise be written out as a picture nobody can see is wrong.
        if not _looks_like(payload, kind):
            continue
        if _write(root, rel, payload):
            return "ok"
    return "gone" if reachable else "down"


def harvest(config: dict, header: dict, *, force: bool = False) -> dict:
    """Fetch every picture this merged chat's header names, once.

    `header` is what ls_chat.read_header() returns. Returns counts, and prints
    a line per tree — this runs inside the post-stream pipeline, where silence
    and a stack trace are the two things nobody wants.
    """
    out = {"emotes_new": 0, "emotes_gone": 0, "badges_new": 0, "badges_gone": 0,
           "skipped": 0}
    root = ls_archive.media_root(config)
    if not root:
        print("  ⚠ assets: no media root — set archive_media_root or check "
              "that nas_path ends with archive_media_prefix")
        return out
    now = int(time.time())
    helix: dict | None = None
    helix_asked = False
    down = False

    for tree, source in (("emotes", header.get("emotes") or {}),
                         ("badges", header.get("badges") or {})):
        if down:
            break
        idx = load_index(root, tree)
        entries = idx["entries"]
        dirty = False
        for platform, records in source.items():
            if down:
                break
            for key, rec in (records or {}).items():
                if not isinstance(rec, dict):
                    continue        # a v1 file, where these were bare strings
                if tree == "badges" and platform == "twitch" and not helix_asked:
                    helix, helix_asked = _helix_badges(config), True
                    if helix is None:
                        print("  ⚠ assets: no Twitch credentials, so badge "
                              "images are skipped (emotes are not affected)")
                target = (_emote_targets(platform, rec) if tree == "emotes"
                          else _badge_targets(platform, key, rec, helix))
                if not target:
                    out["skipped"] += 1
                    continue
                lookup, rel, attempts = target
                have = entries.get(lookup)
                if not force:
                    # On disk AND in the lookup. Either alone is a claim rather
                    # than a fact: a lookup can outlive a wiped directory, and
                    # a file can outlive the merge that explains it.
                    if have and have.get("file") and os.path.exists(
                            os.path.join(root, have["file"])):
                        continue
                    if _fresh_failure(have, now):
                        continue
                got = _fetch_one(root, rel, attempts) if attempts else "gone"
                if got == "down":
                    # Not this emote's fault. Stop rather than tombstone the
                    # rest of the file for a network that is not there.
                    down = True
                    break
                dirty = True
                if got == "ok":
                    entries[lookup] = {"file": rel, "at": now}
                    out[f"{tree}_new"] += 1
                    if tree == "emotes" and platform == "twitch":
                        # The animated original, for a renderer that has not
                        # been written yet. Free to take now and impossible to
                        # take later if Twitch has dropped the emote by then.
                        gif = TWITCH_EMOTE.format(id=rec["id"], fmt="animated")
                        state, payload = _get(gif)
                        time.sleep(SLEEP_S)
                        if state == "ok" and _looks_like(payload, "gif"):
                            _write(root, rel[:-4] + ".gif", payload)
                            entries[lookup]["gif"] = rel[:-4] + ".gif"
                else:
                    tries = (have or {}).get("tries", 0) + 1
                    entries[lookup] = {"gone": True, "at": now, "tries": tries}
                    out[f"{tree}_gone"] += 1
        if dirty:
            save_index(root, tree, idx)

    if down:
        print("  ⚠ assets: nothing answered — no network from here. Nothing "
              "was recorded as missing; run `ls_assets.py <merged>` later.")
    say = []
    for tree in ("emotes", "badges"):
        if out[f"{tree}_new"] or out[f"{tree}_gone"]:
            say.append(f"{out[f'{tree}_new']} {tree}"
                       + (f" ({out[f'{tree}_gone']} unavailable)"
                          if out[f"{tree}_gone"] else ""))
    print("  assets    " + (", ".join(say) if say else "nothing new"))
    return out


def harvest_file(config: dict, path: str, *, force: bool = False) -> dict | None:
    """Harvest for one merged chat, by path."""
    header = ls_chat.read_header(path)
    if header is None:
        print(f"  ⚠ assets: {os.path.basename(path)} is not a merged chat")
        return None
    return harvest(config, header, force=force)


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(
        description="Fetch the emote and badge pictures a merged chat names.")
    ap.add_argument("files", nargs="+", help="merged chat json file(s)")
    ap.add_argument("--force", action="store_true",
                    help="re-fetch even what the lookup already answers for")
    args = ap.parse_args()
    config = ls_common.load_config()
    for path in args.files:
        print(f"\n{os.path.basename(path)}")
        harvest_file(config, path, force=args.force)


if __name__ == "__main__":
    main()
