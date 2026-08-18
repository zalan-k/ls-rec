#!/usr/bin/env python3
"""
ls_chat.py - Convert chat captures to one schema, and merge them.

Formats in:
    irc_capture        ls_common.record_twitch_chat  (JSON array, us offsets)
    twitch_downloader  TwitchDownloaderCLI chatdownload
    ytdlp              yt-dlp live_chat, live and post-hoc

Every message gets `ts` (ms from the source's own zero) and, where the format
carries one, `abs_ms` (epoch ms). Merging is then a subtraction. Inputs are
never modified.

    python ls_chat.py convert FILE [-o OUT] [--stats]
    python ls_chat.py merge YT.json TW.json [-o OUT] [--ref youtube|twitch]
                                            [--zero twitch=2026-03-01T12:02:30]
"""

import argparse
import datetime
import json
import os
import re
import statistics
import sys
from dataclasses import dataclass
from typing import Optional

PROBE_BYTES = 16384
ZERO_SAMPLES = 200

# Renderers that are actual chat events. liveChatViewerEngagementMessageRenderer
# ("Live chat replay is on") heads every post-hoc download and is stamped with
# the DOWNLOAD time, so it must never contribute to a zero.
YT_MESSAGE_RENDERERS = (
    "liveChatTextMessageRenderer",
    "liveChatPaidMessageRenderer",
    "liveChatPaidStickerRenderer",
    "liveChatMembershipItemRenderer",
    "liveChatSponsorshipsGiftPurchaseAnnouncementRenderer",
    "liveChatSponsorshipsGiftRedemptionAnnouncementRenderer",
)

# Derived artifacts, not captures. ls_audit.scan_nas should skip these.
DERIVED_SUFFIXES = (".chat.json", ".posthoc.json", ".unified.json",
                    ".live.json", ".merged.json", ".merging", ".archive",
                    ".timings.json", "merged-chat.json")


def is_derived(filename: str) -> bool:
    low = filename.lower()
    return any(low.endswith(s) for s in DERIVED_SUFFIXES)


# ── schema ────────────────────────────────────────────────────────────────

@dataclass
class Msg:
    type: str                       # chat superchat sub resub gift raid pinned ban
    ts: int                         # ms from source zero
    abs_ms: Optional[int] = None    # epoch ms
    id: Optional[str] = None
    author: Optional[dict] = None   # {id, name, color?}
    badges: Optional[list] = None
    text: Optional[str] = None
    deleted: bool = False            # explicitly removed by a mod
    purged: bool = False             # author banned later, history struck
    amount: Optional[float] = None
    currency: Optional[str] = None
    hearted: bool = False
    tier: Optional[int] = None
    months: Optional[int] = None
    count: Optional[int] = None
    recipient: Optional[dict] = None
    viewers: Optional[int] = None
    pinned_by: Optional[str] = None
    user: Optional[dict] = None
    duration: Optional[int] = None


_OPTIONAL = ("id", "author", "badges", "text", "amount", "currency", "tier",
             "months", "count", "recipient", "viewers", "pinned_by", "user",
             "duration")


def serialize(m: Msg, origin: str = "") -> dict:
    d = {"origin": origin} if origin else {}
    d["type"] = m.type
    d["ts"] = m.ts
    if m.abs_ms is not None:
        d["abs_ms"] = m.abs_ms
    for k in _OPTIONAL:
        v = getattr(m, k)
        if v is not None and v != "" and v != []:
            d[k] = v
    if m.deleted:
        d["deleted"] = True
    if m.purged:
        d["purged"] = True
    if m.hearted:
        d["hearted"] = True
    return d


def emote_token(name: str) -> str:
    return f":{name}:"


def parse_tier(v) -> int:
    s = str(v or "").lower()
    if "3" in s or "three" in s:
        return 3
    if "2" in s or "two" in s:
        return 2
    return 1


def parse_amount(text: str) -> tuple[float, str]:
    """'$1,234.56' and '1.234,56 EUR' both parse. Decimal sep is whichever
    of . or , comes last."""
    if not text:
        return 0.0, ""
    m = re.search(r"[\d][\d.,\s\u00a0]*", text)
    if not m:
        return 0.0, text.strip()
    num = m.group().strip().replace(" ", "").replace("\u00a0", "")
    currency = text.replace(m.group(), "").strip()
    if num.rfind(",") > num.rfind("."):
        num = num.replace(".", "").replace(",", ".")
    else:
        num = num.replace(",", "")
    try:
        return float(num), currency
    except ValueError:
        return 0.0, currency


def parse_zero(value, ref: Optional[int] = None) -> int:
    """
    Parse a manually supplied zero into epoch ms. Accepted forms:

        1772366550000           epoch ms
        1772366550              epoch seconds (< 1e12 is read as seconds)
        2026-03-01T12:02:30     ISO 8601, local time unless offset given
        +150 / -8.3             seconds relative to the merge reference
    """
    v = str(value).strip()
    if not v:
        raise ValueError("empty zero value")

    if v[0] in "+-":
        if ref is None:
            raise ValueError(f"relative zero '{v}' needs a reference "
                             f"(not available for a single file)")
        return ref + int(round(float(v) * 1000))

    if re.fullmatch(r"\d+", v):
        n = int(v)
        return n if n >= 10 ** 12 else n * 1000

    try:
        dt = datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"unparseable zero '{v}' "
                         f"(want epoch, ISO datetime, or +/-seconds)") from None
    return int(dt.timestamp() * 1000)


def parse_zero_args(values: Optional[list]) -> dict:
    """`platform=value` pairs into {platform: raw_value}."""
    out = {}
    for v in values or []:
        if "=" not in v:
            raise ValueError(f"bad --zero '{v}', want platform=value")
        plat, raw = v.split("=", 1)
        plat = plat.strip().lower()
        if plat not in ("youtube", "twitch"):
            raise ValueError(f"unknown platform '{plat}' in --zero")
        out[plat] = raw.strip()
    return out


# ── format detection ──────────────────────────────────────────────────────

IRC, TDC, YTDLP, UNKNOWN = "irc_capture", "twitch_downloader", "ytdlp", "unknown"


def detect_format(path: str) -> str:
    """Bounded probe: YT dumps run to hundreds of MB and TDC writes one
    minified line, so neither json.load nor readline is safe here."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            probe = f.read(PROBE_BYTES)
    except OSError:
        return UNKNOWN
    head = probe.lstrip()
    if not head:
        return UNKNOWN
    # FileInfo only exists on newer TDC builds, so key off the payload.
    if head[0] == "{" and '"comments"' in probe and (
            '"streamer"' in probe or '"video"' in probe):
        return TDC
    if '"replayChatItemAction"' in probe or '"clickTrackingParams"' in probe:
        return YTDLP
    if '"message_type"' in probe or '"action_type"' in probe:
        return IRC
    return UNKNOWN


def peek_zero(path: str, max_lines: int = 20000) -> tuple[Optional[int], str]:
    """
    Derive a capture's zero from the head of the file, without parsing it all.

    Returns (epoch_ms, source). The source says what the zero *means*:
        yt:timestampUsec   broadcast start (videoOffsetTimeMsec is video-relative)
        tdc:created_at     broadcast start (VOD start)
        irc:tmi_sent_ts    record start (offsets are relative to the recorder)
    """
    fmt = detect_format(path)

    if fmt == TDC:
        # created_at sits in the header, ahead of the comments array
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                head = f.read(PROBE_BYTES * 4)
        except OSError:
            return None, "none"
        m = re.search(r'"created_at"\s*:\s*"([^"]+)"', head)
        if m:
            try:
                dt = datetime.datetime.fromisoformat(
                    m.group(1).replace("Z", "+00:00"))
                return int(dt.timestamp() * 1000), "tdc:created_at"
            except ValueError:
                pass
        return None, "none"

    if fmt not in (IRC, YTDLP):
        return None, "none"

    deltas: list[int] = []
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if i >= max_lines or len(deltas) >= ZERO_SAMPLES:
                    break
                line = line.strip().rstrip(",")
                if not line or line in ("[", "]"):
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if fmt == IRC:
                    a = _irc_abs(obj)
                    if a is not None:
                        deltas.append(a - _irc_ts(obj))
                else:
                    ts = YtdlpConverter._ts(obj)
                    # Post-hoc clamps videoOffsetTimeMsec to 0 for everything
                    # before the stream started, so an offset of 0 yields the
                    # message's own send time as the zero -- hours out.
                    if ts == 0:
                        continue
                    for act in (obj.get("replayChatItemAction") or {}).get("actions") or []:
                        item = (act.get("addChatItemAction") or {}).get("item") or {}
                        for key, r in item.items():
                            if key not in YT_MESSAGE_RENDERERS or not isinstance(r, dict):
                                continue
                            if r.get("timestampUsec"):
                                try:
                                    deltas.append(int(r["timestampUsec"]) // 1000 - ts)
                                except (TypeError, ValueError):
                                    pass
    except OSError:
        return None, "none"

    if not deltas:
        return None, "none"
    return (int(statistics.median(deltas)),
            "irc:tmi_sent_ts" if fmt == IRC else "yt:timestampUsec")


# ── base ──────────────────────────────────────────────────────────────────

class Converter:
    platform = "unknown"
    fmt = UNKNOWN

    def __init__(self):
        self.messages: list[Msg] = []
        self.emotes: dict[str, str] = {}
        self.badges: dict[str, str] = {}
        self.deletions: dict[str, int] = {}
        self.bans: dict[str, int] = {}
        self.zero_ms: Optional[int] = None
        self.zero_src = "none"
        self.skipped = 0
        self.meta: dict = {}

    def convert(self, path: str):
        raise NotImplementedError

    def finalize(self):
        if self.zero_ms is not None:
            for m in self.messages:
                if m.abs_ms is None:
                    m.abs_ms = self.zero_ms + m.ts
        for m in self.messages:
            if m.id and m.id in self.deletions:
                m.deleted = True
            aid = (m.author or {}).get("id")
            # Both platforms strike the author's whole prior history on a ban.
            # Flagged separately from a mod deleting one message; nothing is
            # dropped either way.
            if aid and aid in self.bans and m.ts <= self.bans[aid]:
                m.purged = True
        self.messages.sort(key=lambda m: m.ts)

    def apply_zero(self, zero_ms: int, src: str = "manual"):
        """Set the zero and fill abs_ms wherever the format did not carry one.
        Messages that already have an absolute time keep it."""
        self.zero_ms = zero_ms
        self.zero_src = src
        for m in self.messages:
            if m.abs_ms is None:
                m.abs_ms = zero_ms + m.ts

    def result(self) -> dict:
        return {
            "metadata": {
                "platform": self.platform,
                "format": self.fmt,
                "zero_ms": self.zero_ms,
                "zero_source": self.zero_src,
                "messages": len(self.messages),
                "skipped_lines": self.skipped,
                **self.meta,
                "emotes": self.emotes,
                "badges": self.badges,
            },
            "messages": [serialize(m) for m in self.messages],
        }


# ── irc capture (ls_common.record_twitch_chat) ────────────────────────────

def _irc_ts(item: dict) -> int:
    """Recorder writes microseconds; schema is milliseconds."""
    try:
        return int(item.get("timestamp", 0)) // 1000
    except (TypeError, ValueError):
        return 0


def _irc_abs(item: dict) -> Optional[int]:
    v = item.get("tmi_sent_ts")
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


class IrcConverter(Converter):
    platform, fmt = "twitch", IRC

    def convert(self, path):
        items = self._load(path)
        bombs = set()
        for it in items:
            mt = it.get("message_type")
            if mt == "delete_message" and it.get("target_message_id"):
                self.deletions[it["target_message_id"]] = _irc_ts(it)
            elif mt == "ban_user":
                tid = (it.get("author") or {}).get("target_id")
                if tid:
                    self.bans[tid] = _irc_ts(it)
            elif mt == "mystery_subscription_gift":
                o = it.get("origin_id") or it.get("msg_param_community_gift_id")
                if o:
                    bombs.add(o)
        for it in items:
            m = self._one(it, bombs)
            if m:
                self.messages.append(m)
        self._derive_zero(items)

    def _load(self, path) -> list[dict]:
        """Line-based so a capture truncated by a crash still parses."""
        out = []
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip().rstrip(",")
                if not line or line in ("[", "]"):
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    self.skipped += 1
        return out

    def _derive_zero(self, items):
        deltas = []
        for it in items:
            a = _irc_abs(it)
            if a is None:
                continue
            deltas.append(a - _irc_ts(it))
            if len(deltas) >= ZERO_SAMPLES:
                break
        if deltas:
            self.zero_ms = int(statistics.median(deltas))
            self.zero_src = "tmi_sent_ts"

    def _author(self, it) -> dict:
        a = it.get("author") or {}
        d = {"id": str(a.get("id", "")),
             "name": a.get("display_name") or a.get("name") or ""}
        c = it.get("colour") or it.get("color")
        if c:
            d["color"] = c
        return d

    def _badges(self, it) -> list:
        out = []
        for b in (it.get("author") or {}).get("badges") or []:
            key = f"{b.get('name', '')}_{b.get('version', '1')}"
            out.append(key)
            self.badges[key] = b.get("title") or b.get("name", "")
        return out

    def _text(self, text, emotes) -> str:
        """Emote `locations` are byte offsets into the UTF-8 encoding (see
        ls_common._parse_irc_message), so splice on bytes, not codepoints."""
        if not text or not emotes:
            return text or ""
        raw = text.encode("utf-8")
        spans = []
        for e in emotes:
            for loc in e.get("locations") or []:
                m = re.match(r"(\d+)-(\d+)", str(loc))
                if m:
                    spans.append((int(m.group(1)), int(m.group(2)) + 1,
                                  e.get("name", ""), e.get("id", "")))
        for start, end, name, eid in sorted(spans, key=lambda x: -x[0]):
            if start > len(raw):
                continue
            tok = emote_token(name)
            raw = raw[:start] + tok.encode("utf-8") + raw[end:]
            self.emotes[tok] = eid
        return raw.decode("utf-8", errors="replace")

    def _one(self, it, bombs) -> Optional[Msg]:
        mt = it.get("message_type")
        base = dict(ts=_irc_ts(it), abs_ms=_irc_abs(it), id=it.get("message_id"))

        if mt == "text_message":
            txt = self._text(it.get("message", ""), it.get("emotes"))
            kw = dict(author=self._author(it), badges=self._badges(it) or None,
                      text=txt)
            bits = it.get("bits") or 0
            if bits:
                return Msg(type="superchat", **base, **kw,
                           amount=float(bits), currency="bits")
            return Msg(type="chat", **base, **kw)

        if mt in ("subscription", "resubscription"):
            txt = self._text(it.get("message") or "", it.get("emotes"))
            return Msg(type="sub" if mt == "subscription" else "resub", **base,
                       author=self._author(it),
                       badges=self._badges(it) or None, text=txt or None,
                       tier=parse_tier(it.get("subscription_type")),
                       months=(it.get("cumulative_months")
                               if mt == "resubscription" else None))

        if mt == "mystery_subscription_gift":
            return Msg(type="gift", **base, author=self._author(it),
                       badges=self._badges(it) or None,
                       tier=parse_tier(it.get("subscription_type")),
                       count=it.get("mass_gift_count", 1))

        if mt == "subscription_gift":
            o = it.get("origin_id") or it.get("msg_param_community_gift_id")
            if o and o in bombs:
                return None                     # rolled up into the bomb
            return Msg(type="gift", **base, author=self._author(it),
                       badges=self._badges(it) or None,
                       tier=parse_tier(it.get("subscription_type")), count=1,
                       recipient={"id": str(it.get("gift_recipient_id", "")),
                                  "name": it.get("gift_recipient_display_name", "")})

        if mt == "raid":
            return Msg(type="raid", **base, author=self._author(it),
                       viewers=it.get("number_of_raiders", 0))

        if mt == "ban_user":
            a = it.get("author") or {}
            if not a.get("target_id"):
                return None
            return Msg(type="ban", **base,
                       user={"id": str(a["target_id"]), "name": a.get("name", "")},
                       duration=it.get("ban_duration"))
        return None


# ── TwitchDownloaderCLI ───────────────────────────────────────────────────

class TdcConverter(Converter):
    platform, fmt = "twitch", TDC

    def convert(self, path):
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        video = data.get("video") or {}
        self.meta = {"channel": (data.get("streamer") or {}).get("name", ""),
                     "video_id": str(video.get("id", "")),
                     "title": video.get("title")}
        created = video.get("created_at")
        if created:
            try:
                dt = datetime.datetime.fromisoformat(
                    str(created).replace("Z", "+00:00"))
                self.zero_ms = int(dt.timestamp() * 1000)
                self.zero_src = "video.created_at"
            except ValueError:
                pass
        for c in data.get("comments") or []:
            self.messages.append(self._one(c))

    def _one(self, c) -> Msg:
        m = c.get("message") or {}
        cm = c.get("commenter") or {}
        author = {"id": str(cm.get("_id") or cm.get("id") or ""),
                  "name": cm.get("display_name") or cm.get("name") or ""}
        if m.get("user_color"):
            author["color"] = m["user_color"]

        badges = []
        for b in m.get("user_badges") or []:
            bid = b.get("_id") or b.get("id") or ""
            key = f"{bid}_{b.get('version', '1')}"
            badges.append(key)
            self.badges[key] = bid

        parts = []
        for fr in m.get("fragments") or []:
            emo, text = fr.get("emoticon"), fr.get("text", "")
            if emo and emo.get("emoticon_id"):
                tok = emote_token(text.strip())
                parts.append(tok)
                self.emotes[tok] = emo["emoticon_id"]
            else:
                parts.append(text)

        kw = dict(ts=int(float(c.get("content_offset_seconds") or 0) * 1000),
                  id=c.get("_id") or c.get("id"), author=author,
                  badges=badges or None, text="".join(parts))
        bits = m.get("bits_spent") or 0
        if bits:
            return Msg(type="superchat", **kw, amount=float(bits), currency="bits")
        return Msg(type="chat", **kw)


# ── yt-dlp live_chat ──────────────────────────────────────────────────────

class YtdlpConverter(Converter):
    platform, fmt = "youtube", YTDLP

    def convert(self, path):
        zeros = []
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    self.skipped += 1
                    continue
                ts = self._ts(obj)
                for act in (obj.get("replayChatItemAction") or {}).get("actions") or []:
                    m = self._action(act, ts)
                    if m:
                        self.messages.append(m)
                        # ts == 0 is the post-hoc clamp for pre-stream chat;
                        # abs_ms stays correct, the offset does not.
                        if (m.abs_ms is not None and ts != 0
                                and len(zeros) < ZERO_SAMPLES):
                            zeros.append(m.abs_ms - ts)
        if zeros:
            self.zero_ms = int(statistics.median(zeros))
            self.zero_src = "timestampUsec"

    @staticmethod
    def _ts(obj) -> int:
        raw = obj.get("videoOffsetTimeMsec")
        if raw is None:
            raw = (obj.get("replayChatItemAction") or {}).get("videoOffsetTimeMsec")
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _abs(r) -> Optional[int]:
        try:
            return int(r["timestampUsec"]) // 1000
        except (KeyError, TypeError, ValueError):
            return None

    def _action(self, act, ts) -> Optional[Msg]:
        if "addChatItemAction" in act:
            item = (act["addChatItemAction"] or {}).get("item") or {}
            for key, fn in (("liveChatTextMessageRenderer", self._chat),
                            ("liveChatPaidMessageRenderer", self._superchat),
                            ("liveChatMembershipItemRenderer", self._member),
                            ("liveChatSponsorshipsGiftPurchaseAnnouncementRenderer",
                             self._gift),
                            ("liveChatSponsorshipsGiftRedemptionAnnouncementRenderer",
                             self._gift_redeem)):
                if key in item:
                    return fn(item[key], ts)
        elif "addBannerToLiveChatCommand" in act:
            return self._banner(act["addBannerToLiveChatCommand"] or {}, ts)
        # remove* is the live form, mark*AsDeleted the replay form. A
        # post-hoc download only ever carries the latter, so handling just
        # the live pair meant post-hoc conversions flagged zero deletions.
        elif "removeChatItemAction" in act or "markChatItemAsDeletedAction" in act:
            key = ("removeChatItemAction" if "removeChatItemAction" in act
                   else "markChatItemAsDeletedAction")
            t = (act[key] or {}).get("targetItemId")
            if t:
                self.deletions[t] = ts
        elif ("removeChatItemByAuthorAction" in act
                or "markChatItemsByAuthorAsDeletedAction" in act):
            key = ("removeChatItemByAuthorAction"
                   if "removeChatItemByAuthorAction" in act
                   else "markChatItemsByAuthorAsDeletedAction")
            c = (act[key] or {}).get("externalChannelId")
            if c:
                self.bans[c] = ts
        return None

    @staticmethod
    def _author(r) -> dict:
        n = (r.get("authorName") or {}).get("simpleText") or ""
        return {"id": r.get("authorExternalChannelId", ""),
                "name": n[1:] if n.startswith("@") else n}

    def _badges(self, r) -> list:
        out = []
        for b in r.get("authorBadges") or []:
            tip = (b.get("liveChatAuthorBadgeRenderer") or {}).get("tooltip") or ""
            if tip:
                key = "yt_" + re.sub(r"[^a-z0-9]+", "_", tip.lower()).strip("_")
                out.append(key)
                self.badges[key] = tip
        return out

    def _runs(self, runs) -> str:
        parts = []
        for r in runs or []:
            if "text" in r:
                parts.append(r["text"])
            elif "emoji" in r:
                e = r["emoji"]
                eid = e.get("emojiId", "")
                if e.get("isCustomEmoji"):
                    sc = e.get("shortcuts") or []
                    tok = emote_token(sc[0].strip(":") if sc else "emoji")
                    parts.append(tok)
                    self.emotes[tok] = eid
                else:
                    parts.append(eid)
        return "".join(parts)

    def _chat(self, r, ts) -> Msg:
        return Msg(type="chat", ts=ts, abs_ms=self._abs(r), id=r.get("id"),
                   author=self._author(r), badges=self._badges(r) or None,
                   text=self._runs((r.get("message") or {}).get("runs")))

    def _superchat(self, r, ts) -> Msg:
        amount, currency = parse_amount(
            (r.get("purchaseAmountText") or {}).get("simpleText", ""))
        hearted = bool((r.get("creatorHeartButton") or {})
                       .get("creatorHeartViewModel", {}).get("heartedHoverText"))
        return Msg(type="superchat", ts=ts, abs_ms=self._abs(r), id=r.get("id"),
                   author=self._author(r), badges=self._badges(r) or None,
                   text=self._runs((r.get("message") or {}).get("runs")) or None,
                   amount=amount, currency=currency, hearted=hearted)

    def _member(self, r, ts) -> Msg:
        header = r.get("headerPrimaryText") or {}
        kw = dict(ts=ts, abs_ms=self._abs(r), id=r.get("id"),
                  author=self._author(r), badges=self._badges(r) or None, tier=1)
        if header:
            text = "".join(x.get("text", "") for x in header.get("runs") or [])
            m = re.search(r"(\d+)", text)
            return Msg(type="resub", **kw, months=int(m.group(1)) if m else 1)
        return Msg(type="sub", **kw)

    def _gift(self, r, ts) -> Msg:
        header = (r.get("header") or {}).get(
            "liveChatSponsorshipsHeaderRenderer") or {}
        name = (header.get("authorName") or {}).get("simpleText") or ""
        primary = "".join(x.get("text", "")
                          for x in (header.get("primaryText") or {}).get("runs") or [])
        m = re.search(r"(\d+)", primary)
        return Msg(type="gift", ts=ts, abs_ms=self._abs(r), id=r.get("id"),
                   author={"id": r.get("authorExternalChannelId", ""),
                           "name": name[1:] if name.startswith("@") else name},
                   tier=1, count=int(m.group(1)) if m else 1)

    def _gift_redeem(self, r, ts) -> Msg:
        """
        The recipient side of a gifted membership. The renderer's author IS
        the recipient; the gifter is only a name inside the message runs, so
        it carries no id. Shaped like Twitch's subscription_gift for
        consistency: author = gifter, recipient = who received it.
        """
        runs = (r.get("message") or {}).get("runs") or []
        gifter = ""
        for run in reversed(runs):
            t = (run.get("text") or "").strip()
            if t and not t.lower().endswith("by") and "gifted" not in t.lower():
                gifter = t.lstrip("@")
                break
        return Msg(type="gift", ts=ts, abs_ms=self._abs(r), id=r.get("id"),
                   author={"id": "", "name": gifter} if gifter else None,
                   badges=self._badges(r) or None, tier=1, count=1,
                   recipient={"id": r.get("authorExternalChannelId", ""),
                              "name": self._author(r)["name"]})

    def _banner(self, cmd, ts) -> Optional[Msg]:
        banner = (cmd.get("bannerRenderer") or {}).get("liveChatBannerRenderer") or {}
        header = (banner.get("header") or {}).get("liveChatBannerHeaderRenderer") or {}
        contents = banner.get("contents") or {}

        if (header.get("icon") or {}).get("iconType") != "KEEP":
            redirect = contents.get("liveChatBannerRedirectRenderer")
            if not redirect:
                return None
            raider = ""
            for run in (redirect.get("bannerMessage") or {}).get("runs") or []:
                if run.get("text", "").startswith("@"):
                    raider = run["text"][1:]
                    break
            return Msg(type="raid", ts=ts,
                       author={"id": "", "name": raider} if raider else None)

        pinned_by = ""
        for run in (header.get("text") or {}).get("runs") or []:
            t = run.get("text", "")
            if t.startswith("@"):
                pinned_by = t[1:]
                break
            if "Pinned by" not in t:
                pinned_by = t

        r = contents.get("liveChatTextMessageRenderer")
        if not r:
            return Msg(type="pinned", ts=ts, pinned_by=pinned_by or None)
        return Msg(type="pinned", ts=ts, abs_ms=self._abs(r), id=r.get("id"),
                   author=self._author(r), badges=self._badges(r) or None,
                   text=self._runs((r.get("message") or {}).get("runs")),
                   pinned_by=pinned_by or None)


# ── entry point ───────────────────────────────────────────────────────────

CONVERTERS = {IRC: IrcConverter, TDC: TdcConverter, YTDLP: YtdlpConverter}


def convert_file(path: str, zero_ms: Optional[int] = None) -> Converter:
    fmt = detect_format(path)
    cls = CONVERTERS.get(fmt)
    if cls is None:
        raise ValueError(f"Unrecognised chat format: {os.path.basename(path)}")
    c = cls()
    c.convert(path)
    c.finalize()
    if zero_ms is not None:
        c.apply_zero(zero_ms)
    return c


# ── merge ─────────────────────────────────────────────────────────────────

def merge(paths: list[str], ref="youtube", zeros: Optional[dict] = None,
          fallback_zeros: Optional[dict] = None) -> dict:
    """
    Merge converted sources onto one timeline, tagging each message `origin`.

    `ts` in the output is ms from the reference zero; `abs_ms` is preserved.
    `ref` is a platform name or an epoch-ms int.
    `zeros` maps platform -> a manual zero (see parse_zero), which supplies an
    absolute reference to a capture that has none. Absolute forms are applied
    before the reference is chosen so they can serve as it; relative ones
    (+/-seconds) are applied after, since they are measured against it.

    `fallback_zeros` is the same shape but applies only where the file carried
    no zero of its own, so a caller can offer known values without overriding
    what the capture actually says.
    """
    zeros = zeros or {}
    convs = [convert_file(p) for p in paths]

    for plat, val in (fallback_zeros or {}).items():
        # An explicit zero wins outright. Applying the fallback first would
        # fill abs_ms, and apply_zero only fills gaps, so the explicit value
        # would silently fail to move anything.
        if plat in zeros:
            continue
        for c in convs:
            if c.platform == plat and c.zero_ms is None:
                c.apply_zero(parse_zero(val), src="cache")
    by_plat = {}
    for c in convs:
        by_plat.setdefault(c.platform, []).append(c)

    unknown = set(zeros) - set(by_plat)
    if unknown:
        raise ValueError(f"--zero given for {', '.join(sorted(unknown))} "
                         f"but no such source was loaded")

    deferred = {}
    for plat, raw in zeros.items():
        if str(raw).strip()[:1] in "+-":
            deferred[plat] = raw
        else:
            for c in by_plat[plat]:
                c.apply_zero(parse_zero(raw))

    if isinstance(ref, int):
        ref_ms, ref_src = ref, "explicit"
    else:
        ref_ms = next((c.zero_ms for c in convs
                       if c.platform == ref and c.zero_ms), None)
        ref_src = ref
        if ref_ms is None:
            ref_ms = min((c.zero_ms for c in convs if c.zero_ms), default=None)
            ref_src = f"earliest (no {ref} zero)"
    if ref_ms is None:
        raise ValueError("no source has an absolute reference; cannot merge. "
                         "Supply one with --zero PLATFORM=<epoch|ISO>")

    for plat, raw in deferred.items():
        for c in by_plat[plat]:
            c.apply_zero(parse_zero(raw, ref=ref_ms))
    ref = ref_ms

    out, seen, dupes, unplaced = [], set(), 0, 0
    for c in convs:
        for m in c.messages:
            if m.abs_ms is None:
                unplaced += 1
                continue
            if m.id and (c.platform, m.id) in seen:
                dupes += 1
                continue
            if m.id:
                seen.add((c.platform, m.id))
            d = serialize(m, origin=c.platform)
            d["ts"] = m.abs_ms - ref
            out.append(d)
    out.sort(key=lambda d: d["ts"])

    emotes: dict[str, dict] = {}
    badges: dict[str, dict] = {}
    for c in convs:
        # Nested by platform: the ID namespaces differ, so a renderer needs
        # the origin to resolve one. Also fewer bytes than prefixed keys.
        emotes.setdefault(c.platform, {}).update(c.emotes)
        badges.setdefault(c.platform, {}).update(c.badges)

    return {
        "metadata": {
            "merged": True,
            "zero_epoch_ms": ref,
            "zero_source": ref_src,
            "messages": len(out),
            "duplicates_removed": dupes,
            "unplaced_no_abs": unplaced,
            "sources": [{"file": os.path.basename(p), "platform": c.platform,
                         "format": c.fmt, "messages": len(c.messages),
                         "zero_ms": c.zero_ms, "zero_source": c.zero_src,
                         "skipped_lines": c.skipped}
                        for p, c in zip(paths, convs)],
            "emotes": emotes,
            "badges": badges,
        },
        "messages": out,
    }


def main():
    ap = argparse.ArgumentParser(description="Chat capture converter / merger.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pc = sub.add_parser("convert", help="one capture to the unified schema")
    pc.add_argument("input")
    pc.add_argument("-o", "--output")
    pc.add_argument("--zero", metavar="VALUE",
                    help="define this capture's zero: epoch ms, epoch s, "
                         "or ISO datetime")
    pc.add_argument("--stats", action="store_true", help="report only")

    pm = sub.add_parser("merge", help="several captures onto one timeline")
    pm.add_argument("inputs", nargs="+")
    pm.add_argument("-o", "--output")
    pm.add_argument("--ref", default="youtube",
                    help="reference timeline: youtube | twitch | epoch ms")
    pm.add_argument("--zero", action="append", metavar="PLATFORM=VALUE",
                    help="define a source's zero when the file has none: "
                         "epoch, ISO datetime, or +/-seconds from the "
                         "reference. Repeatable.")
    pm.add_argument("--stats", action="store_true", help="report only")

    args = ap.parse_args()
    paths = [args.input] if args.cmd == "convert" else args.inputs
    for p in paths:
        if not os.path.exists(p):
            sys.exit(f"  Not found: {p}")

    if args.cmd == "merge":
        ref = int(args.ref) if args.ref.lstrip("-").isdigit() else args.ref
        try:
            res = merge(args.inputs, ref=ref, zeros=parse_zero_args(args.zero))
        except (ValueError, json.JSONDecodeError) as e:
            sys.exit(f"  {e}")
        md = res["metadata"]
        for s_ in md["sources"]:
            z = (f"{datetime.datetime.fromtimestamp(s_['zero_ms'] / 1000):%H:%M:%S}"
                 if s_["zero_ms"] else "UNKNOWN")
            print(f"  {s_['platform']:<8} {s_['messages']:>7,}  zero {z} "
                  f"({s_['zero_source']})  {s_['file']}")
        when = datetime.datetime.fromtimestamp(md["zero_epoch_ms"] / 1000)
        print(f"  zero      {when:%Y-%m-%d %H:%M:%S} via {md['zero_source']}")
        print(f"  merged    {md['messages']:,}")
        if md["duplicates_removed"]:
            print(f"  dupes     {md['duplicates_removed']:,}")
        if md["unplaced_no_abs"]:
            print(f"  UNPLACED  {md['unplaced_no_abs']:,} (no absolute time, omitted)")
        if res["messages"]:
            lo, hi = res["messages"][0]["ts"], res["messages"][-1]["ts"]
            print(f"  range     {lo / 1000:.0f}s to {hi / 1000:.0f}s")
        if args.stats:
            return
        out = args.output or os.path.splitext(args.inputs[0])[0] + ".merged.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=1)
        print(f"  written   {out}")
        return

    try:
        zero = parse_zero(args.zero) if args.zero else None
        c = convert_file(args.input, zero_ms=zero)
    except (ValueError, json.JSONDecodeError) as e:
        sys.exit(f"  {e}")

    res = c.result()
    md, msgs = res["metadata"], res["messages"]
    counts: dict[str, int] = {}
    for m in msgs:
        counts[m["type"]] = counts.get(m["type"], 0) + 1

    print(f"  format    {md['format']} ({md['platform']})")
    print(f"  messages  {md['messages']:,}"
          + (f"   skipped {md['skipped_lines']}" if md["skipped_lines"] else ""))
    if md["zero_ms"]:
        when = datetime.datetime.fromtimestamp(md["zero_ms"] / 1000)
        print(f"  zero      {when:%Y-%m-%d %H:%M:%S} via {md['zero_source']}")
    else:
        print("  zero      UNKNOWN - no absolute reference in file")
    print(f"  absolute  {sum(1 for m in msgs if 'abs_ms' in m):,}/{len(msgs):,}")
    for k in sorted(counts):
        print(f"    {k:<10} {counts[k]:>7,}")

    if args.stats:
        return
    out = args.output or os.path.splitext(args.input)[0] + ".chat.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(res, f, ensure_ascii=False, indent=1)
    print(f"  written   {out}")


if __name__ == "__main__":
    main()
