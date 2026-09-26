"""Witnesses, and what to believe when they disagree.

C3's core. Everything else in this round is plumbing around it.

WHAT THIS REPLACES. Today every timing in an audit comes from a FALLBACK
CHAIN: ask the cache, then the chat file, then the log, then the filename, and
the first one that answers wins. The rest are never asked. So a wrong first
answer is not merely wrong, it is SILENT — nothing downstream can tell "the
cache said 13:02" from "four sources agreed on 13:02", and nothing anywhere
records that the other three were never consulted.

The inversion: every witness answers the same question, all of them are asked,
and the settlement records who answered, how well they measured it, and
whether anybody disagreed. Agreement is the verdict; disagreement is the
product, not something to resolve away quietly.

WHAT A SURVEY OF THE REAL CACHE CHANGED ABOUT THE DESIGN. 450 rows, measured
rather than assumed:

  · `start_time` is THREE FIELDS WEARING ONE COAT, concurrently. 257 rows hold
    the recorder's naive LOCAL clock at DETECTION; 178 hold Helix's `created_at`
    in UTC, which is the BROADCAST START; 15 hold a yt-dlp upload date with no
    time in it at all. Not chronological strata — both main writers ran from
    April to September. Read as one thing, 257 rows are five hours wrong.
  · That is not a rounding error in the abstract: comparing the two where a row
    has both, the offset is 17,959s — five hours minus ~41 seconds. The five
    hours is the timezone. THE 41 SECONDS IS DETECTION LAG, and it splits hard:
    Twitch is 12–71s late, YouTube is 7–588s late. So a detection time is a
    fine witness to when RECORDING started and a poor one to when the BROADCAST
    did — on YouTube it can be nine minutes out.
  · The witness set is SPARSE. 363 of 450 rows carry only two of the four
    time-bearing fields; `stream_start_epoch_ms` exists on 14%. No recorder
    sidecar exists anywhere yet. So for the median entry there is exactly one
    clock witness.

That last point decides the shape of `settle()`. Single-sourced is not the
degenerate case to be handled at the end — it is the MAIN PATH, and what it
has to produce is not a shrug but a value with its provenance and its
precision attached. `local_start_precision_s` on the archive's capture rows is
the pattern; this generalises it.

NO I/O IN THIS MODULE, deliberately. Everything here is a pure function over
values somebody else read off a disk or a socket. That is what lets the whole
thing be verified against four hundred real rows without a NAS, and what stops
the authority table turning into something only an integration test can reach.
"""
import datetime
import re


# ═══════════════════════════════════════════════════════════════════════════
#  WHAT A WITNESS IS
# ═══════════════════════════════════════════════════════════════════════════

#  A claim is a QUESTION, not a field. Two witnesses disagreeing about
#  `broadcast_start` is news; a witness to `record_start` and a witness to
#  `broadcast_start` disagreeing is just the 41 seconds above, and treating
#  that as a conflict is how a report cries wolf on every entry.
BROADCAST_START = "broadcast_start"
RECORD_START = "record_start"
FILE_DURATION = "file_duration"
BROADCAST_DURATION = "broadcast_duration"

CLAIMS = (BROADCAST_START, RECORD_START, FILE_DURATION, BROADCAST_DURATION)

#  How well a source measures, in seconds. Not a confidence score — a physical
#  property of the measurement. A filename carries minutes because that is what
#  is written in it; a chat file's first message carries the message's own
#  millisecond stamp. Kept separate from authority because they are different
#  facts: a filename is low authority AND coarse, but the platform's own
#  duration is high authority and coarse-ish, and collapsing the two into one
#  number would lose the distinction the report needs to explain itself.
PRECISE = 1
MINUTE = 60


def witness(claim: str, value, source: str, precision_s: int = PRECISE,
            **detail) -> dict:
    """One source's answer to one question, with how well it knows it.

    `value` is milliseconds for the two clocks and seconds for the two
    durations, matching what the rest of the codebase already passes around.
    Mixing units inside one claim would be a bug; mixing them ACROSS claims is
    what every existing caller already expects, so this does not invent a
    third convention to be converted at every boundary.

    `detail` is whatever that particular reading needs to explain itself —
    the timezone a naive stamp was read in, say. Kept open rather than typed
    because the set differs per source and a fixed schema would mean every
    source carrying every other source's fields as None.
    """
    out = {"claim": claim, "value": value, "source": source,
           "precision_s": int(precision_s)}
    out.update(detail)
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  THE AUTHORITY TABLE
# ═══════════════════════════════════════════════════════════════════════════
#
#  From AUDIT.md's "Who answers what", which is the actual design. There is no
#  primary source of truth and looking for one is what made this feel
#  unbounded: each witness is authoritative about a DIFFERENT question.
#
#  Higher wins. A source absent from a claim's table is not merely
#  low-authority — it is NOT A WITNESS to that question and is refused, which
#  is the whole correction the survey produced. The recorder's detection time
#  has always been in the running for `broadcast_start` and has never been
#  entitled to answer it.

AUTHORITY = {
    BROADCAST_START: {
        # Measured at detection off the live dump, by the program that was
        # there. On Twitch this number is unrecoverable once the stream ends,
        # which is why the sidecar outranks everything.
        "sidecar": 100,
        # The same origin, written into the cache at record time.
        "cache:epoch": 90,
        # The platform's own word. Helix `created_at`; authoritative about
        # when the broadcast began and nothing else.
        "cache:start_time(utc)": 80,
        "platform": 80,
        # A chat file whose zero is an ABSOLUTE wall clock. Exact, and it
        # survives everything else being lost, which is what makes it the
        # back catalogue's witness.
        "chat:absolute": 70,
        # Minute-accurate at best, and ambiguous besides: the recorder stamps
        # detection time, an ls-audit re-download stamps the broadcast start.
        "filename": 20,
    },
    RECORD_START: {
        "sidecar": 100,
        "cache:epoch": 90,
        # An IRC capture's first message: the moment the recorder attached.
        "chat:relative": 70,
        "log": 60,
        # THE CORRECTION. The recorder's naive local `start_time` is a precise
        # answer to this question and was being offered as an answer to the
        # one above, five hours adrift and a minute late.
        "cache:start_time(local)": 55,
        "filename": 20,
    },
    FILE_DURATION: {
        # The file wins; it is what you have.
        "ffprobe": 100,
        "sidecar": 60,
    },
    BROADCAST_DURATION: {
        # The platform's number is about the BROADCAST. ffprobe's is about the
        # FILE. They disagree legitimately — a VOD trimmed at the far end, a
        # capture that started late or died early — and that disagreement is a
        # finding rather than noise.
        "platform": 100,
        "cache:duration": 90,
        "sidecar": 50,
    },
}


def may_answer(claim: str, source: str) -> bool:
    """Is this source a witness to this question at all?"""
    return source in AUTHORITY.get(claim, {})


# ═══════════════════════════════════════════════════════════════════════════
#  READING `start_time`, WHICH IS THREE FIELDS
# ═══════════════════════════════════════════════════════════════════════════

_MICRO_NAIVE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+$")
_UTC_Z = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
_NAIVE = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d$")
_BARE_DATE = re.compile(r"^\d{8}$")

#  What wrote it, recovered from how it is written. Fragile by construction —
#  it is a regex standing in for a field nobody stamped — and it is the right
#  trade only because the alternative is changing what gets written to a file
#  C5 deletes. Three writers, three shapes, and they have never overlapped:
#
#    ls_rec:1001       record_start.isoformat()   naive local, microseconds
#    ls_common:741     Helix v["created_at"]      UTC with a Z
#    ls_audit:557      yt-dlp upload_date         YYYYMMDD, no clock
#
#  A fourth writer appearing without a fourth shape is the failure mode, and
#  the answer to it is `UNKNOWN` rather than a guess: a start time that cannot
#  be attributed is not a witness, and saying so beats offering it as one.
LOCAL = "local"
UTC = "utc"
DATE_ONLY = "date"
UNKNOWN = "unknown"


def start_time_kind(raw) -> str:
    """Which of the three `start_time`s this is."""
    s = "" if raw is None else str(raw).strip()
    if _MICRO_NAIVE.match(s):
        return LOCAL
    if _UTC_Z.match(s):
        return UTC
    if _BARE_DATE.match(s):
        return DATE_ONLY
    #  A bare naive second-resolution stamp. Fourteen rows, all YouTube, all
    #  written before the recorder started carrying microseconds — so it is
    #  the same writer and the same meaning, and reading it as UTC would put
    #  those fourteen five hours out for no reason.
    if _NAIVE.match(s):
        return LOCAL
    return UNKNOWN


def _epoch_ms(s: str, kind: str, offset_min: int | None) -> int | None:
    """Parse to absolute milliseconds, honouring what the shape means.

    A naive stamp is CONVERTED by an offset, never stripped of one. That is
    the distinction two existing readers get wrong — `find_vod_by_date` and
    `resolve_id`'s off-by label both call `.replace(tzinfo=None)`, which
    DISCARDS the offset rather than converting by it, leaving every Helix row
    five hours from where it belongs and outside that function's one-hour
    window.

    `offset_min` is explicit, and the reason is a bug this module had for
    about ten minutes. The obvious spelling is `.astimezone()`, which attaches
    the SYSTEM's local zone — correct on the Pi, which is the machine that
    wrote these, and wrong everywhere else. Run over the real cache in a UTC
    container it made all 67 rows that carry both a naive `start_time` and an
    epoch field disagree by exactly 18,000 seconds; run at UTC-5 the same 67
    agree to the second. A witness whose answer depends on who is asking is
    not a witness, and it would have made this suite's verdict depend on the
    runner's `TZ`.

    None still falls back to the system zone, because on the recorder that is
    right and requiring config would break the CLI for no gain — but the
    reading records which of the two it used, so a report can say so.
    """
    try:
        if kind == DATE_ONLY:
            d = datetime.datetime.strptime(s, "%Y%m%d")
        else:
            d = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo is None:
        if offset_min is None:
            d = d.astimezone()
        else:
            d = d.replace(tzinfo=datetime.timezone(
                datetime.timedelta(minutes=offset_min)))
    return int(d.timestamp() * 1000)


def read_start_time(vod: dict, offset_min: int | None = None) -> dict | None:
    """`start_time` → the witness it actually is, or None.

    The whole correction in one function. A UTC row witnesses the broadcast
    start; a local row witnesses when RECORDING started; a bare date witnesses
    neither, because a day is not a clock.
    """
    raw = vod.get("start_time")
    if raw in (None, ""):
        return None
    kind = start_time_kind(raw)
    if kind in (DATE_ONLY, UNKNOWN):
        return None
    ms = _epoch_ms(str(raw).strip(), kind, offset_min)
    if ms is None:
        return None
    if kind == UTC:
        #  Already absolute; no offset was assumed and saying so matters,
        #  because it is the one row shape no timezone question applies to.
        return witness(BROADCAST_START, ms, "cache:start_time(utc)", PRECISE,
                       read_as="utc", label="cache (start_time)")
    return witness(RECORD_START, ms, "cache:start_time(local)", PRECISE,
                   read_as="local", label="cache (start_time)",
                   #  The recorder writes this and `record_start_epoch_ms`
                   #  from one datetime in one line. Two spellings, one
                   #  observation.
                   corroborates=False,
                   offset_min=offset_min,
                   offset_from="system" if offset_min is None else "given")


def read_cache(vod: dict, offset_min: int | None = None) -> list[dict]:
    """Every witness one cache row can offer, each routed to its own question.

    Deliberately returns a LIST rather than the single best answer. Picking is
    `settle`'s job and it needs the losers: "the epoch field and the platform
    agree" and "the epoch field answered and nothing else was asked" are
    different reports, and today they are the same one.
    """
    out = []
    if not vod:
        return out
    if vod.get("stream_start_epoch_ms"):
        out.append(witness(BROADCAST_START, vod["stream_start_epoch_ms"],
                           "cache:epoch", PRECISE, label="cache"))
    if vod.get("record_start_epoch_ms"):
        out.append(witness(RECORD_START, vod["record_start_epoch_ms"],
                           "cache:epoch", PRECISE, label="cache"))
    st = read_start_time(vod, offset_min)
    if st:
        out.append(st)
    #  The platform's number for how long the BROADCAST ran, which is not how
    #  long the file is. Held as a different claim rather than a different
    #  opinion about the same one.
    if isinstance(vod.get("duration"), int) and vod["duration"] > 0:
        out.append(witness(BROADCAST_DURATION, vod["duration"],
                           "cache:duration", PRECISE, label="cache"))
    return out


# ═══════════════════════════════════════════════════════════════════════════
#  THE OTHER WITNESSES
# ═══════════════════════════════════════════════════════════════════════════
#
#  Each takes a VALUE somebody else read off a disk, never a path. That is
#  what keeps this module pure and the authority table drivable from a test
#  file — the alternative is a witness list only an integration run over a NAS
#  can reach, which is how the fallback chain got to be untestable in the
#  first place.
#
#  Every reader attaches a `label`, which is the string the CLI and the
#  archive have always displayed for that source. The witness's `source` is
#  the authority table's key and is not for reading; the label is for reading
#  and is not for deciding. Keeping them apart is what lets the table be
#  renamed without changing a word of what anybody sees.


def read_sidecar(doc: dict | None) -> list[dict]:
    """A recorder-written sidecar: the best witness there is, when it exists.

    It was written by the program that was present, at the moment, off the
    live probe. On Twitch the broadcast start cannot be rebuilt once the
    stream ends, which is the whole reason C1 built this before C3 needed it.

    An AUDIT-written sidecar is refused. It holds `cmd_timings`' own
    conclusions, so believing it here would be the audit reading back its own
    homework and scoring it as corroboration — the one failure a witness
    system has to be built against.
    """
    out = []
    if not doc or doc.get("written_by") != "recorder":
        return out
    if doc.get("stream_start_epoch_ms"):
        out.append(witness(BROADCAST_START, doc["stream_start_epoch_ms"],
                           "sidecar", PRECISE,
                           label=doc.get("stream_start_source") or "sidecar"))
    if doc.get("record_start_epoch_ms"):
        out.append(witness(RECORD_START, doc["record_start_epoch_ms"],
                           "sidecar", PRECISE,
                           label=doc.get("record_start_source") or "sidecar"))
    if doc.get("duration_secs"):
        #  The recorder timed its own capture, so this is about the FILE.
        out.append(witness(FILE_DURATION, doc["duration_secs"],
                           "sidecar", PRECISE,
                           label=doc.get("duration_source") or "sidecar"))
    return out


#  Which question a chat file's zero answers depends on the format it is in,
#  and that is not a detail — it is the same split as `start_time`'s, one
#  level down. A YouTube replay stamp and a Twitch downloader stamp are wall
#  clocks and so answer when the BROADCAST began; an IRC capture's first
#  message is the moment the recorder attached and answers when RECORDING
#  did. Reading either as the other is hours wrong on a waiting room.
_CHAT_ABSOLUTE = ("yt:timestampUsec", "tdc:created_at")
_CHAT_RELATIVE = ("irc:tmi_sent_ts",)


def read_chat_zero(zero_ms, zsrc: str) -> list[dict]:
    """`ls_chat.peek_zero`'s answer, routed by what its format means."""
    if not zero_ms or not zsrc:
        return []
    label = f"chat ({zsrc})"
    if zsrc in _CHAT_ABSOLUTE:
        return [witness(BROADCAST_START, zero_ms, "chat:absolute", PRECISE,
                        label=label)]
    if zsrc in _CHAT_RELATIVE:
        return [witness(RECORD_START, zero_ms, "chat:relative", PRECISE,
                        label=label)]
    #  A format nobody has taught this what it means. Declining beats routing
    #  it to whichever question happens to be missing an answer.
    return []


def read_log(record_ms) -> list[dict]:
    """The recorder's own log line for this capture's first part."""
    if not record_ms:
        return []
    return [witness(RECORD_START, record_ms, "log", PRECISE, label="log")]


def read_filename(fname_ms) -> list[dict]:
    """The `@ YYYY-MM-DD_HH-MM` stamp, which knows minutes and is ambiguous.

    Ambiguous in a way the authority table handles rather than this function:
    the recorder stamps DETECTION time and an ls-audit re-download stamps the
    BROADCAST start, so the same string means two things depending on which
    program wrote the file. It is offered to both questions at the lowest rank
    either has, so it answers only when nothing better did — which is exactly
    what a minute-accurate guess should be allowed to do.
    """
    if not fname_ms:
        return []
    return [witness(RECORD_START, fname_ms, "filename", MINUTE,
                    label="filename (minute)", corroborates=False),
            witness(BROADCAST_START, fname_ms, "filename", MINUTE,
                    label="filename (minute)", corroborates=False)]


def read_ffprobe(duration_s) -> list[dict]:
    """How long the file on disk actually is. The file wins; it is what you have."""
    if not duration_s:
        return []
    return [witness(FILE_DURATION, duration_s, "ffprobe", PRECISE,
                    label="ffprobe")]


# ═══════════════════════════════════════════════════════════════════════════
#  SETTLEMENT
# ═══════════════════════════════════════════════════════════════════════════

#  What the settlement says happened. `single` is not a failure — on this
#  archive it is the common case, and a report that treated it as one would
#  flag 363 of 450 entries as a problem.
SINGLE = "single"
AGREE = "agree"
DISAGREE = "disagree"
NONE = "none"


#  Two sources that both claim the second still will not agree ON the second.
#  A platform reports when its ingest went live; a chat file's zero is derived
#  from the first message the chat service stamped. Those are different events
#  a few seconds apart, and no amount of precision on either side closes the
#  gap — so with a floor of 1 they DISAGREE for ever, on every healthy entry
#  in the archive.
#
#  Entry #716 is the case: cache 10:59:28 and chat 10:59:25, reported as a
#  warning reading "witnesses disagree about the broadcast start by 00:00:02".
#  Nothing was wrong and nothing was actionable, which is the definition of
#  the noise that makes a report stop being read.
#
#  `precision_s` answers "how finely can this source STATE a number". It was
#  being asked "how closely should two different methods agree", which is a
#  different question, and conflating them is what put the warning there.
CLOCK_SLOP_S = 10


def tolerance_s(witnesses: list[dict]) -> int:
    """How far apart two measurements may be and still be the same one.

    The COARSEST witness sets it, over a floor. Two sources one minute apart
    are in agreement when one of them is a filename that only knows minutes,
    and in disagreement when both claim to know the second — the same
    numbers, and the difference is what they were able to measure, which is
    exactly what `precision_s` is for.

    The floor is what `precision_s` cannot express: see `CLOCK_SLOP_S`.
    """
    return max([w["precision_s"] for w in witnesses] + [CLOCK_SLOP_S])


def settle(claim: str, witnesses: list[dict]) -> dict:
    """Every witness to one question → one answer that remembers the others.

    The authority table picks the value. It does NOT pick by majority, and
    that is deliberate: three coarse sources agreeing do not outrank the one
    program that was present with a stopwatch. Majority rule is how a
    filename's rounded minute wins against a measurement.

    What comes back always carries the whole bench, so a caller that wants to
    say "the platform and the file disagree by four minutes" can, and a caller
    that just wants the number can ignore it.
    """
    usable = [w for w in witnesses
              if w.get("claim") == claim and w.get("value") is not None
              and may_answer(claim, w.get("source"))]
    refused = [w for w in witnesses
               if w.get("claim") == claim and not may_answer(claim, w.get("source"))]
    if not usable:
        return {"claim": claim, "value": None, "source": None,
                "precision_s": None, "agreement": NONE, "best": None,
                "witnesses": [], "refused": refused, "spread_s": None}

    rank = AUTHORITY[claim]
    #  Ties broken by precision, then by source name. The last of those is
    #  arbitrary and exists only so the answer is the same on two runs over
    #  the same data — an audit that reports a different source each sweep is
    #  an audit whose findings cannot be diffed.
    best = max(usable, key=lambda w: (rank[w["source"]], -w["precision_s"],
                                      w["source"]))

    #  CORROBORATION IS NOT AGREEMENT, and conflating them was a hole this
    #  module had until `test_gather` walked into it.
    #
    #  A filename's minute matches the cache's epoch because the recorder
    #  wrote both, off one clock, in one moment; the cache's naive
    #  `start_time` matches its epoch field for the same reason, which is why
    #  all 67 real rows holding both agree to the millisecond. Reporting any
    #  of that as two witnesses concurring says something false — it is one
    #  witness spelled twice, and believing it is the same failure as
    #  believing an audit-written sidecar, which this module already refuses.
    #
    #  So a DERIVED witness may answer and may not vouch. It stays in
    #  `witnesses`, it can still win on authority when it is the best there
    #  is, and it is simply not counted when deciding whether anybody
    #  independent concurred.
    vouching = [w for w in usable if w.get("corroborates", True)]
    if len(vouching) < 2:
        agreement = SINGLE
        spread = None
    else:
        #  Seconds, for both units. A clock's value is milliseconds and a
        #  duration's is seconds, so the spread has to be converted for one of
        #  them or the tolerance means two different things.
        scale = 1000 if claim in (BROADCAST_START, RECORD_START) else 1
        vals = [w["value"] / scale for w in vouching]
        spread = int(round(max(vals) - min(vals)))
        agreement = AGREE if spread <= tolerance_s(vouching) else DISAGREE

    return {"claim": claim, "value": best["value"], "source": best["source"],
            "precision_s": best["precision_s"], "agreement": agreement,
            #  The winner whole, not just its number. Callers need its
            #  `label` — the string the CLI has always printed for that source
            #  — and re-deriving it from `source` here would put a second
            #  display vocabulary in the settlement.
            "best": best,
            "witnesses": usable, "refused": refused, "spread_s": spread}


# ═══════════════════════════════════════════════════════════════════════════
#  TESTIMONY
# ═══════════════════════════════════════════════════════════════════════════
#
#  The sidecar was a REGENERATION: every run rebuilt it from whatever files
#  were on the NAS that minute, and everything the files no longer said was
#  gone. Deleting one duplicated recording erased its platform's entire block
#  — start, record start, duration, the lot — and the recorder's two wall
#  times, the only numbers in this system that nothing can recover afterwards,
#  survived that only because the archive happened to hold a copy.
#
#  So it accumulates instead. A witness is one source's answer to one
#  question, which makes (claim, source) its identity: ask again and the new
#  answer replaces the old one under that key; do not ask, or ask and get
#  nothing, and the stored answer stays.
#
#  Two stamps, recorded and not acted on:
#
#    seen_at  when this reading was last taken.
#    stale    this run could not re-take it. The file it came from is gone,
#             the cache row went, the log rolled over.
#
#  A stale witness keeps its authority and settles exactly as it always did.
#  That is deliberate and it is not obviously right for every claim — a
#  `file_duration` is a fact about a file, and one whose file has been deleted
#  is testimony about something that no longer exists, while a
#  `broadcast_start` is a fact about the broadcast and stays true whatever
#  happens to the recordings of it. Marking it and leaving the behaviour alone
#  keeps that a question somebody can look at and answer, rather than one this
#  function answered quietly on their behalf.

def testimony_key(w: dict) -> tuple:
    """What makes two readings the same reading: one source, one question."""
    return (str(w.get("claim") or ""), str(w.get("source") or ""))


def merge_testimony(kept, fresh, *, now_s: int) -> list[dict]:
    """Everything ever heard about one platform, newest answer per source.

    `fresh` wins on collision, and that is the whole of the overwrite rule: a
    renamed file, a refreshed cache row, a platform asked a second time are
    all the same source answering again, and the newer answer is the one to
    keep. `kept` entries with no fresh counterpart survive, marked stale.

    Order is fresh-first then kept, so a caller reading the list top-down sees
    what was just measured before what merely persists.
    """
    out, seen = [], set()
    for w in fresh or []:
        k = testimony_key(w)
        if k in seen:
            continue
        seen.add(k)
        out.append({**w, "seen_at": now_s, "stale": False})
    for w in kept or []:
        k = testimony_key(w)
        if k in seen or not k[0]:
            continue
        seen.add(k)
        #  `seen_at` is preserved, never restamped: it says when the reading
        #  was TAKEN, and a sweep that merely carried it forward has not taken
        #  it again. Restamping would make a five-month-old measurement look
        #  like this morning's on every run.
        out.append({**w, "seen_at": w.get("seen_at"), "stale": True})
    return out
