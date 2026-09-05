# ls-rec
Livestream recorder.

## ls_assets.py — the pictures a merged chat only names

A merged chat says `:tenmaWow:` and `subscriber_12`. The pictures live on
Twitch's and YouTube's CDNs, and the archive serves files rather than links —
it is offline-shaped on purpose, and an emote that 404s in three years is a
message nobody can read. So each one is fetched once, into the media root
beside the videos:

    emotes/index.json
    emotes/twitch/<id>.png            and .gif beside it when it animates
    emotes/youtube/<channel>/<hash>.png
    badges/index.json
    badges/twitch/<set>_<version>.png
    badges/youtube/<key>.png

`index.json` is the lookup: everything already tried, including what came back
404 and when. A re-merge asks the network about the emotes that are new and
nothing else. A failure is believed for a week and then asked about again —
long enough that a sweep over two hundred entries is not a sweep over two
hundred timeouts, short enough to recover from a channel that lost its
affiliate status and got it back. The lookup is a cache and not the truth: a
file deleted behind its back is fetched again.

Everything the renderer reads is a `.png` at a path derivable from the merged
file alone, which is the point of that rule — the file names an emote and the
page must be able to turn that name into a URL without asking anything else.

    ls_assets.py 097_merged-chat.json     one file
    ls_assets.py *_merged-chat.json       the lot
    ls-audit 97 --merge-chat --no-assets  merge without fetching

It also runs at the end of every `--merge-chat`, which is where it belongs:
the URLs for YouTube's emotes and member badges exist **only** in the raw dump,
and the pipeline archives the raws minutes later. ls_chat now carries those
URLs into the merged file so the fetch is no longer racing that, but the merge
is still the moment when everything needed is in one place.

**Twitch emotes need no credentials** — the id is the address. **Twitch badges
do**: their images come from Helix, keyed by set and version. Without
`twitch_client_id` and `twitch_client_secret` the badges are skipped, loudly,
and everything else still lands. YouTube needs none for either.

**A 404 and a dead network are not the same answer.** A 404 is Twitch saying
the emote is gone, and is remembered. A refused connection is this machine
having no network; nothing is recorded and the sweep stops, because writing six
hundred tombstones for pictures that are all still there would then take a week
to expire.

## ls_jobs.py — the archive's other half

The tenma archive holds no write handle inside the media tree; `/media` is
mounted read-only in its compose file and that is deliberate. So it cannot move
a file, delete one, or download one. It writes down what it wants and this
worker comes and takes it.

    fetch     a url someone pasted -> a file in quarantine, for review
    promote   an approved clip     -> quarantine into the media tree
    purge     an admin said so     -> gone

Its own service, not part of the recorder. `ls_archive.py` is a library on the
recorder's poll tick and its rule is that nothing in it may ever break a
recording — six-second timeouts, every call wrapped. The work here is minutes
of yt-dlp, multi-gigabyte moves and `unlink()` on masters, which does not
belong on the tick that notices she has gone live. `ls_rec.py` and `ls_chat.py`
are untouched by all of this.

### What the archive is allowed to say

A job carries a kind, a snippet id, a url, and one or two **relative** names —
`4f3c….mp4` in quarantine, `snippets/4f3c….mp4` in the media tree. Never a
directory, never an absolute path, never a command. The roots are resolved
here, from this machine's config, and every name is checked to land inside them
with symlinks already resolved.

The host allowlist for `fetch` lives in `config.json` on this machine. The
archive has its own copy and refuses links that fail it, but that copy is a
courtesy to whoever is pasting — this one is the rule, and it is the reason a
compromised archive still cannot make the recorder fetch from an attacker's
host.

### Setting it up

    ls_jobs.py --check

resolves both roots, proves it can write to them, prints exactly what it found,
and exits. Do that before enabling the service. Config keys, all optional:

| key | default |
| --- | --- |
| `archive_media_root` | `nas_path` with `archive_media_prefix` walked back off the end |
| `archive_quarantine_dir` | the sibling of the media root named `quarantine` |
| `archive_job_kinds` | `["fetch", "promote", "purge"]` |
| `archive_job_interval` | 20 seconds between polls when idle |
| `archive_worker_name` | this machine's hostname |
| `archive_fetch_hosts` | youtube, twitch, x, discord cdn |
| `archive_fetch_max_s` / `archive_fetch_max_mb` | 600 / 200, the archive's own caps |
| `archive_fetch_timeout_s` | 1800 |

Then `ls-jobs.service` — fill in the placeholders, `systemctl enable --now
ls-jobs`, and watch it with `journalctl -u ls-jobs -f`.

    ls_jobs.py --once                  one pass through the queue, then exit
    ls_jobs.py --kinds promote,purge   leave fetch to somebody else

### Things worth knowing

**Promote is a `rename(2)`** when quarantine and the media tree are on the same
filesystem, which is the whole reason quarantine sits beside `archives/` rather
than somewhere convenient: a 200 MB clip moves in microseconds and there is
never a half-written master. Across filesystems it copies to a `.part-` name
and renames into place instead, and `--check` says which of the two you have.

**A lost report is the only thing that costs anything.** Claiming takes a
five-minute lease, so a worker that dies frees its job — but work already done
and never reported gets done again. Reports retry, then spool to
`.archive_jobs_outbox.json`, and are replayed before the next claim. Every
handler is written so that doing it twice is not a failure: a promote whose
file is already moved reports `done`, and so does a purge that finds nothing.

**Purge deletes.** It is guarded twice before it reaches here — admin only, and
the archive refuses to purge anything still published, so a clip has to be
unlisted first — but this end does not keep a copy.
