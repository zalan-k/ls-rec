import functools, inspect, re, threading, time

from yt_dlp.extractor.youtube import YoutubeIE
from yt_dlp.utils import LazyList, bug_reports_message, traverse_obj

RETRY_S, EXPIRE_MARGIN_S = 120, 300

_EXPECT = {
    "_prepare_live_from_start_formats": ["self", "formats", "video_id", "live_start_time", "url",
                                         "webpage_url", "smuggled_data", "is_live"],
    "_live_adaptive_fragments": ["self", "video_id", "itag", "client_name", "live_start_time",
                                 "url_feed", "base_url", "fragment_duration", "last_seq_cache", "ctx"],
    "_initial_extract": ["self", "url", "smuggled_data", "webpage_url", "webpage_client", "video_id"],
}


def _compatible():
    try:
        ok = all(list(inspect.signature(getattr(YoutubeIE, n)).parameters) == p for n, p in _EXPECT.items())
        lf = list(inspect.signature(YoutubeIE._list_formats).parameters)[:6]
        return ok and lf == ["self", "video_id", "microformats", "video_details", "player_responses", "player_url"]
    except Exception:
        return False


_COMPATIBLE = _compatible()


def _expire_left(url):
    m = re.search(r"[?&/]expire[=/](\d+)", url or "")
    return int(m.group(1)) - time.time() if m else None


class LsRecYoutubeIE(YoutubeIE, plugin_name="lsrec"):

    def _lsrec_fresh(self, webpage_url):
        from yt_dlp import YoutubeDL
        params = {**self._downloader.params, "quiet": True, "simulate": True, "skip_download": True}
        with YoutubeDL(params) as ydl:
            info = ydl.extract_info(webpage_url, download=False, process=False)
        return (info.get("live_status") == "is_live",
                [f for f in info.get("formats") or [] if f.get("_itag") and f.get("_client")])

    def _prepare_live_from_start_formats(self, formats, video_id, live_start_time, url, webpage_url, smuggled_data, is_live):
        if not _COMPATIBLE:
            self.report_warning("[lsrec] yt-dlp internals changed; refresh repair off, stock behaviour", only_once=True)
            return super()._prepare_live_from_start_formats(
                formats, video_id, live_start_time, url, webpage_url, smuggled_data, is_live)

        lock = threading.Lock()
        start_time = time.time()
        formats = [f for f in formats if f.get("is_from_start")]
        adaptive_last_seq_cache = {}
        refresh_s = int(self._configuration_arg("lsrec_refresh_s", ["18000"])[0])
        state = {"force_miss": int(self._configuration_arg("lsrec_force_miss", ["0"])[0]),
                 "retry_at": None, "gave_up": False, "player_url": None}
        last_good = {(f["_itag"], f["_client"]): f["url"] for f in formats if f.get("url")}

        def forced_miss():
            if state["force_miss"] > 0:
                state["force_miss"] -= 1
                return True
            return False

        def refetch_manifest(itag, client_name, delay):
            nonlocal formats, start_time, is_live
            if time.time() <= start_time + delay:
                return
            _, _, _, _, prs, player_url = self._initial_extract(
                url, smuggled_data, webpage_url, self._webpage_client, video_id)
            video_details = traverse_obj(prs, (..., "videoDetails"), expected_type=dict)
            microformats = traverse_obj(prs, (..., "microformat", "playerMicroformatRenderer"), expected_type=dict)
            _, live_status, formats, _ = self._list_formats(video_id, microformats, video_details, prs, player_url)
            is_live = live_status == "is_live"
            start_time = time.time()
            state["player_url"] = player_url
            if forced_miss():
                formats = []

        def find(itag, client_name):
            return next((f for f in formats if f.get("_itag") == itag and f.get("_client") == client_name), None)

        def try_fresh(why):
            nonlocal formats, start_time, is_live
            try:
                live, fresh = self._lsrec_fresh(webpage_url)
            except Exception as e:
                self.report_warning(f"[lsrec] fresh extraction failed ({why}): {e}")
                return
            if forced_miss():
                fresh = []
            if fresh or not live:
                formats, is_live, start_time = fresh, live, time.time()
            self.report_warning(f"[lsrec] fresh extraction ({why}): live={live}, "
                                f"{len(fresh)} from-start formats")

        def recover(itag, client_name):
            key = (itag, client_name)
            if state["retry_at"] is None:
                pairs = sorted({f"{f['_itag']}/{f['_client']}" for f in formats if f.get("_itag") and f.get("_client")})
                self.report_warning(
                    f"[lsrec] refresh missed {itag}/{client_name}: returned {len(formats)} formats, "
                    f"from-start {pairs or 'none'}, player {state['player_url']}")
                try_fresh("first miss")
            elif time.time() >= state["retry_at"]:
                try_fresh("retry")
            else:
                return last_good.get(key)
            if (f := find(itag, client_name)) or not is_live:
                state["retry_at"] = None
                if f:
                    self.report_warning(f"[lsrec] recovered {itag}/{client_name}")
                return f and f.get("url")
            left = _expire_left(last_good.get(key))
            if left is not None and left > EXPIRE_MARGIN_S:
                state["retry_at"] = time.time() + RETRY_S
                self.report_warning(f"[lsrec] {itag}/{client_name}: keeping current URL "
                                    f"({int(left)}s left), next try in {RETRY_S}s")
                return last_good[key]
            state["gave_up"] = True
            self.report_warning(f"[lsrec] {itag}/{client_name}: headroom spent; stock behaviour from here")
            return None

        def url_feed(itag, client_name, delay):
            for retry in self.RetryManager(fatal=False):
                with lock:
                    refetch_manifest(itag, client_name, min(delay, refresh_s))
                    f = find(itag, client_name)
                    if f and f.get("url"):
                        last_good[(itag, client_name)] = f["url"]
                    elif not f and is_live and not state["gave_up"]:
                        if u := recover(itag, client_name):
                            return u, True
                        f = find(itag, client_name)

                if not f:
                    if not is_live:
                        retry.error = f"{video_id}: Video is no longer live"
                    else:
                        retry.error = f"Cannot find refreshed url for format {itag}{bug_reports_message()}"
                    continue

                if not f.get("url"):
                    break

                return f["url"], is_live
            return None

        for f in formats:
            f["is_live"] = is_live
            gen = functools.partial(
                self._live_adaptive_fragments,
                video_id,
                f["_itag"],
                f["_client"],
                live_start_time,
                url_feed if is_live else None,
                f.get("url") if not is_live else None,
                f.get("target_duration"),
                adaptive_last_seq_cache,
            )
            if is_live:
                f["fragments"] = gen
                f["protocol"] = "http_dash_segments_generator"
            else:
                f["fragments"] = LazyList(gen({}))
                f["protocol"] = "http_dash_segments"
                del f["is_from_start"]
