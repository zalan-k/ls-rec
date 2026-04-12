#!/usr/bin/env python3
"""
ls-download - Download livestream VODs and chat (post-hoc).

CLI mode (for use by ls-audit or direct):
    ls-download --url URL --prefix 515 --type both --output /path/to/raws
    ls-download --url URL --type video
    ls-download --url URL --type chat --prefix 515

Interactive mode (no arguments):
    ls-download

Options:
    --url       Stream URL (YouTube or Twitch)
    --prefix    Index prefix for filename + cache upsert (e.g., 515)
    --type      What to download: video, chat, or both (default: both)
    --output    Output directory (default: config.nas_path)

yt-dlp invocation, cache writes and config loading all live in ls_common.
This script is a thin wrapper around ls_common.Downloader.
"""

import argparse
import datetime
import os
import signal
import subprocess
import sys

from yt_dlp.utils import sanitize_filename

import ls_common


TWITCH_DOWNLOADER_CLI = "/mnt/nvme/livestream-recorder/twitch-downloader/TwitchDownloaderCLI"


class PostHocDownloader:
    """Post-hoc VOD + chat downloader.

    This is intentionally separate from the live-recording path in
    livestream-recorder.py because the two modes have different yt-dlp
    flags and different completion semantics. The common ground
    (binary path, cookies, format strings) lives in ls_common.YtDlp.
    """

    def __init__(self, config, output_dir=None):
        self.config = config
        self.ytdlp = ls_common.YtDlp(config)
        self.cache = ls_common.StreamCache()
        self.output_dir = output_dir or config["nas_path"]
        os.makedirs(self.output_dir, exist_ok=True)

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print("\n\nInterrupted by user.")
        sys.exit(1)

    # ── low-level runner ──────────────────────────────────────────────────────

    def _run(self, cmd, title, check_path, label):
        try:
            subprocess.run(cmd, cwd=self.output_dir, check=True, env=os.environ.copy())
            if os.path.exists(check_path):
                # Rename yt-dlp chat format to plain .json
                if ".live_chat.json" in check_path:
                    final = check_path.replace(".live_chat.json", ".json")
                    os.rename(check_path, final)
                print(f"  ✔ {label} completed: {title}")
                return True
            print(f"  ⚠ {label} file not found after download.")
            return False
        except subprocess.CalledProcessError as e:
            print(f"  ✗ {label} failed (exit {e.returncode})")
            return False
        except Exception as e:
            print(f"  ✗ {label} error: {e}")
            return False

    # ── video / chat ──────────────────────────────────────────────────────────

    def download_video(self, url, title):
        print(f"\n  ↓ Downloading video: {title}")
        output_path = os.path.join(self.output_dir, f"{title}.mp4")
        cmd = self.ytdlp.build_vod_cmd(url, f"{title}.%(ext)s")
        return self._run(cmd, title, output_path, "Video")

    def download_chat(self, url, title):
        print(f"\n  ↓ Downloading chat: {title}")
        output_path = os.path.join(self.output_dir, f"{title}.json")

        if "twitch.tv" in url:
            vod_id = url.rstrip("/").split("/")[-1]
            if not os.path.exists(TWITCH_DOWNLOADER_CLI):
                print(f"  ⚠ TwitchDownloaderCLI not found at {TWITCH_DOWNLOADER_CLI}")
                print("  Falling back to yt-dlp for Twitch chat...")
                return self._download_chat_ytdlp(url, title)
            cmd = [TWITCH_DOWNLOADER_CLI, "chatdownload", "--id", vod_id, "-o", output_path]
            return self._run(cmd, title, output_path, "Chat (TwitchDownloaderCLI)")
        return self._download_chat_ytdlp(url, title)

    def _download_chat_ytdlp(self, url, title):
        cmd = self.ytdlp.build_chat_cmd(url, f"{title}.%(ext)s")
        check_path = os.path.join(self.output_dir, f"{title}.live_chat.json")
        return self._run(cmd, title, check_path, "Chat (yt-dlp)")

    # ── metadata probe ────────────────────────────────────────────────────────

    def get_stream_info(self, url):
        """Probe a URL once. Returns dict with raw_title, timestamp, and yt-dlp fields."""
        print("  ⌛ Getting stream info...")
        data = self.ytdlp.probe(url)
        if not data:
            ts = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
            return {"raw_title": f"Manual_Download @ {ts}", "timestamp": ts, "data": None}

        release_ts = data.get("release_timestamp")
        if release_ts:
            formatted = datetime.datetime.fromtimestamp(release_ts).strftime("%Y-%m-%d_%H-%M")
        else:
            formatted = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")

        raw_title = f"{data.get('title', 'Unknown')} [{data.get('id', 'unknown')}] @ {formatted}"
        return {"raw_title": raw_title, "timestamp": formatted, "data": data}

    # ── orchestration ─────────────────────────────────────────────────────────

    def download(self, url, prefix=None, download_type="both"):
        """Download a stream. Returns dict of results.

        If `prefix` is provided it's used both as the filename index and as
        the cache entry key — the cache entry is upserted with the probed
        metadata (title, id, starttime) so ls-audit has a record of this
        download without a separate refresh pass.
        """
        info = self.get_stream_info(url)
        raw_title = info["raw_title"]
        data = info["data"]
        title = sanitize_filename(raw_title)

        if prefix:
            title = f"{prefix}_{title}"

        print(f"\n  Saving as: {title}")
        print("  " + "-" * 50)

        video_success = False
        chat_success = False

        if download_type in ("video", "both"):
            video_success = self.download_video(url, title)
        if download_type in ("chat", "both"):
            chat_success = self.download_chat(url, title)

        # Cache write — tie this download to an internal index for ls-audit.
        if prefix and data:
            self._upsert_cache(int(prefix), url, data)

        return {
            "title": title,
            "video_success": video_success,
            "chat_success": chat_success,
        }

    def _upsert_cache(self, index, url, data):
        """Merge probed metadata into the cache entry for `index`."""
        platform = "twitch" if "twitch.tv" in url else "youtube"
        tag = "yt" if platform == "youtube" else "tw"

        video_id = data.get("id")
        title = data.get("title") or data.get("description") or "Unknown"
        duration = data.get("duration")

        release_ts = data.get("release_timestamp")
        if release_ts:
            starttime = datetime.datetime.fromtimestamp(release_ts).isoformat()
        elif data.get("upload_date"):
            starttime = datetime.datetime.strptime(data["upload_date"], "%Y%m%d").isoformat()
        else:
            starttime = datetime.datetime.now().isoformat()

        self.cache.upsert(
            index,
            **{
                f"{tag}_id":        video_id,
                f"{tag}_title":     title,
                f"{tag}_starttime": starttime,
                f"{tag}_duration":  duration,
            },
        )
        self.cache.save()


# ── CLI Mode ──────────────────────────────────────────────────────────────────

def cli_mode(config):
    parser = argparse.ArgumentParser(description="Download livestream VODs and chat")
    parser.add_argument("--url", required=True, help="Stream URL (YouTube or Twitch)")
    parser.add_argument("--prefix", help="Index prefix for filename (e.g., 515)")
    parser.add_argument("--type", dest="dl_type", default="both",
                        choices=["video", "chat", "both"], help="What to download")
    parser.add_argument("--output", help="Output directory (default: config.nas_path)")

    args = parser.parse_args()

    downloader = PostHocDownloader(config, output_dir=args.output)
    result = downloader.download(args.url, prefix=args.prefix, download_type=args.dl_type)

    if args.dl_type == "both":
        sys.exit(0 if result["video_success"] and result["chat_success"] else 1)
    if args.dl_type == "video":
        sys.exit(0 if result["video_success"] else 1)
    sys.exit(0 if result["chat_success"] else 1)


# ── Interactive Mode ──────────────────────────────────────────────────────────

def interactive_mode(config):
    print("\n" + "=" * 50)
    print("         MANUAL STREAM DOWNLOADER")
    print("=" * 50 + "\n")

    try:
        print("What would you like to download?")
        print("1 - Video only")
        print("2 - Chat only")
        print("3 - Both video and chat")
        print()

        while True:
            opt = input("Select option (1/2/3): ").strip()
            if opt in ("1", "2", "3"):
                break
            print("  ✗ Invalid option.\n")
        dl_type = {"1": "video", "2": "chat", "3": "both"}[opt]
        print()

        while True:
            prefix = input("Enter prefix number (e.g., 512) or Enter to skip: ").strip()
            if not prefix:
                prefix = None
                break
            if prefix.isdigit():
                break
            print("  ✗ Numbers only.\n")
        print()

        url = input("Enter stream URL (YouTube or Twitch): ").strip()
        if not url:
            print("  ✗ No URL provided!")
            return

        output = input("Output directory (Enter for default): ").strip() or None

        downloader = PostHocDownloader(config, output_dir=output)
        result = downloader.download(url, prefix=prefix, download_type=dl_type)

        print("\n" + "=" * 50)
        print("         DOWNLOAD COMPLETE")
        print("=" * 50)

        if dl_type in ("video", "both"):
            status = "✔" if result["video_success"] else "✗"
            print(f"  {status} Video: {result['title']}.mp4")
        if dl_type in ("chat", "both"):
            status = "✔" if result["chat_success"] else "✗"
            print(f"  {status} Chat: {result['title']}.json")
        print(f"  ▶ Files saved to: {downloader.output_dir}")

    except KeyboardInterrupt:
        print("\n\nCancelled.")


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    config = ls_common.load_config()
    if "--url" in sys.argv:
        cli_mode(config)
    else:
        interactive_mode(config)


if __name__ == "__main__":
    main()
