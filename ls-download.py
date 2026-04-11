#!/usr/bin/env python3
"""
ls-download - Download livestream VODs and chat.

CLI mode (for use by ls-audit or direct):
    ls-download --url URL --prefix 515 --type both --output /path/to/raws
    ls-download --url URL --type video
    ls-download --url URL --type chat --prefix 515

Interactive mode (no arguments):
    ls-download

Options:
    --url       Stream URL (YouTube or Twitch)
    --prefix    Index prefix for filename (e.g., 515)
    --type      What to download: video, chat, or both (default: both)
    --output    Output directory (default: NAS raws path)
"""

import os, re, glob, json, subprocess, datetime, sys, signal, atexit, argparse
from yt_dlp.utils import sanitize_filename

VIDEO_EXTS = (".mp4", ".mkv", ".webm", ".ts", ".flv", ".mov")

# ── Config ────────────────────────────────────────────────────────────────────
DEFAULT_OUTPUT          = '/mnt/nas/edit-video_library/Tenma Maemi/archives/raws'
TWITCH_DOWNLOADER_CLI   = '/mnt/nvme/livestream-recorder/twitch-downloader/TwitchDownloaderCLI'

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(SCRIPT_DIR, "config.json")) as f:
    _CONFIG = json.load(f)
YTDLP = os.path.join(_CONFIG["venv"], "bin", "yt-dlp")

class ManualRecorder:
    def __init__(self, output_dir=None):
        self.output_dir = output_dir or DEFAULT_OUTPUT
        os.makedirs(self.output_dir, exist_ok=True)
        
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        print("\n\nInterrupted by user.")
        sys.exit(1)
    
    def _run_download(self, cmd, title, check_path, label):
        """Run a download command and verify output.

        check_path may be an exact path or a glob pattern (for formats whose
        extension is decided by yt-dlp). Resolves to the first non-intermediate
        video file matched.
        """
        try:
            subprocess.run(cmd, cwd=self.output_dir, check=True, env=os.environ.copy())

            resolved = self._resolve_output(check_path)
            if resolved:
                # Rename yt-dlp chat format to plain .json
                if resolved.endswith(".live_chat.json"):
                    final = resolved[:-len(".live_chat.json")] + ".json"
                    os.rename(resolved, final)
                print(f"  ✔ {label} completed: {title}")
                return True
            else:
                print(f"  ⚠ {label} file not found after download.")
                return False
        except subprocess.CalledProcessError as e:
            print(f"  ✗ {label} failed (exit {e.returncode})")
            return False
        except Exception as e:
            print(f"  ✗ {label} error: {e}")
            return False

    @staticmethod
    def _resolve_output(check_path):
        """Return the actual downloaded file for an exact or glob check_path."""
        if "*" not in check_path and "?" not in check_path:
            return check_path if os.path.exists(check_path) else None

        candidates = []
        for path in glob.glob(check_path):
            base = os.path.basename(path)
            # Skip yt-dlp fragment/intermediate files like `title.f140.m4a`
            if re.search(r'\.f\d+\.\w+$', base):
                continue
            if base.endswith(".part") or base.endswith(".ytdl"):
                continue
            ext = os.path.splitext(base)[1].lower()
            if ext in VIDEO_EXTS or ext == ".json":
                candidates.append(path)
        if not candidates:
            return None
        # Prefer mp4, then the largest file (likely the muxed output)
        candidates.sort(key=lambda p: (0 if p.endswith(".mp4") else 1, -os.path.getsize(p)))
        return candidates[0]
    
    def download_video(self, url, title):
        """Download video from URL."""
        print(f"\n  ↓ Downloading video: {title}")

        # YouTube: plain "best" — avoid mp4/avc1 constraints that hurt
        # quality and cause long-stream breakage. Output ext set by yt-dlp.
        # Twitch: keep the mp4/avc1 format and remux to mp4 (VODs are HLS).
        if "twitch.tv" in url:
            fmt = "bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best[ext=mp4]/best"
            expected_ext = ".mp4"
        else:
            fmt = "best"
            expected_ext = None  # determined by yt-dlp

        cmd = [
            YTDLP,
            "-f", fmt,
            "-o", f"{title}.%(ext)s",
            "--no-part",
            "--no-mtime",
            "--concurrent-fragments", "16",
            "--cookies-from-browser", "firefox",
            url
        ]

        # Twitch VODs need remux
        if "twitch.tv" in url:
            cmd.insert(-1, "--remux-video")
            cmd.insert(-1, "mp4")

        # For YouTube we can't predict the extension up front; pass a glob
        # pattern and let _run_download resolve it after the fact.
        if expected_ext:
            check_path = os.path.join(self.output_dir, f"{title}{expected_ext}")
        else:
            check_path = os.path.join(self.output_dir, f"{title}.*")

        return self._run_download(cmd, title, check_path, "Video")
    
    def download_chat(self, url, title):
        """Download chat from URL."""
        print(f"\n  ↓ Downloading chat: {title}")
        output_path = os.path.join(self.output_dir, f"{title}.json")
        
        if "twitch.tv" in url:
            # Extract VOD ID from URL (e.g., .../video/316307569766)
            vod_id = url.rstrip('/').split('/')[-1]
            
            if not os.path.exists(TWITCH_DOWNLOADER_CLI):
                print(f"  ⚠ TwitchDownloaderCLI not found at {TWITCH_DOWNLOADER_CLI}")
                print("  Falling back to yt-dlp for Twitch chat...")
                return self._download_chat_ytdlp(url, title)
            
            cmd = [
                TWITCH_DOWNLOADER_CLI,
                "chatdownload",
                "--id", vod_id,
                "-o", output_path
            ]
            return self._run_download(cmd, title, output_path, "Chat (TwitchDownloaderCLI)")
        else:
            return self._download_chat_ytdlp(url, title)
    
    def _download_chat_ytdlp(self, url, title):
        """Download chat using yt-dlp (YouTube or fallback)."""
        cmd = [
            YTDLP,
            "--skip-download",
            "--write-subs",
            "--sub-langs", "live_chat",
            "--cookies-from-browser", "firefox",
            "-o", f"{title}.%(ext)s",
            url
        ]
        
        check_path = os.path.join(self.output_dir, f"{title}.live_chat.json")
        return self._run_download(cmd, title, check_path, "Chat (yt-dlp)")
    
    def get_stream_info(self, url):
        """Get stream info via yt-dlp, returns (raw_title, timestamp)."""
        try:
            print("  ⌛ Getting stream info...")
            cmd = [
                YTDLP,
                "--cookies-from-browser", "firefox",
                "--dump-json",
                url
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                data = json.loads(result.stdout.strip())
                release_ts = data.get('release_timestamp')
                
                if release_ts:
                    formatted = datetime.datetime.fromtimestamp(release_ts).strftime('%Y-%m-%d_%H-%M')
                else:
                    formatted = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
                
                raw_title = f"{data.get('title', 'Unknown')} [{data.get('id', 'unknown')}] @ {formatted}"
                return raw_title, formatted
            else:
                ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
                return f"Manual_Download @ {ts}", ts
                
        except Exception as e:
            print(f"  ⚠ Couldn't get stream info: {e}")
            ts = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
            return f"Manual_Download @ {ts}", ts
    
    def download(self, url, prefix=None, download_type="both"):
        """
        Download a stream. Returns dict of results.
        
        Args:
            url: Stream URL
            prefix: Optional index prefix (e.g., "515")
            download_type: "video", "chat", or "both"
        
        Returns:
            dict with keys: title, video_success, chat_success
        """
        raw_title, timestamp = self.get_stream_info(url)
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
        
        return {
            "title": title,
            "video_success": video_success,
            "chat_success": chat_success
        }

# ── CLI Mode ──────────────────────────────────────────────────────────────────

def cli_mode():
    parser = argparse.ArgumentParser(description="Download livestream VODs and chat")
    parser.add_argument("--url", required=True, help="Stream URL (YouTube or Twitch)")
    parser.add_argument("--prefix", help="Index prefix for filename (e.g., 515)")
    parser.add_argument("--type", dest="dl_type", default="both",
                        choices=["video", "chat", "both"], help="What to download")
    parser.add_argument("--output", help=f"Output directory (default: {DEFAULT_OUTPUT})")
    
    args = parser.parse_args()
    
    recorder = ManualRecorder(output_dir=args.output)
    result = recorder.download(args.url, prefix=args.prefix, download_type=args.dl_type)
    
    # Exit code: 0 if all requested downloads succeeded
    if args.dl_type == "both":
        sys.exit(0 if result["video_success"] and result["chat_success"] else 1)
    elif args.dl_type == "video":
        sys.exit(0 if result["video_success"] else 1)
    else:
        sys.exit(0 if result["chat_success"] else 1)

# ── Interactive Mode ──────────────────────────────────────────────────────────

def interactive_mode():
    print("\n" + "=" * 50)
    print("         MANUAL STREAM DOWNLOADER")
    print("=" * 50 + "\n")
    
    try:
        # Download type
        print("What would you like to download?")
        print("1 - Video only")
        print("2 - Chat only")
        print("3 - Both video and chat")
        print()
        
        while True:
            opt = input("Select option (1/2/3): ").strip()
            if opt in ('1', '2', '3'):
                break
            print("  ✗ Invalid option.\n")
        
        dl_type = {"1": "video", "2": "chat", "3": "both"}[opt]
        print()
        
        # Prefix
        while True:
            prefix = input("Enter prefix number (e.g., 512) or Enter to skip: ").strip()
            if not prefix:
                prefix = None
                break
            if prefix.isdigit():
                break
            print("  ✗ Numbers only.\n")
        
        print()
        
        # URL
        url = input("Enter stream URL (YouTube or Twitch): ").strip()
        if not url:
            print("  ✗ No URL provided!")
            return
        
        # Output directory
        output = input(f"Output directory (Enter for default): ").strip() or None
        
        recorder = ManualRecorder(output_dir=output)
        result = recorder.download(url, prefix=prefix, download_type=dl_type)
        
        # Summary
        print("\n" + "=" * 50)
        print("         DOWNLOAD COMPLETE")
        print("=" * 50)
        
        if dl_type in ("video", "both"):
            status = "✔" if result["video_success"] else "✗"
            print(f"  {status} Video: {result['title']}.mp4")
        
        if dl_type in ("chat", "both"):
            status = "✔" if result["chat_success"] else "✗"
            print(f"  {status} Chat: {result['title']}.json")
        
        print(f"  ▶ Files saved to: {recorder.output_dir}")
        
    except KeyboardInterrupt:
        print("\n\nCancelled.")

# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    # If --url is present, use CLI mode; otherwise interactive
    if "--url" in sys.argv:
        cli_mode()
    else:
        interactive_mode()

if __name__ == "__main__":
    main()
