#!/usr/bin/env python3

import os, re, glob, time, json, logging, subprocess, datetime, sys, shutil, signal, threading, ctypes, urllib.parse, socket
from pathlib import Path
from yt_dlp.utils import sanitize_filename

# Setup logging with proper UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("livestream_recorder.log", encoding='utf-8'),
        logging.StreamHandler(stream=open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1))
    ]
)
logger = logging.getLogger("LivestreamRecorder")
SOCKET_PATH = "/tmp/livestream-recorder.sock"

class CommandServer:
    """Unix socket server - thin dispatcher to existing recorder methods"""
    
    def __init__(self, recorder):
        self.recorder = recorder
        self.running = False
        self.server_socket = None
    
    def start(self):
        if os.path.exists(SOCKET_PATH):
            os.unlink(SOCKET_PATH)
        self.server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server_socket.bind(SOCKET_PATH)
        os.chmod(SOCKET_PATH, 0o666)
        self.server_socket.listen(1)
        self.server_socket.settimeout(1.0)
        self.running = True
        threading.Thread(target=self._serve, daemon=True).start()
        logger.info(f"  > Command server listening on {SOCKET_PATH}")
    
    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        if os.path.exists(SOCKET_PATH):
            os.unlink(SOCKET_PATH)
    
    def _serve(self):
        while self.running:
            try:
                conn, _ = self.server_socket.accept()
                try:
                    data = conn.recv(1024).decode('utf-8').strip()
                    if data:
                        response = self._handle(data)
                        conn.sendall(response.encode('utf-8'))
                finally:
                    conn.close()
            except socket.timeout:
                continue
            except OSError:
                break
    
    def _handle(self, command):
        parts = command.split()
        cmd = parts[0].lower()
        rec = self.recorder
        
        if cmd == "status":
            if not rec.active_streams:
                return "No active streams."
            lines = [f"Active streams ({len(rec.active_streams)}):"]
            for key, s in rec.active_streams.items():
                elapsed = str(datetime.datetime.now() - s["start_time"]).split('.')[0]
                vid = "recording" if s.get("video_process") and s["video_process"].poll() is None else "stopped"
                lines.append(f"  [{s['platform'].upper()}] {s['obsidian_title']}")
                lines.append(f"    Video: {vid} | Elapsed: {elapsed} | URL: {s['url']}")
            return "\n".join(lines)
        
        elif cmd == "check":
            platforms = [parts[1]] if len(parts) > 1 and parts[1] in ("youtube", "twitch") else ["youtube", "twitch"]
            lines = []
            for plat in platforms:
                result = rec.probe_platform(plat)
                if result:
                    already = "(already recording)" if result["stream_key"] in rec.active_streams else "(not recording)"
                    lines.append(f"  ✔ {plat.upper()}: LIVE - {result['obsidian_title']} {already}")
                else:
                    lines.append(f"  ✗ {plat.upper()}: offline")
            return "\n".join(lines)
        
        elif cmd == "record":
            if len(parts) < 2:
                return "Usage: record <youtube|twitch|url>"
            target = parts[1]
            
            # Platform shorthand (existing behavior)
            if target in ("youtube", "twitch"):
                result = rec.probe_platform(target)
                if not result:
                    return f"No live stream found on {target}."
                if result["stream_key"] in rec.active_streams:
                    return f"Already recording: {result['obsidian_title']}"
                
                obsidian_index, is_dual = rec.get_stream_index(target, datetime.datetime.now())
                rec.start_stream_recording(
                    result["stream_url"], target, result["video_id"],
                    result["stream_title"], result["obsidian_title"],
                    result["obsidian_url"], obsidian_index, is_dual
                )
                return f"✔ Started recording {target.upper()}: {result['obsidian_title']} (#{obsidian_index:03d})"
            
            # Direct URL
            url = target
            try:
                probe_cmd = ["yt-dlp", "--cookies-from-browser", "firefox", "--dump-json",
                            "--playlist-items", "1", url]
                probe = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30)
                if probe.returncode != 0:
                    return f"✗ Could not fetch: {url}"
                
                data = json.loads(probe.stdout.strip())
                platform = "twitch" if "twitch.tv" in url else "youtube"
                title = data.get('fulltitle') or data.get('title') or 'Unknown'
                video_id = data.get('id', 'unknown')
                stream_key = f"{platform}_{video_id}"
                if stream_key in rec.active_streams:
                    return f"Already recording: {title}"
                
                if data.get('is_live', False):
                    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
                    stream_title = sanitize_filename(f"{title} [{video_id}] @ {timestamp}")
                    obsidian_url = f"https://www.youtube.com/watch?v={video_id}" if platform == "youtube" else url
                    obsidian_index, is_dual = rec.get_stream_index(platform, datetime.datetime.now())
                    
                    rec.start_stream_recording(
                        url, platform, video_id, stream_title, title,
                        obsidian_url, obsidian_index, is_dual
                    )
                    return f"✔ LIVE — recording started: {title} (#{obsidian_index:03d})"
                else:
                    watch_entry = {
                        "title"      : title,
                        "last_check" : time.time()
                    }
                    rec.watch_list[url] = watch_entry
                    release_ts = data.get('release_timestamp')
                    if release_ts:
                        watch_entry["start_time"] = release_ts
                        starts_in = release_ts - time.time()
                        h, m = divmod(int(starts_in) // 60, 60)
                        return f"✔ Not live yet, watching: {title} (starts in ~{h}h{m:02d}m)"
                    
                    return f"✔ Not live yet, watching: {title}"
            
            except Exception as e:
                return f"✗ Error: {e}"

        elif cmd == "unwatch":
            if len(parts) < 2:
                if not rec.watch_list:
                    return "No watched URLs."
                lines = ["Watched URLs:"]
                for i, (url, info) in enumerate(rec.watch_list.items(), 1):
                    lines.append(f"  {i}) {info['title']}")
                    lines.append(f"     {url}")
                return "\n".join(lines)
            
            url = parts[1]
            if url in rec.watch_list:
                removed = rec.watch_list.pop(url)
                return f"✔ Removed: {removed['title']}"
            # Try by index
            try:
                idx = int(url) - 1
                key = list(rec.watch_list.keys())[idx]
                removed = rec.watch_list.pop(key)
                return f"✔ Removed: {removed['title']}"
            except (ValueError, IndexError):
                return "✗ URL not in watch list."

        else:
            return "Commands: status | check [youtube|twitch] | record <youtube|twitch>"

class LivestreamRecorder:
    def __init__(self):
        self.config = {
            "youtube"           : '@TenmaMaemi',
            "twitch"            : 'tenma',
            "priority"          : "youtube",
            "output"            : "/mnt/nvme/livestream-recorder/tempfiles",
            "obsidian"          : "/mnt/nas/edit-video_library/Tenma Maemi/archives/Tenma Maemi Livestreams.md",
            "obsidian_vault"    : "archives",
            "shellcmd_id"       : "4gtship619",
            "nas_path"          : "/mnt/nas/edit-video_library/Tenma Maemi/archives/raws",
            "check_interval"    :  60,
            "cleanup_hour"      :   3,
            "cooldown_duration" :  30,
            "dual_stream_cycle" :  10
        }
        
        # Startup banner
        logger.info("=" * 60)
        logger.info("Livestream Recorder starting...")
        logger.info("=" * 60)

        # Intitial checks
        self.active_streams = {}
        self.watch_list = {}
        self.create_output_dirs()
        self.check_disk_space()
        
        # Initialize variables for cleaner console output
        self.was_streaming = False
        self.last_cleanup_date = None
        
        # Add cooldown tracking for testing
        self.monitoring_cooldown_until = None
        self.manual_termination_in_progress = False
        
        # Store the original SIGINT handler
        self.original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self.handle_sigint)

        # External probing
        self.command_server = CommandServer(self)

    def handle_sigint(self, sig, frame):
        """Simple SIGINT handler - let yt-dlp handle Ctrl+C naturally, just set flag"""
        if self.manual_termination_in_progress:
            # Already handling termination, force exit
            print("\nForce terminating...")
            signal.signal(signal.SIGINT, self.original_sigint_handler)
            os.kill(os.getpid(), signal.SIGINT)
            return
        
        if self.active_streams:
            print("\nReceived Ctrl+C. Letting yt-dlp handle termination naturally...")
            # Just set the flag - don't do any processing here
            # yt-dlp will receive Ctrl+C naturally since it's in the same process group
            # The monitor_completion threads will detect when processes exit and handle cleanup
            self.manual_termination_in_progress = True
            
            # Stop all chat downloads immediately
            for stream_key, stream in self.active_streams.items():
                if stream.get("chat_stop_event"):
                    stream["chat_stop_event"].set()
            
            # Set cooldown period
            self.monitoring_cooldown_until = datetime.datetime.now() + datetime.timedelta(
                seconds=self.config["cooldown_duration"]
            )
            
            print("yt-dlp processes should terminate naturally. Waiting for completion...")
            print("Press Ctrl+C again to force exit.")
            return  # Don't exit, let monitor threads handle the rest
        
        # No active streams, exit normally
        print("\nShutting down...")
        signal.signal(signal.SIGINT, self.original_sigint_handler)
        os.kill(os.getpid(), signal.SIGINT)

    def is_monitoring_allowed(self):
        """Check if stream monitoring is currently allowed (not in cooldown)"""
        if self.monitoring_cooldown_until is None:
            return True
        
        if datetime.datetime.now() >= self.monitoring_cooldown_until:
            # Cooldown period has ended
            self.monitoring_cooldown_until = None
            logger.info("Stream monitoring cooldown period ended. Resuming normal operation.")
            return True
        
        return False
    
    def get_stream_index(self, platform, start_time):
        """Get index for new stream, sharing with dual-stream partner if detected"""
        window_seconds = self.config["dual_stream_cycle"] * self.config["check_interval"]
        other_platform = "twitch" if platform == "youtube" else "youtube"
        
        # Check for dual-stream partner
        for stream in self.active_streams.values():
            if stream["platform"] != other_platform:
                continue
            time_diff = abs((start_time - stream["start_time"]).total_seconds())
            if time_diff <= window_seconds:
                # Found partner - share its index
                return stream["obsidian_index"], True
        
        # No partner - get new index from Obsidian
        return self.get_next_obsidian_index(), False

    def get_next_obsidian_index(self):
        """Get the next available index from the Obsidian log file"""
        if not os.path.exists(self.config["obsidian"]):
            return 1
        
        try:
            with open(self.config["obsidian"], 'r', encoding='utf-8') as f:
                content = f.read()
            
            if matches := re.findall(r'\*\*(\d{3})\*\*', content):
                return max(int(m) for m in matches) + 1
            return 1
        except Exception as e:
            logger.error(f"Error reading obsidian file for index: {str(e)}")
            return 1
    
    def create_obsidian_entry(self, index, platform, title, url):
        """Create a new Obsidian entry with both platform lines"""
        if not os.path.exists(os.path.dirname(self.config["obsidian"])):
            logger.warning(f"NAS path unavailable: {os.path.dirname(self.config['obsidian'])}")
            return False
        
        now = datetime.datetime.now()
        utc_offset = now.astimezone().utcoffset()
        hours_offset = int(utc_offset.total_seconds() / 3600)
        tz_str = f"GMT{hours_offset:+d}"
        today = now.strftime(f"%Y.%m.%d %H:%M ({tz_str})")

        yt_line = f"[📁]() [📄]() [ {title} ]({url})" if platform == "youtube" else ""
        tw_line = f"[📁]() [📄]() [ {title} ]({url})" if platform == "twitch" else ""
        
        try:
            try:
                with open(self.config["obsidian"], 'r', encoding='utf-8') as f:
                    content = f.read()
            except FileNotFoundError:
                content = ""
            
            entry = (
                f"- [ ] **{index:03d}** : {today}  #stream\n"
                f"\t`YT` {yt_line}\n"
                f"\t`TW` {tw_line}\n"
                f"\t- [ ] \n"
                f"---\n"
            )
            
            with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                f.write(entry + content)
            
            logger.info(f"Created new Obsidian entry #{index:03d}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating obsidian entry: {str(e)}")
            return False

    def update_obsidian_entry(self, index, platform, title=None, url=None, stream_title=None, duration_seconds=None, video_ext=".mp4"):
        """Update platform line or file path in an existing entry"""
        if not os.path.exists(self.config["obsidian"]):
            logger.warning("Obsidian file not found for update")
            return False

        try:
            tag = "YT" if platform == "youtube" else "TW"
            with open(self.config["obsidian"], 'r', encoding='utf-8') as f:
                content = f.read()

            # Update platform title/url
            if title and url:
                pattern = rf'(\t`{tag}` )[^\n]*\n'
                replacement = f'\\1[📁]() [📄]() [ {title} ]({url})\n'
                content = re.sub(pattern, replacement, content, count=1)

            # Update file path
            if stream_title:
                shell_base = (f"obsidian://shell-commands/?vault={self.config['obsidian_vault']}"
                            f"&execute={self.config['shellcmd_id']}&_arg0=raws/")
                encoded = urllib.parse.quote(stream_title, safe='')
                ext = video_ext if video_ext and video_ext.startswith('.') else f".{video_ext or 'mp4'}"
                pattern = rf'(\t`{tag}` )\[📁\]\(\) \[📄\]\(\)'
                replacement = f'\\1[📁]({shell_base}{encoded}{ext}) [📄]({shell_base}{encoded}.json)'
                content = re.sub(pattern, replacement, content, count=1)

            # Update duration (keep longer of existing vs new)
            if duration_seconds is not None:
                idx_str = str(index).zfill(3)
                h, rem = divmod(int(duration_seconds), 3600)
                m, s = divmod(rem, 60)
                new_dur_str = f"[{h:02d}:{m:02d}:{s:02d}]"
                
                existing_match = re.search(
                    rf'\*\*{idx_str}\*\*.*?\[(\d{{2}}):(\d{{2}}):(\d{{2}})\]', content
                )
                
                if existing_match:
                    existing_secs = (int(existing_match.group(1)) * 3600 
                                + int(existing_match.group(2)) * 60 
                                + int(existing_match.group(3)))
                    if duration_seconds > existing_secs:
                        content = re.sub(
                            rf'(\*\*{idx_str}\*\*.*?)\[\d{{2}}:\d{{2}}:\d{{2}}\]',
                            rf'\1{new_dur_str}',
                            content, count=1
                        )
                else:
                    content = re.sub(
                        rf'(\*\*{idx_str}\*\*.*?)\s+#stream',
                        rf'\1 {new_dur_str}  #stream',
                        content, count=1
                    )

            with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                f.write(content)
            
            logger.info(f"Updated Obsidian entry #{index:03d} ({platform})")
            return True
            
        except Exception as e:
            logger.error(f"Error updating obsidian entry: {str(e)}")
            return False
    
    def probe_watchlist(self):
        """Check watched URLs for live status, record when live."""
        now = time.time()

        for url in list(self.watch_list.keys()):
            entry = self.watch_list[url]
            start_time = entry.get("start_time")
            if start_time:
                until = start_time - now
                if until > 4 * 3600:
                    interval = 3600       # >4h: check hourly
                elif until > 900:
                    interval = 300        # 15m–4h: check every 5 min
                else:
                    interval = 60         # <15m: check every minute
            else:
                interval = 120

            last_check = entry.get("last_check", 0)
            if now - last_check < interval:
                continue
            try:
                probe_cmd = ["yt-dlp", "--cookies-from-browser", "firefox", "--dump-json", url]
                result = subprocess.run(probe_cmd, capture_output=True, text=True, timeout=30)
                if result.returncode != 0:
                    continue
                
                data = json.loads(result.stdout.strip())
                if not data.get('is_live', False):
                    release_ts = data.get('release_timestamp')
                    if release_ts:
                        entry["start_time"] = release_ts
                    continue
                
                platform = "twitch" if "twitch.tv" in url else "youtube"
                title = data.get('fulltitle') or data.get('title') or 'Unknown'
                video_id = data.get('id', 'unknown')
                stream_key = f"{platform}_{video_id}"
                
                if stream_key in self.active_streams:
                    continue
                
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
                stream_title = sanitize_filename(f"{title} [{video_id}] @ {timestamp}")
                obsidian_url = f"https://www.youtube.com/watch?v={video_id}" if platform == "youtube" else url
                obsidian_index, is_dual = self.get_stream_index(platform, datetime.datetime.now())
                
                logger.info(f"Watched stream went live: {title}")
                self.start_stream_recording(
                    url, platform, video_id, stream_title, title,
                    obsidian_url, obsidian_index, is_dual
                )
                del self.watch_list[url]
                
            except Exception as e:
                logger.warning(f"Watch list probe failed for {url}: {e}")

    def probe_platform(self, platform):
        """Probe a single platform for live stream. Returns dict with stream info or None."""
        services = {
            'youtube': {
                'url': f"https://www.youtube.com/{self.config['youtube']}/live",
                'extra_args': ["--playlist-items", "1"]
            },
            'twitch': {
                'url': f"https://www.twitch.tv/{self.config['twitch']}",
                'extra_args': []
            }
        }
        svc = services[platform]
        try:
            cmd = ["yt-dlp", "--cookies-from-browser", "firefox", "--dump-json"] + svc['extra_args'] + [svc['url']]
            process = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if process.returncode != 0:
                return None
            
            data = json.loads(process.stdout.strip())
            if not data.get('is_live', False):
                return None
            
            video_id = data.get('id')
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
            
            if platform == 'youtube':
                stream_url = f"https://www.youtube.com/watch?v={video_id}"
                obsidian_title = data.get('fulltitle')
                obsidian_url = stream_url
            else:
                stream_url = svc['url']
                obsidian_title = data.get('description')
                obsidian_url = f"{svc['url']}/videos/{video_id.lstrip('v')}"
            
            raw_title = f"{obsidian_title} [{video_id}] @ {timestamp}"
            stream_title = sanitize_filename(raw_title)
            
            return {
                "platform": platform,
                "video_id": video_id,
                "stream_url": stream_url,
                "stream_title": stream_title,
                "obsidian_title": obsidian_title,
                "obsidian_url": obsidian_url,
                "stream_key": f"{platform}_{video_id}"
            }
        except (subprocess.TimeoutExpired, json.JSONDecodeError, Exception) as e:
            logger.warning(f"Probe failed for {platform}: {e}")
            return None
    
    def check_stream_status(self):
        if not self.is_monitoring_allowed():
            return
        
        for platform in ("youtube", "twitch"):
            result = self.probe_platform(platform)
            if result and result["stream_key"] not in self.active_streams:
                logger.info(f"Found new active {platform.capitalize()} livestream: {result['stream_title']} ({result['stream_url']})")
                
                obsidian_index, is_dual = self.get_stream_index(platform, datetime.datetime.now())
                if is_dual:
                    logger.info(f"Dual-stream detected, sharing index {obsidian_index:03d}")
                
                self.start_stream_recording(
                    result["stream_url"], platform, result["video_id"],
                    result["stream_title"], result["obsidian_title"],
                    result["obsidian_url"], obsidian_index, is_dual
                )

    def start_stream_recording(self, url, platform, identifier, stream_title, obsidian_title, obsidian_url, obsidian_index, is_dual):
        """Start recording a complete stream (video + chat) in separate threads"""
        logger.info(f"Starting stream recording for {platform}: {stream_title}")
        
        # Update Obsidian log
        if is_dual:
            self.update_obsidian_entry(obsidian_index, platform, title=obsidian_title, url=obsidian_url)
        else:
            self.create_obsidian_entry(obsidian_index, platform, obsidian_title, obsidian_url)
        
        # Prepend index to stream title for filename
        stream_title = f"{obsidian_index:03d}_{stream_title}"

        # Create unified stream entry
        stream_key = f"{platform}_{identifier}"
        self.active_streams[stream_key] = {
            "url"                   : url,
            "platform"              : platform,
            "identifier"            : identifier,
            "stream_title"          : stream_title,
            "obsidian_title"        : obsidian_title,
            "start_time"            : datetime.datetime.now(),
            "video_process"         : None,
            "chat_thread"           : None,
            "chat_stop_event"       : None,
            "obsidian_index"        : obsidian_index
        }
        
        # Start video recording in a separate thread
        video_thread = threading.Thread(
            target=self.start_video_recording, 
            args=(stream_key,),
            daemon=True
        )
        video_thread.start()
        
        # Start chat download in a separate thread
        chat_thread = threading.Thread(
            target=self.start_chat_recording,
            args=(stream_key,),
            daemon=True
        )
        chat_thread.start()
    
    def start_video_recording(self, stream_key):
        """Start video recording for a stream"""
        stream = self.active_streams[stream_key]
        url = stream["url"]
        platform = stream["platform"]
        stream_title = stream["stream_title"]
        
        try:
            # Build yt-dlp command structure.
            # YouTube: use plain "best" — constraining to mp4/avc1 produces
            # poor quality and causes yt-dlp to drop the recording after ~4h
            # on long streams. The output container may end up as webm/mkv.
            # Twitch: keep mp4/avc1 selection, since HLS VODs segment cleanly.
            fmt = (
                "best"
                if platform == "youtube"
                else "bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best[ext=mp4]/best"
            )
            cmd = [
                "yt-dlp",
                "--format",               fmt,
                "-o",                     f"{stream_title}.%(ext)s",
                "--no-part",
                "--retries",              "10",
                "--fragment-retries",     "3",
                "--retry-sleep",          "exp=1::10",
                "--retry-sleep",          "fragment:exp=2::15",
                "--socket-timeout",       "15",
                "--cookies-from-browser", "firefox"
            ]

            if platform == "twitch":
                cmd.extend([
                    "--concurrent-fragments",   "4"
                    ])

            cmd.append(url)
            
            process = subprocess.Popen(cmd, cwd=self.config["output"])
            self.active_streams[stream_key]["video_process"] = process
            
            logger.info(f"Started video recording for {platform} stream: {stream_title}")
            
            # Monitor completion
            def monitor_completion():
                process.wait()
                
                # # For YouTube: check if stream is still live before treating as complete
                # if platform == "youtube" and not self.manual_termination_in_progress:
                #     if process.returncode in (0, 1):
                #         logger.warning(f"yt-dlp exited for YouTube stream — verifying stream is actually over...")
                #         time.sleep(15)  # brief wait before re-probe
                #         still_live = self.probe_platform("youtube")
                #         if still_live and still_live["video_id"] == identifier:
                #             logger.warning(f"Stream still live! yt-dlp dropped early. Restarting recording...")
                #             # Start a fresh recording under the same index/title
                #             self.start_video_recording(stream_key)
                #             return  # don't run completion logic yet

                if process.returncode == 0 or process.returncode == 1:
                    logger.info(f"Video download completed successfully: {stream_title}")
                else:
                    logger.error(f"Video download failed for {stream_title}: Return code {process.returncode}")
                
                self.handle_stream_completion(stream_key, upload_files=True)
                
                # If this was a manual termination, check if all streams are done
                if self.manual_termination_in_progress:
                    remaining_streams = len([s for s in self.active_streams.values() 
                                           if s.get("video_process") and s["video_process"].poll() is None])
                    if remaining_streams == 0:
                        print("All streams completed after manual termination.")
                        print(f"Stream monitoring paused for {self.config['cooldown_duration']} seconds.")
                        self.manual_termination_in_progress = False
            
            # Start monitoring thread
            monitor_thread = threading.Thread(target=monitor_completion, daemon=True)
            monitor_thread.start()
            
        except Exception as e:
            logger.error(f"Error starting video recording for {url}: {str(e)}")
            if stream_key in self.active_streams:
                del self.active_streams[stream_key]

    def start_chat_recording(self, stream_key):
        """Start incremental chat recording for a stream"""
        stream = self.active_streams[stream_key]
        url = stream["url"]
        platform = stream["platform"]
        stream_title = stream["stream_title"]
        try:
            stop_event = threading.Event()
            self.active_streams[stream_key]["chat_stop_event"] = stop_event

            if platform == "twitch":
                channel = self.config["twitch"]
                stream_start = int(stream["start_time"].timestamp() * 1000)
                output_path = os.path.join(self.config["output"], f"{stream_title}.json")
                
                def chat_download_thread():
                    sock = socket.socket()
                    sock.settimeout(5.0)
                    
                    try:
                        sock.connect(("irc.chat.twitch.tv", 6667))
                        sock.send(b"CAP REQ :twitch.tv/tags twitch.tv/commands\r\n")
                        sock.send(f"NICK justinfan{int(time.time()) % 99999}\r\n".encode())
                        sock.send(f"JOIN #{channel.lower()}\r\n".encode())
                        logger.info(f"Connected to Twitch IRC for #{channel}")
                        
                        with open(output_path, 'w', encoding='utf-8') as f:
                            f.write('[\n')
                            first_message = True
                            buffer = ""
                            
                            while not stop_event.is_set():
                                try:
                                    buffer += sock.recv(4096).decode('utf-8', errors='replace')
                                    
                                    while '\r\n' in buffer:
                                        line, buffer = buffer.split('\r\n', 1)
                                        
                                        if line.startswith("PING"):
                                            sock.send(b"PONG :tmi.twitch.tv\r\n")
                                            continue
                                        
                                        if not line.startswith('@'):
                                            continue
                                        
                                        match = re.match(r'@(?P<tags>[^ ]+) :(?P<user>[^!]+)![^ ]+ (?P<cmd>\w+) #[^ ]+(?: :(?P<msg>.*))?', line)
                                        if not match:
                                            continue
                                        
                                        # Parse tags
                                        tags = {}
                                        for tag in match.group('tags').split(';'):
                                            if '=' in tag:
                                                k, v = tag.split('=', 1)
                                                tags[k] = v.replace('\\s', ' ').replace('\\:', ';')
                                        
                                        cmd, username, message = match.group('cmd'), match.group('user'), match.group('msg') or ''
                                        ts = int((int(tags.get('tmi-sent-ts', time.time() * 1000)) - stream_start) * 1000)
                                        
                                        # Parse badges
                                        badges = []
                                        for b in tags.get('badges', '').split(','):
                                            if '/' in b:
                                                name, ver = b.split('/', 1)
                                                badges.append({'name': name, 'version': ver, 'title': name.replace('-', ' ').title()})
                                        
                                        # Parse emotes
                                        emotes = []
                                        msg_bytes = message.encode('utf-8')
                                        for e in tags.get('emotes', '').split('/'):
                                            if ':' not in e:
                                                continue
                                            eid, positions = e.split(':', 1)
                                            locs, ename = [], None
                                            for pos in positions.split(','):
                                                if '-' in pos:
                                                    s, end = int(pos.split('-')[0]), int(pos.split('-')[1])
                                                    locs.append(f"{s}-{end}")
                                                    if not ename:
                                                        try: ename = msg_bytes[s:end+1].decode('utf-8')
                                                        except: ename = f"emote_{eid}"
                                            if ename:
                                                emotes.append({'id': eid, 'name': ename, 'locations': locs})
                                        
                                        # Build base author
                                        author = {
                                            'id': tags.get('user-id', ''),
                                            'name': username,
                                            'display_name': tags.get('display-name', username),
                                            'badges': badges
                                        }
                                        
                                        msg = None
                                        
                                        if cmd == 'PRIVMSG':
                                            msg = {'message_type': 'text_message', 'timestamp': ts, 'message_id': tags.get('id', ''),
                                                'author': author, 'colour': tags.get('color', ''), 'message': message, 'emotes': emotes}
                                            if tags.get('bits'):
                                                msg['bits'] = int(tags['bits'])
                                        
                                        elif cmd == 'USERNOTICE':
                                            msg_id = tags.get('msg-id', '')
                                            msg = {'timestamp': ts, 'message_id': tags.get('id', ''), 'author': author,
                                                'colour': tags.get('color', ''), 'message': message or None, 'emotes': emotes}
                                            if msg_id == 'sub':
                                                msg['message_type'] = 'subscription'
                                                msg['subscription_type'] = tags.get('msg-param-sub-plan', '1000')
                                            elif msg_id == 'resub':
                                                msg['message_type'] = 'resubscription'
                                                msg['subscription_type'] = tags.get('msg-param-sub-plan', '1000')
                                                msg['cumulative_months'] = int(tags.get('msg-param-cumulative-months', 1))
                                            elif msg_id == 'submysterygift':
                                                msg['message_type'] = 'mystery_subscription_gift'
                                                msg['subscription_type'] = tags.get('msg-param-sub-plan', '1000')
                                                msg['mass_gift_count'] = int(tags.get('msg-param-mass-gift-count', 1))
                                                msg['origin_id'] = tags.get('msg-param-origin-id', '')
                                            elif msg_id == 'subgift':
                                                msg['message_type'] = 'subscription_gift'
                                                msg['subscription_type'] = tags.get('msg-param-sub-plan', '1000')
                                                msg['gift_recipient_id'] = tags.get('msg-param-recipient-id', '')
                                                msg['gift_recipient_display_name'] = tags.get('msg-param-recipient-display-name', '')
                                                msg['origin_id'] = tags.get('msg-param-origin-id', '')
                                            elif msg_id == 'raid':
                                                msg['message_type'] = 'raid'
                                                msg['number_of_raiders'] = int(tags.get('msg-param-viewerCount', 0))
                                            else:
                                                msg = None
                                        
                                        elif cmd == 'CLEARCHAT' and message:
                                            msg = {'message_type': 'ban_user', 'timestamp': ts,
                                                'author': {'target_id': tags.get('target-user-id', ''), 'name': message},
                                                'ban_duration': int(tags['ban-duration']) if tags.get('ban-duration') else None}
                                        
                                        elif cmd == 'CLEARMSG' and tags.get('target-msg-id'):
                                            msg = {'message_type': 'delete_message', 'timestamp': ts,
                                                'target_message_id': tags['target-msg-id']}
                                        
                                        if msg:
                                            if not first_message:
                                                f.write(',\n')
                                            json.dump(msg, f, ensure_ascii=False)
                                            first_message = False
                                            
                                except socket.timeout:
                                    continue
                                except Exception as e:
                                    logger.error(f"IRC error: {e}")
                                    break
                            
                            f.write('\n]')
                        logger.info(f"Chat download completed for: {stream_title}")
                        
                    except Exception as e:
                        logger.error(f"IRC connection error: {e}")
                    finally:
                        try: sock.close()
                        except: pass

            else:
                def chat_download_thread():
                    try:
                        cmd = [
                            "yt-dlp",
                            "--skip-download",
                            "--write-subs",
                            "--sub-langs", "live_chat",
                            "--cookies-from-browser", "firefox",
                            "-o", f"{stream_title}.%(ext)s",
                            url
                        ]
                        
                        process = subprocess.Popen(
                            cmd,
                            cwd=self.config["output"],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True
                        )
                        
                        self.active_streams[stream_key]["chat_process"] = process
                        
                        # Wait for process to complete or stop event
                        while process.poll() is None:
                            if stop_event.is_set():
                                logger.info("Chat download termination requested")
                                process.terminate()
                                try:
                                    process.wait(timeout=5)
                                except subprocess.TimeoutExpired:
                                    process.kill()
                                self._merge_chat_fragments(stream_title)
                                break
                            time.sleep(0.5)
                        
                        if process.returncode == 0:
                            logger.info(f"Chat download completed for: {stream_title}")
                            self._merge_chat_fragments(stream_title)
                        elif not stop_event.is_set():
                            logger.warning(f"Chat download ended with code {process.returncode}")
                            self._merge_chat_fragments(stream_title)
                            
                    except Exception as e:
                        logger.error(f"Error in chat download thread: {str(e)}")

            thread = threading.Thread(target=chat_download_thread, daemon=True)
            thread.start()
            self.active_streams[stream_key]["chat_thread"] = thread
            logger.info(f"Started chat recording for {platform} stream")
 
        except Exception as e:
            logger.error(f"Error starting chat recording for {url}: {str(e)}")

    def _merge_chat_fragments(self, stream_title):
        """Merge yt-dlp chat fragments into single JSON file"""
        output_dir = self.config["output"]
        
        base = f"{stream_title}.live_chat.json"
        main_part = os.path.join(output_dir, f"{base}.part")
        frag_pattern = os.path.join(output_dir, f"{base}.part-Frag*.part")
        final_output = os.path.join(output_dir, f"{stream_title}.json")
        
        try:
            all_lines = []
            
            # Read main part file
            if os.path.exists(main_part):
                with open(main_part, 'r', encoding='utf-8') as f:
                    all_lines.extend(f.readlines())
            
            # Read fragment files in order
            frag_files = sorted(glob.glob(frag_pattern))
            for frag_file in frag_files:
                with open(frag_file, 'r', encoding='utf-8') as f:
                    all_lines.extend(f.readlines())
            
            if not all_lines:
                logger.warning(f"No chat data found for {stream_title}")
                return False
            
            # Write merged output
            with open(final_output, 'w', encoding='utf-8') as f:
                f.writelines(all_lines)
            
            # Clean up part files
            if os.path.exists(main_part):
                os.remove(main_part)
            for frag_file in frag_files:
                os.remove(frag_file)
            
            return True
            
        except Exception as e:
            logger.error(f"Error merging chat fragments: {str(e)}")
            return False

    def _find_recorded_video(self, stream_title):
        """Return the path to the recorded video file for stream_title, or None.

        yt-dlp may emit mp4/mkv/webm/ts depending on the stream; we glob
        candidate extensions rather than assuming mp4. Intermediate
        fragment files (e.g. `.f140.m4a`) are skipped.
        """
        output_dir = self.config["output"]
        video_exts = (".mp4", ".mkv", ".webm", ".ts", ".flv", ".mov")
        for ext in video_exts:
            candidate = os.path.join(output_dir, f"{stream_title}{ext}")
            if os.path.exists(candidate):
                return candidate
        # Fallback: glob for anything matching the title, skip fragment/intermediate files
        for path in sorted(glob.glob(os.path.join(output_dir, f"{stream_title}.*"))):
            base = os.path.basename(path)
            if re.search(r'\.f\d+\.\w+$', base):
                continue
            if base.endswith('.json') or base.endswith('.part'):
                continue
            if os.path.splitext(base)[1].lower() in video_exts:
                return path
        return None

    def _probe_duration(self, src):
        try:
            probe = subprocess.run(
                ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", src],
                capture_output=True, text=True
            )
            if probe.returncode == 0 and probe.stdout.strip():
                return float(probe.stdout.strip())
        except Exception:
            pass
        return None

    def _upload_file(self, src, dst):
        try:
            if os.path.exists(dst):
                logger.info(f"File already exists on server, removing local copy: {os.path.basename(src)}")
                os.remove(src)
                return True, None

            ext = os.path.splitext(src)[1].lower()
            is_video = ext in (".mp4", ".mkv", ".webm", ".ts", ".flv", ".mov")

            if ext == '.mp4':
                duration = self._probe_duration(src)

                # Remux with faststart for Premiere compatibility (mp4 only)
                result = subprocess.run(
                    ["ffmpeg", "-i", src, "-c", "copy", "-movflags", "+faststart", dst],
                    capture_output=True, text=True
                )
                if result.returncode == 0:
                    os.remove(src)
                    logger.info(f"Successfully uploaded {os.path.basename(src)} (faststart remux)")
                    return True, duration
                else:
                    logger.error(f"ffmpeg remux failed: {result.stderr[-200:]}")
                    return False, None

            # Non-mp4 video (webm/mkv/ts/...) or chat/other files: just copy to NAS.
            duration = self._probe_duration(src) if is_video else None

            if os.name == 'posix':
                subprocess.run(["rsync", "-av", "--remove-source-files", src, dst], check=True)
                logger.info(f"Successfully uploaded {os.path.basename(src)} using rsync")
            else:
                shutil.copy2(src, dst)
                if os.path.getsize(src) == os.path.getsize(dst):
                    os.remove(src)
                    logger.info(f"Successfully uploaded {os.path.basename(src)} using shutil")
                else:
                    raise Exception("Size mismatch after copy")
            return True, duration

        except Exception as e:
            logger.error(f"Upload failed for {os.path.basename(src)}: {str(e)}")
            return False, None

    def handle_stream_completion(self, stream_key, upload_files=True):
        if stream_key not in self.active_streams:
            return
            
        stream = self.active_streams[stream_key]
        stream_title = stream["stream_title"]
        platform = stream["platform"]
        obsidian_index = stream.get("obsidian_index")
        
        logger.info(f"Handling completion of stream: {stream_title}")
        try:
            # Terminate video process if still running
            video_process = stream.get("video_process")
            if video_process and video_process.poll() is None:
                try:
                    video_process.terminate()
                    logger.info(f"Video process terminated for: {stream_title}")
                except Exception as e:
                    logger.error(f"Error terminating video process: {str(e)}")
            
            # Stop chat download with simplified logic
            if stream.get("chat_stop_event"):
                logger.info(f"Stopping chat download for: {stream_title}")
                stream["chat_stop_event"].set()
                
                if stream.get("chat_thread"):
                    stream["chat_thread"].join(timeout=5)  # Reasonable timeout
                    logger.info(f"Chat download stopped for: {stream_title}")
            
            # Upload files to server if requested
            if upload_files and os.path.exists(self.config["nas_path"]):
                chat_file = os.path.join(self.config["output"], f"{stream_title}.json")

                uploaded_path = None
                uploaded_ext = None
                duration = None

                # Locate the actual recorded video file — yt-dlp picks the
                # extension based on the chosen format (mp4/mkv/webm/ts...),
                # so we glob instead of assuming .mp4.
                video_file = self._find_recorded_video(stream_title)
                if video_file:
                    video_ext = os.path.splitext(video_file)[1]
                    video_dst = os.path.join(self.config["nas_path"], f"{stream_title}{video_ext}")
                    success, duration = self._upload_file(video_file, video_dst)
                    if success:
                        uploaded_path = video_dst
                        uploaded_ext = video_ext
                
                # Upload chat file (simplified verification)
                if os.path.exists(chat_file):
                    chat_file_ready = os.path.getsize(chat_file) > 100  # Minimal valid JSON
                    if chat_file_ready:
                        chat_dst = os.path.join(self.config["nas_path"], f"{stream_title}.json")
                        self._upload_file(chat_file, chat_dst)
                    else:
                        logger.warning(f"Chat file too small, skipping upload: {chat_file}")
                
                # Update Obsidian log with server file path
                if uploaded_path and obsidian_index:
                    self.update_obsidian_entry(obsidian_index, platform, stream_title=stream_title, duration_seconds=duration, video_ext=uploaded_ext)
                    logger.info(f"Successfully uploaded and logged: {stream_title}")
            
            # Remove from active streams
            del self.active_streams[stream_key]
            logger.info(f"Stream cleanup completed for: {stream_title}")
                
        except Exception as e:
            logger.error(f"Error handling stream completion for {stream_title}: {str(e)}")
            if stream_key in self.active_streams:
                del self.active_streams[stream_key]
    
    def run(self):
        """Main loop to check for livestreams and manage recordings"""
        logger.info(f"  > Starting livestream monitoring (ping frequency: {self.config['check_interval']} seconds)")
        # logger.info(f"Weekly cleanup scheduled for {self.config['cleanup_hour']}:00")
        logger.info(f"  > Manual termination cooldown: {self.config['cooldown_duration']} seconds")
        self.command_server.start()
        print('-' * 100)
        
        try:
            while True:
                # If we have active streams, show status
                if self.active_streams:
                    if not self.was_streaming:
                        logger.info(f"Active streams: {len(self.active_streams)}")
                        self.was_streaming = True

                else:
                    # Check if we're in cooldown period
                    if not self.is_monitoring_allowed():
                        now = datetime.datetime.now()
                        cooldown_remaining = max(0, (self.monitoring_cooldown_until - now).total_seconds())
                        progress = int(20 * (1 - cooldown_remaining / self.config["cooldown_duration"]))
                        current_time = now.strftime("%H:%M:%S")
                        print(f"[{current_time}] Cooldown: [{'#'*progress}{'.'*(20-progress)}] {cooldown_remaining:.0f}s")
                        time.sleep(self.config["check_interval"])
                        continue
                    
                    if self.was_streaming:
                        logger.info("No active streams, checking for new livestreams")
                        self.was_streaming = False

                    # Check for new streams (async)
                    self.check_stream_status()
                    
                    if not self.active_streams:
                        current_time = datetime.datetime.now()
                        next_check = (current_time + datetime.timedelta(seconds=self.config["check_interval"]))
                        print(f"[{current_time.strftime('%H:%M:%S')}] No active livestreams detected. Next check at {next_check.strftime('%H:%M:%S')}.")
                
                # Check watched URLs
                if self.watch_list:
                    self.probe_watchlist()

                # Wait for next check
                time.sleep(self.config["check_interval"])
        
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down")
            self.shutdown()
    
    def create_output_dirs(self):
        Path(self.config["output"]).mkdir(parents=True, exist_ok=True)
        logger.info("  > Output directories validated.")
    
    def check_disk_space(self):
        try:
            output_dir = Path(self.config["output"])
            # MacOS / Linux check
            if os.name == 'posix':
                output_stat = os.statvfs(output_dir if output_dir.exists() else output_dir.parent)
                output_free_gb = (output_stat.f_bavail * output_stat.f_frsize) / (1024**3)
            # Windows check
            else:
                free_bytes = ctypes.c_ulonglong(0)
                ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                    ctypes.c_wchar_p(str(output_dir)), None, None, ctypes.pointer(free_bytes))
                output_free_gb = free_bytes.value / (1024**3)
            
            logger.info(f"  > Disk space available: {output_free_gb:.2f} GB.")
            if output_free_gb < 10:
                logger.warning(f"Low disk space: {output_free_gb:.2f} GB remaining.")
        except Exception as e:
            logger.error(f"Error checking disk space: {str(e)}")
        
    def shutdown(self):
        logger.info("Shutting down Livestream Recorder...")
        self.command_server.stop()
        for stream_key, stream in list(self.active_streams.items()):
            logger.info("Terminating stream...")
            self.handle_stream_completion(stream_key, upload_files=False)
        
        logger.info("Livestream Recorder terminated successfully.")

if __name__ == "__main__":
    recorder = LivestreamRecorder()
    recorder.run()