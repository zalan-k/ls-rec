#!/usr/bin/env python3

import os, re, glob, time, json, logging, subprocess, datetime, sys, shutil, signal, threading, ctypes, urllib.parse
from pathlib import Path
from yt_dlp.utils import sanitize_filename
from chat_downloader import ChatDownloader

# Setup logging with proper UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("livestream_recorder.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("LivestreamRecorder")

def sanitize_for_logging(text):
    """Sanitize text for logging to avoid Unicode encoding errors"""
    if not isinstance(text, str):
        return str(text)
    return ''.join(char if ord(char) < 128 else ' ' for char in text)

class LivestreamRecorder:
    def __init__(self):
        self.config = {
            "youtube"           : '@TenmaMaemi',
            "twitch"            : 'tenma',
            "priority"          : "youtube",
            "output"            : "/mnt/nvme/livestream-recorder/tempfiles",
            "obsidian"          : "/mnt/nas/edit-video_library/Tenma Maemi/archives/Tenma Maemi Livestreams.md",
            "nas_path"          : "/mnt/nas/edit-video_library/Tenma Maemi/archives/raws",
            "check_interval"    : 120,
            "cleanup_hour"      :   3,
            "cooldown_duration" :  30,
            "dual_stream_cycle" :   3
        }
        
        # Intitial checks
        self.active_streams = {}
        self.create_output_dirs()
        self.check_disk_space()
        
        # Initialize variables for cleaner console output
        self.first_stream_ping = True
        self.last_status_line_count = 0
        self.first_void_ping = True
        self.last_cleanup_date = None
        
        # Add cooldown tracking for testing
        self.monitoring_cooldown_until = None
        self.manual_termination_in_progress = False
        
        # Store the original SIGINT handler
        self.original_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self.handle_sigint)

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
            
            if matches := re.findall(r'\[(\d{3})_', content):
                return max(int(m) for m in matches) + 1
            return 1
        except Exception as e:
            logger.error(f"Error reading obsidian file for index: {str(e)}")
            return 1
    
    def create_obsidian_entry(self, index, title, url):
        """Create a new Obsidian entry with full structure (first detection)"""
        if not os.path.exists(os.path.dirname(self.config["obsidian"])):
            logger.warning(f"NAS path unavailable: {os.path.dirname(self.config['obsidian'])}")
            return False
        
        # Get timestamp
        now = datetime.datetime.now()
        utc_offset = now.astimezone().utcoffset()
        hours_offset = int(utc_offset.total_seconds() / 3600)
        tz_str = f"GMT{hours_offset:+d}"
        today = now.strftime(f"%Y.%m.%d %H:%M ({tz_str})")
        
        try:
            # Read existing content
            try:
                with open(self.config["obsidian"], 'r', encoding='utf-8') as f:
                    content = f.read()
            except FileNotFoundError:
                content = ""
            
            # Create new entry
            entry_line = f"- [ ] [{index:03d}_{title}]({url})\n"
            date_line = f"\t{today} [📁]() [🖿]()\n\n"
            
            # Prepend new entry
            new_content = entry_line + date_line + content
            
            with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            logger.info(f"Created new Obsidian entry #{index:03d} for {title}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating obsidian entry: {str(e)}")
            return False
    
    def append_dual_stream_line(self, index, title, url):
        """Append a title line to an existing entry (dual-stream partner detection)"""
        if not os.path.exists(self.config["obsidian"]):
            logger.warning("Obsidian file not found for dual-stream append")
            return False
        
        try:
            with open(self.config["obsidian"], 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find the entry by index pattern: - [ ] [XXX_
            pattern = rf'(- \[ \] \[{index:03d}_[^\n]+\n)'
            match = re.search(pattern, content)
            
            if not match:
                logger.warning(f"Could not find entry #{index:03d} for dual-stream append")
                return False
            
            # Insert new title line after the main entry line
            original_line = match.group(1)
            new_title_line = f"\t[{title}]({url})\n"
            replacement = original_line + new_title_line
            
            new_content = content.replace(original_line, replacement)
            
            with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            logger.info(f"Appended dual-stream line to entry #{index:03d}")
            return True
            
        except Exception as e:
            logger.error(f"Error appending dual-stream line: {str(e)}")
            return False
    
    def update_obsidian_path(self, index, platform, file_path):
        """Update the file path in an Obsidian entry by index"""
        if not os.path.exists(self.config["obsidian"]):
            logger.warning("Obsidian file not found for path update")
            return False
        
        try:
            with open(self.config["obsidian"], 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Find the entry block by index
            # Match from the entry line through the date line with file links
            pattern = rf'(- \[ \] \[{index:03d}_[^\n]+\n(?:\t\[[^\]]+\]\([^\)]+\)\n)*\t[^\[]+)(\[📁\]\([^\)]*\)) (\[🖿\]\([^\)]*\))'
            match = re.search(pattern, content)
            
            if not match:
                logger.warning(f"Could not find entry #{index:03d} for path update")
                return False
            
            prefix = match.group(1)
            yt_link = match.group(2)
            tw_link = match.group(3)
            
            relative_path = f"raws/{os.path.basename(file_path)}"
            
            if platform == "youtube":
                yt_link = f"[📁]({relative_path})"
            else:
                tw_link = f"[🖿]({relative_path})"
            
            new_block = f"{prefix}{yt_link} {tw_link}"
            new_content = content[:match.start()] + new_block + content[match.end():]
            
            with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            logger.info(f"Updated Obsidian path for entry #{index:03d} ({platform})")
            return True
            
        except Exception as e:
            logger.error(f"Error updating obsidian path: {str(e)}")
            return False

    def check_stream_status(self):
        if not self.is_monitoring_allowed():
            return
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
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
        
        for platform, service_config in services.items():
            try:
                cmd = ["yt-dlp", "--cookies-from-browser", "firefox", "--dump-json"] + service_config['extra_args'] + [service_config['url']]
                process = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                
                if process.returncode != 0:
                    continue
                
                try:
                    stream_data = json.loads(process.stdout.strip())
                    is_live = stream_data.get('is_live', False)
                    
                    if is_live:
                        video_id = stream_data.get('id')
                                
                        if platform == 'youtube':
                            stream_url = f"https://www.youtube.com/watch?v={video_id}"
                            obsidian_title = stream_data.get('fulltitle')
                            obsidian_url = stream_url
                            raw_title = f"{obsidian_title} [{video_id}] @ {timestamp}"
                            stream_title = sanitize_filename(raw_title)
                        else:  # twitch
                            stream_url = service_config['url']
                            obsidian_title = stream_data.get('description')
                            obsidian_url = f"{service_config['url']}/video/{video_id.lstrip('v')}"
                            raw_title = f"{obsidian_title} [{video_id}] @ {timestamp}"
                            stream_title = sanitize_filename(raw_title)


                        stream_key = f"{platform}_{video_id}"
                        if stream_key not in self.active_streams:
                            logger.info(f"Found new active {platform.capitalize()} livestream: {stream_title} ({stream_url})")

                            # Get index (shared with dual-stream partner if detected)
                            obsidian_index, is_dual = self.get_stream_index(platform, datetime.datetime.now())
                            if is_dual:
                                logger.info(f"Dual-stream detected, sharing index {obsidian_index:03d}")

                            self.start_stream_recording(stream_url, platform, video_id, stream_title, obsidian_title, obsidian_url, obsidian_index, is_dual)
                        
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse yt-dlp output for {platform}")
                    
            except subprocess.TimeoutExpired:
                logger.warning(f"{platform.capitalize()} check timed out")
            except Exception as e:
                logger.error(f"Error checking {platform}: {str(e)}")

    def start_stream_recording(self, url, platform, identifier, stream_title, obsidian_title, obsidian_url, obsidian_index, is_dual):
        """Start recording a complete stream (video + chat) in separate threads"""
        logger.info(f"Starting stream recording for {platform}: {stream_title}")
        
        # Update Obsidian log
        if is_dual:
            self.append_dual_stream_line(obsidian_index, obsidian_title, obsidian_url)
        else:
            self.create_obsidian_entry(obsidian_index, obsidian_title, obsidian_url)
        
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
            # Build yt-dlp command structure
            fmt = "bestvideo[ext=mp4][vcodec^=avc1]+bestaudio[ext=m4a]/best[ext=mp4]/best"
            cmd = [
                "yt-dlp",
                "--format",               fmt,
                "-o",                     f"{stream_title}.%(ext)s",
                "--no-part",
                "--no-mtime",
                "--retries",              "10",
                "--fragment-retries",     "10",
                "--retry-sleep",          "5",
                "--socket-timeout",       "120",
                "--cookies-from-browser", "firefox",
                "--hls-use-mpegts"

            ]
            if platform == "youtube":
                cmd.extend(["--live-from-start"])

            cmd.append(url)
            
            process = subprocess.Popen(cmd, cwd=self.config["output"])
            self.active_streams[stream_key]["video_process"] = process
            
            logger.info(f"Started video recording for {platform} stream: {stream_title}")
            
            # Monitor completion
            def monitor_completion():
                process.wait()
                
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
                stream_start = int(stream["start_time"].timestamp() * 1_000_000)
                output_path = os.path.join(self.config["output"], f"{stream_title}.json")
                
                def chat_download_thread():
                    try:
                        chat_downloader = ChatDownloader()
                        with open(output_path, 'w', encoding='utf-8') as f:
                            f.write('[\n')  # Start JSON array
                            first_message = True
                            
                            chat = chat_downloader.get_chat(
                                url=url,
                                message_groups=['all'],
                                sort_keys=True,
                                indent=4
                            )
                            
                            for message in chat:
                                if stop_event.is_set():
                                    logger.info("Chat download terminated by stop event")
                                    break
                                
                                # Adjust timestamp to be relative to stream start
                                if 'timestamp' in message:
                                    original_timestamp = message['timestamp']
                                    message['timestamp'] = original_timestamp - stream_start
                                    message['original_timestamp'] = original_timestamp
                                
                                # Write message immediately
                                if not first_message:
                                    f.write(',\n')
                                json.dump(message, f, ensure_ascii=False)
                                f.flush()  # Ensure immediate write
                                first_message = False
                                
                            f.write('\n]')  # Close JSON array
                            logger.info(f"Chat download completed for: {stream_title}")
                            
                    except Exception as e:
                        logger.error(f"Error in chat download thread: {str(e)}")
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

    def _upload_file(self, src, dst):
        """Unified file upload handler"""
        try:
            if os.path.exists(dst):
                logger.info(f"File already exists on server, removing local copy: {os.path.basename(src)}")
                os.remove(src)
                return True
            
            if os.name == 'posix':  # macOS/Linux
                result = subprocess.run(["rsync", "-av", "--remove-source-files", src, dst], check=True)
                logger.info(f"Successfully uploaded {os.path.basename(src)} using rsync (exit code: {result})")
            else:  # Windows
                shutil.copy2(src, dst)
                if os.path.getsize(src) == os.path.getsize(dst):
                    os.remove(src)
                    logger.info(f"Successfully uploaded {os.path.basename(src)} using shutil")
                else:
                    raise Exception("Size mismatch after copy")
            
            return True
        except Exception as e:
            logger.error(f"Upload failed for {os.path.basename(src)}: {str(e)}")
            return False    

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
                video_file = os.path.join(self.config["output"], f"{stream_title}.mp4")
                chat_file = os.path.join(self.config["output"], f"{stream_title}.json")
                
                uploaded_path = None
                
                # Upload video file
                if os.path.exists(video_file):
                    video_dst = os.path.join(self.config["nas_path"], f"{stream_title}.mp4")
                    if self._upload_file(video_file, video_dst):
                        uploaded_path = video_dst
                
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
                    self.update_obsidian_path(obsidian_index, platform, uploaded_path)
                    logger.info(f"Successfully uploaded and logged: {stream_title}")
            
            # Remove from active streams
            del self.active_streams[stream_key]
            logger.info(f"Stream cleanup completed for: {stream_title}")
                
        except Exception as e:
            logger.error(f"Error handling stream completion for {stream_title}: {str(e)}")
            if stream_key in self.active_streams:
                del self.active_streams[stream_key]
    
    def daily_maintenance(self):
        logger.info("Running daily maintenance routine...")
        
        # Ensure disk space
        self.check_disk_space()

        # Ensure temporary output directory is cleaned
        try:
            # Find all straggler files
            video_files = glob.glob(os.path.join(self.config["output"], "*.mp4"))
            chat_files = glob.glob(os.path.join(self.config["output"], "*.json"))
            
            if not video_files and not chat_files:
                logger.info("No files to clean up.")
                return
            logger.info(f"Found {len(video_files)} video files and {len(chat_files)} chat files in temp directory")
            
            # Process all files using unified upload handler
            all_files = [(f, f"{self.config['nas_path']}/{os.path.basename(f)}") for f in video_files + chat_files]
            
            for src_file, dst_file in all_files:
                try:
                    logger.info(f"Attempting to upload straggler file: {os.path.basename(src_file)}")
                    if self._upload_file(src_file, dst_file):
                        logger.info(f"Successfully uploaded straggler file: {os.path.basename(src_file)}")
                    else:
                        logger.error(f"Failed to upload straggler file: {os.path.basename(src_file)}")
                except Exception as e:
                    logger.error(f"Error processing straggler file {src_file}: {str(e)}")
        
            logger.info("Cleanup complete.")
                    
        except Exception as e:
            logger.error(f"Error during daily cleanup: {str(e)}")
    
    def run(self):
        """Main loop to check for livestreams and manage recordings"""
        logger.info("Starting Enhanced Livestream Recorder")
        logger.info(f"Checking every {self.config['check_interval']} seconds")
        # logger.info(f"Weekly cleanup scheduled for {self.config['cleanup_hour']}:00")
        logger.info(f"Manual termination cooldown: {self.config['cooldown_duration']} seconds")
        print('-' * 100)
        
        try:
            while True:
                # Check if we should run daily cleanup
                now = datetime.datetime.now()
                if (now.hour == self.config["cleanup_hour"] and self.last_cleanup_date != now.date()):
                    if self.active_streams:
                        logger.info("Skipping daily maintenance - active streams in progress")
                    else:
                        # self.daily_maintenance()
                        self.last_cleanup_date = datetime.datetime.now().date()
                
                # If we have active streams, show status
                if self.active_streams:
                    self.first_void_ping = True

                    if self.first_stream_ping:
                        active_count = len(self.active_streams)
                        logger.info(f"Active streams: {active_count}")
                        self.first_stream_ping = False

                else:
                    # Check if we're in cooldown period
                    if not self.is_monitoring_allowed():
                        cooldown_remaining = max(0, (self.monitoring_cooldown_until - now).total_seconds())
                        progress = int(20 * (1 - cooldown_remaining / self.config["cooldown_duration"]))
                        current_time = now.strftime("%H:%M:%S")
                        cooldown_status = f"[{current_time}] Cooldown: [{'#'*progress}{'.'*(20-progress)}] {cooldown_remaining:.0f}s"
                        self.print_status(cooldown_status, overwrite=True)
                        time.sleep(self.config["check_interval"])
                        continue
                    
                    if self.first_void_ping:
                        logger.info("No active streams, checking for new livestreams")
                        print()
                        self.first_void_ping = False
                        self.first_stream_ping = True
                    
                    # Reset status line count to ensure proper overwriting
                    self.last_status_line_count = 1
                    
                    # Check for new streams (these run async so won't block)
                    self.check_stream_status()
                    
                    if not self.active_streams:
                        current_time = datetime.datetime.now().strftime("%H:%M:%S")
                        next_check = (datetime.datetime.now() 
                                      + datetime.timedelta(seconds=self.config["check_interval"])).strftime("%H:%M:%S")
                        final_status = f"[{current_time}] No active livestreams detected. Next check at {next_check}."
                        
                        # Update the status line (overwrite previous status)
                        self.print_status(final_status, overwrite=True)
                
                # Wait for next check
                time.sleep(self.config["check_interval"])
        
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, shutting down")
            self.shutdown()
    
    def print_status(self, message, overwrite=False):
        """Print status messages to console, with option to overwrite previous lines"""
        if overwrite and self.last_status_line_count > 0:
            # Move cursor up and clear lines
            sys.stdout.write(f"\033[{self.last_status_line_count}A")  # Move cursor up
            sys.stdout.write("\033[J")  # Clear from cursor to end of screen
        
        # Print the new message
        print(message)
        
        # Count the number of lines in the message
        if isinstance(message, str):
            self.last_status_line_count = message.count('\n') + 1
        else:
            self.last_status_line_count = 1
        
        # Force the output to display immediately
        sys.stdout.flush()
    
    def create_output_dirs(self):
        Path(self.config["output"]).mkdir(parents=True, exist_ok=True)
        logger.info("Output directories created")
    
    def check_disk_space(self):
        logger.info("Checking free disk space...")
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
            
            logger.info(f"Disk space available: {output_free_gb:.2f} GB.")
            
            # Warn if disk space is low (less than 10GB)
            if output_free_gb < 10:
                lsw = f"Low disk space: {output_free_gb:.2f} GB remaining."
                logger.warning(lsw)
                self.print_status(f"⚠️ WARNING: {lsw}", overwrite=False)
        except Exception as e:
            logger.error(f"Error checking disk space: {str(e)}")
        
    def shutdown(self):
        logger.info("Shutting down Livestream Recorder...")
        for stream_key, stream in list(self.active_streams.items()):
            logger.info("Terminating stream...")
            self.handle_stream_completion(stream_key, upload_files=False)
        
        logger.info("Livestream Recorder terminated successfully.")

if __name__ == "__main__":
    recorder = LivestreamRecorder()
    recorder.run()