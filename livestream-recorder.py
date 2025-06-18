#!/usr/bin/env python3

import os, re, glob, time, json, logging, subprocess, datetime, sys, shutil, signal, threading, ctypes, urllib.parse, browser_cookie3
from pathlib import Path
from chat_downloader import ChatDownloader
from yt_dlp.utils import sanitize_filename

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
            "output"            : "/mnt/nvme/livestream-recorder/tempfiles",
            "obsidian"          : "/mnt/nas/edit-video_library/Tenma Maemi/notes/MaemiArchive/Tenma Maemi Livestreams.md",
            "nas_path"          : "/mnt/nas/edit-video_library/Tenma Maemi/raws",
            "check_interval"    : 120,
            "cleanup_hour"      :   3, # Hour to run daily cleanup (3 AM)
            "cooldown_duration" : 30  # Block monitoring post termination (seconds)
        }
        
        # Intitial checks
        self.active_streams = {}
        self.create_output_dirs()
        self.check_disk_space()
        
        # Initialize variables for cleaner console output
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
                            raw_title = f"{obsidian_title} [{video_id}] @ {timestamp}"
                            stream_title = sanitize_filename(raw_title)
                        else:  # twitch
                            stream_url = service_config['url']
                            obsidian_title = stream_data.get('description')
                            raw_title = f"{obsidian_title} [{video_id}] @ {timestamp}"
                            stream_title = sanitize_filename(raw_title)
                            
                        logger.info(f"Found active {platform.capitalize()} livestream: {stream_title} ({stream_url})")

                        stream_key = f"{platform}_{video_id}"
                        if stream_key not in self.active_streams:
                            self.start_stream_recording(stream_url, platform, video_id, stream_title, obsidian_title)
                        
                except json.JSONDecodeError:
                    logger.warning(f"Could not parse yt-dlp output for {platform}")
                    
            except subprocess.TimeoutExpired:
                logger.warning(f"{platform.capitalize()} check timed out")
            except Exception as e:
                logger.error(f"Error checking {platform}: {str(e)}")

    def start_stream_recording(self, url, platform, identifier, stream_title, obsidian_title):
        """Start recording a complete stream (video + chat) in separate threads"""
        logger.info(f"Starting stream recording for {platform}: {stream_title}")
        
        # Update Obsidian log immediately when stream is detected
        obsidian_entry_info = self.update_obsidian_log(obsidian_title, url)

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
            "base_filename"         : stream_title,
            "obsidian_entry_info"   : obsidian_entry_info
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
                "--live-from-start",
            ]
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
        
        stream_start = int(stream["start_time"].timestamp() * 1_000_000)
        output_path = os.path.join(self.config["output"], f"{stream['stream_title']}.json")
        
        try:
            chat_downloader = ChatDownloader()
            stop_event = threading.Event()
            self.active_streams[stream_key]["chat_stop_event"] = stop_event
            
            def chat_download_thread():
                try:
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
                        
                except Exception as e:
                    logger.error(f"Error in chat download thread: {str(e)}")
            
            thread = threading.Thread(target=chat_download_thread, daemon=True)
            thread.start()
            self.active_streams[stream_key]["chat_thread"] = thread
            logger.info(f"Started chat recording for {platform} stream")
            
        except Exception as e:
            logger.error(f"Error starting chat recording for {url}: {str(e)}")

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
        obsidian_entry_info = stream.get("obsidian_entry_info")
        
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
                if uploaded_path:
                    self.update_obsidian_log(
                        local_file_path=uploaded_path,
                        update_path_only=True,
                        entry_info=obsidian_entry_info
                    )
                    logger.info(f"Successfully uploaded and logged: {stream_title}")
            
            # Remove from active streams
            del self.active_streams[stream_key]
            logger.info(f"Stream cleanup completed for: {stream_title}")
                
        except Exception as e:
            logger.error(f"Error handling stream completion for {stream_title}: {str(e)}")
            if stream_key in self.active_streams:
                del self.active_streams[stream_key]
    
    def update_obsidian_log(self, stream_title=None, stream_url=None, 
                            local_file_path=None, update_path_only=False, entry_info=None):
        """Update Obsidian log using content-based matching"""
        today = datetime.datetime.now().strftime("%Y.%m.%d")

        if not os.path.exists(os.path.dirname(self.config["obsidian"])):
            logger.warning(f"NAS path unavailable: {os.path.dirname(self.config['obsidian'])}")
            return None

        try:
            with open(self.config["obsidian"], 'r+', encoding='utf-8') as f:
                content = f.read()
        except FileNotFoundError:
            content = ""

        # Handle path-only updates using stored entry info
        if update_path_only and entry_info and local_file_path:
            # Extract the original entry lines we need to find
            original_entry = entry_info.get("entry_line", "")
            original_date_line = entry_info.get("date_line", "")
            
            if not original_entry or not original_date_line:
                logger.warning("Invalid entry info for update")
                return None
            
            # Create the new date line with updated path
            formatted_path = 'file://' + urllib.parse.quote(local_file_path)
            new_date_line = re.sub(r'\[📁\]\(.*?\)', 
                                f'[📁]({formatted_path})', 
                                original_date_line)
            
            # Replace only the file path portion in the original content block
            content_block = original_entry + original_date_line
            new_block = original_entry + new_date_line
            
            # Perform the replacement
            if content_block in content:
                new_content = content.replace(content_block, new_block)
                
                # Write back to file
                with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                    f.write(new_content)
                
                logger.info(f"Updated Obsidian log entry with file path: {local_file_path}")
                return entry_info
            else:
                logger.warning("Original entry not found in log file")
                return None
        
        # Creating a new entry
        if not update_path_only and stream_title and stream_url:
            # Find next available index
            index = 1
            if matches := re.findall(r'\[(\d{3})_', content):
                index = max(int(m) for m in matches) + 1
            
            index_str = f'{index:03d}'
            
            # Create new entry blocks
            entry_line = f"- [ ] [{index_str}_{stream_title}]({stream_url})\n"
            date_line = f"\t{today} [📁]()\n\n"
            
            # Prepend new entry
            new_content = entry_line + date_line + content
            
            with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                f.write(new_content)
            
            # Return full entry info for future updates
            entry_info = {
                "entry_line": entry_line,
                "date_line": date_line,
                "stream_title": stream_title,
                "stream_url": stream_url
            }
            
            logger.info(f"Created new Obsidian entry #{index_str} for {stream_title}")
            return entry_info
        
        return None

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
        logger.info(f"Daily cleanup scheduled for {self.config['cleanup_hour']}:00")
        logger.info(f"Manual termination cooldown: {self.config['cooldown_duration']} seconds")
        print('-' * 100)
        
        try:
            while True:
                # Check if we should run daily cleanup
                now = datetime.datetime.now()
                if (now.hour == self.config["cleanup_hour"] and self.last_cleanup_date != now.date()):
                    self.daily_maintenance()
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