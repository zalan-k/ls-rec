#!/usr/bin/env python3

import os, re, glob, time, json, logging, subprocess, datetime, sys, shutil, signal, threading, ctypes, urllib.parse
from pathlib import Path
from chat_downloader import ChatDownloader
from yt_dlp.utils import sanitize_filename

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("livestream_recorder.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("LivestreamRecorder")

class LivestreamRecorder:
    def __init__(self):
        self.config = {
            "check_interval": 120,  # 2 minutes
            "youtube"       : 'https://www.youtube.com/@TenmaMaemi',
            "twitch"        : 'tenma',
            "output"        : '/mnt/nvme/livestream-recorder/tempfiles',
            "obsidian"      : '/mnt/nas/edit-video_library/Tenma Maemi/notes/MaemiArchive/Tenma Maemi Livestreams.md',
            "nas_path"      : '/mnt/nas/edit-video_library/Tenma Maemi/raws',
            "cleanup_hour"  : 3  # Hour to run daily cleanup (3 AM)
        }
    
        # Changed from active_downloads to active_streams - unified stream instances
        self.active_streams = {}
        self.create_output_dirs()
        self.check_disk_space()
        
        # Initialize variables for cleaner console output
        self.last_status_line_count = 0
        self.first_void_ping = True
        self.last_cleanup_date = None
        
        # Store the original SIGINT handler
        self.original_sigint_handler = signal.getsignal(signal.SIGINT)
        # Set up custom signal handler
        signal.signal(signal.SIGINT, self.handle_sigint)
        self.current_ytdlp_process = None

    def handle_sigint(self, sig, frame):
        """Custom SIGINT handler that forwards to yt-dlp if running, otherwise exits"""
        if self.current_ytdlp_process and self.current_ytdlp_process.poll() is None:
            # yt-dlp is running, send SIGINT to it instead of main program
            print("\nForwarding Ctrl+C to yt-dlp process...")
            try:
                if os.name == 'posix':  # Unix/Mac
                    # Send SIGINT to process group
                    pgid = os.getpgid(self.current_ytdlp_process.pid)
                    os.killpg(pgid, signal.SIGINT)
                else:  # Windows
                    # On Windows, we need to send a Ctrl+C event
                    import ctypes
                    ctypes.windll.kernel32.GenerateConsoleCtrlEvent(0, self.current_ytdlp_process.pid)
                
                # Give it a moment to process the signal
                time.sleep(1)
                return  # Don't exit, let the main program continue
            except Exception as e:
                logger.error(f"Error sending SIGINT to yt-dlp: {str(e)}")
        
        # Either no yt-dlp process is running or we've already tried to stop it
        # Restore original handler and re-raise the signal
        signal.signal(signal.SIGINT, self.original_sigint_handler)
        os.kill(os.getpid(), signal.SIGINT)
    
    def check_youtube_livestreams(self):
        """Check if configured YouTube channels are currently streaming"""
        channel = self.config["youtube"]
        
        try:
            process = subprocess.run(
                ["yt-dlp", "--dump-json", "--playlist-items", "1", f"{channel}/live"],
                capture_output=True, text=True, timeout=30
            )
            if process.returncode != 0:
                return
            
            try: 
                stream_data = json.loads(process.stdout.strip())
                is_live = stream_data.get('is_live', False)
                if is_live:
                    video_id = stream_data.get('id')
                    stream_url = f"https://www.youtube.com/watch?v={video_id}"
                    stream_title = sanitize_filename(stream_data.get('title'))
                    
                    logger.info(f"Found active YouTube livestream: {stream_title} ({stream_url})")
                    stream_key = f"youtube_{video_id}"
                    if stream_key not in self.active_streams:
                        self.start_stream_recording(stream_url, "youtube", video_id, stream_title)
            
            except json.JSONDecodeError:
                logger.warning(f"Could not parse yt-dlp output for {channel}")
        except subprocess.TimeoutExpired:
            logger.warning("YouTube check timed out")
        except Exception as e:
            logger.error(f"Error checking YouTube channel {channel}: {str(e)}")
    
    def check_twitch_livestreams(self):
        """Check if configured Twitch channels are currently streaming"""
        channel = self.config["twitch"]
        
        try:
            process = subprocess.run(
                ["yt-dlp", "--dump-json", f"https://www.twitch.tv/{channel}"],
                capture_output=True, text=True, timeout=30
            )
            if process.returncode != 0:
                return
            
            # Parse output to determine if stream is live
            try:
                stream_data = json.loads(process.stdout.strip())
                is_live = stream_data.get('is_live', False)
                if is_live:
                    video_id = stream_data.get('id')
                    stream_url = f"https://www.twitch.tv/{channel}"
                    stream_title = sanitize_filename(f"{stream_data['description']} [{video_id}]")
                    
                    # Start new stream recording
                    logger.info(f"Found active Twitch livestream: {stream_title} ({stream_url})")
                    stream_key = f"twitch_{video_id}"
                    if stream_key not in self.active_streams:
                        self.start_stream_recording(stream_url, "twitch", video_id, stream_title)
            
            except json.JSONDecodeError:
                logger.warning(f"Could not parse yt-dlp output for {channel}")
        except subprocess.TimeoutExpired:
            logger.warning("Twitch check timed out")
        except Exception as e:
            logger.error(f"Error checking Twitch channel {channel}: {str(e)}")

    def start_stream_recording(self, url, platform, identifier, stream_title):
        """Start recording a complete stream (video + chat) in separate threads"""
        logger.info(f"Starting stream recording for {platform}: {stream_title}")
        
        # Update Obsidian log immediately when stream is detected
        self.update_obsidian_log(stream_title, url)
        
        # Create unified stream entry
        stream_key = f"{platform}_{identifier}"
        self.active_streams[stream_key] = {
            "url": url,
            "platform": platform,
            "identifier": identifier,
            "stream_title": stream_title,
            "start_time": datetime.datetime.now(),
            "video_process": None,
            "chat_thread": None,
            "chat_stop_event": None
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
            ]
            if platform == 'youtube':
                cmd.append('--live-from-start')
            cmd.append(url)
            
            # Create a process group so we can send signals to the entire group
            if os.name == 'posix':  # Unix/Mac
                # Start process in new process group
                process = subprocess.Popen(
                    cmd,
                    cwd=self.config["output"],
                    preexec_fn=os.setpgrp
                )
            else:  # Windows
                # In Windows, create a new process group
                process = subprocess.Popen(
                    cmd,
                    cwd=self.config["output"],
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )
            
            # Save the current process and update stream record
            self.current_ytdlp_process = process
            self.active_streams[stream_key]["video_process"] = process
            
            logger.info(f"Started video recording for {platform} stream: {stream_title}")
            
            # Monitor completion
            def monitor_completion():
                process.wait()  # Wait for process to complete
                
                if process.returncode == 0 or process.returncode == 1:
                    logger.info(f"Video download completed successfully: {stream_title}")
                else:
                    logger.error(f"Video download failed for {stream_title}: Return code {process.returncode}")
                
                # Handle stream completion (includes chat stopping and upload)
                self.handle_stream_completion(stream_key, upload_files=True)
                
                # Clean up from current process tracker
                if self.current_ytdlp_process is process:
                    self.current_ytdlp_process = None
            
            # Start monitoring thread
            monitor_thread = threading.Thread(target=monitor_completion, daemon=True)
            monitor_thread.start()
            
        except Exception as e:
            logger.error(f"Error starting video recording for {url}: {str(e)}")
            # Clean up failed stream
            if stream_key in self.active_streams:
                del self.active_streams[stream_key]
            self.current_ytdlp_process = None

    def start_chat_recording(self, stream_key):
        """Start chat recording for a stream"""
        stream = self.active_streams[stream_key]
        url = stream["url"]
        platform = stream["platform"]
        
        stream_start = int(stream["start_time"].timestamp() * 1_000_000)
        output_path = os.path.join(self.config["output"], f"{stream['stream_title']}.json")
        
        try:
            chat_downloader = ChatDownloader()
            # Create an event to signal when to stop the chat download
            stop_event = threading.Event()
            # Update stream record with chat components
            self.active_streams[stream_key]["chat_stop_event"] = stop_event
            
            # Use threading to handle this in background
            def chat_download_thread():
                try:
                    # Get chat generator
                    chat = chat_downloader.get_chat(
                        url=url,
                        message_groups=['all'],
                        sort_keys=True,
                        indent=4
                    )
                    messages = []
                    
                    # Process messages as they arrive
                    for message in chat:
                        if stop_event.is_set():
                            logger.info("Chat download terminated by stop event")
                            break
                        
                        # Adjust timestamp to be relative to stream start
                        if 'timestamp' in message:
                            original_timestamp = message['timestamp']
                            message['timestamp'] = original_timestamp - stream_start
                            message['original_timestamp'] = original_timestamp
                        
                        messages.append(message)
                    
                    with open(output_path, 'w', encoding='utf-8') as f:
                        json.dump(messages, f, sort_keys=True, indent=4)
                        
                except Exception as e:
                    logger.error(f"Error in chat download thread: {str(e)}")
                finally:
                    if messages:
                        try:
                            with open(output_path, 'w', encoding='utf-8') as f:
                                json.dump(messages, f, sort_keys=True, indent=4)
                        except Exception as e:
                            logger.error(f"Error saving chat messages: {e}")
            
            # Start thread
            thread = threading.Thread(target=chat_download_thread, daemon=True)
            thread.start()
            self.active_streams[stream_key]["chat_thread"] = thread
            logger.info(f"Started chat recording for {platform} stream")
            
        except Exception as e:
            logger.error(f"Error starting chat recording for {url}: {str(e)}")

    def handle_stream_completion(self, stream_key, upload_files=True):
        if stream_key not in self.active_streams:
            return
            
        stream = self.active_streams[stream_key]
        stream_title = stream["stream_title"]
        
        logger.info(f"Handling completion of stream: {stream_title}")
        try:
            # Terminate video process if still running
            video_process = stream.get("video_process")
            if video_process and video_process.poll() is None:  # Still running
                try:
                    video_process.terminate()
                    logger.info(f"Video process terminated for: {stream_title}")
                except Exception as e:
                    logger.error(f"Error terminating video process: {str(e)}")
            
            # Stop chat download if it's running
            if stream.get("chat_stop_event"):
                logger.info(f"Stopping chat download for: {stream_title}")
                stream["chat_stop_event"].set()
                
                # Wait for chat thread to terminate (up to 5 seconds)
                if stream.get("chat_thread"):
                    stream["chat_thread"].join(timeout=5)
                    logger.info(f"Chat download stopped for: {stream_title}")
            
            # Upload files to server if requested
            if upload_files:
                uploaded_path = self.upload_to_server(self.config["output"], self.config["nas_path"])
                
                if uploaded_path:
                    # Update Obsidian log with server file path
                    self.update_obsidian_log(local_file_path=uploaded_path, update_path_only=True)
                    logger.info(f"Successfully uploaded and logged: {stream_title}")
                else:
                    logger.error(f"Failed to upload files for: {stream_title}")
            
            # Remove from active streams
            del self.active_streams[stream_key]
            logger.info(f"Stream cleanup completed for: {stream_title}")
                
        except Exception as e:
            logger.error(f"Error handling stream completion for {stream_title}: {str(e)}")
            # Still remove from active streams even if there was an error
            if stream_key in self.active_streams:
                del self.active_streams[stream_key]
    
    def update_obsidian_log(self, stream_title=None, stream_url=None, 
                            local_file_path=None, update_path_only=False):
        """Update Obsidian log with stream information"""
        today = datetime.datetime.now().strftime("%Y.%m.%d")
        date_pattern = r'\s+\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}$'
        
        # Check if NAS is mounted before attempting to access
        if not os.path.exists(os.path.dirname(self.config["obsidian"])):
            logger.warning(f"NAS path not available: {os.path.dirname(self.config['obsidian'])}")
            return
            
        try:
            with open(self.config["obsidian"], 'r', encoding='utf-8') as f:
                content = f.readlines()
        except FileNotFoundError:
            content = []
        
        if update_path_only and content:
            if len(content) >= 2 and content[1].strip().startswith(today):
                formatted_file_path = 'file://' + urllib.parse.quote(local_file_path)
                content[1] = f"\t{today} [📁]({formatted_file_path})\n"
                
                with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                    f.writelines(content)
                
                logger.info(f"Updated Obsidian log with file path: {local_file_path}")
                return
        
        # Creating a new entry
        if not update_path_only and stream_title and stream_url:
            # Find the last index by looking at the first line
            if content and '- [ ] [' in content[0]:
                last_index = int(content[0].split('- [ ] [')[1].split('_')[0])
                new_index = f'{last_index + 1}'.zfill(3)
            else:
                new_index = "001"
            
            # Create the new entry with empty file path initially
            cleaned_title = re.sub(date_pattern, '', stream_title)
            entry_line = f"- [ ] [{new_index}_{cleaned_title}]({stream_url})\n"
            local_link_line = f"\t{today} [📁]()\n\n"  # Empty path, will update later
            
            # Write back to file with new entry at the top
            with open(self.config["obsidian"], 'w', encoding='utf-8') as f:
                f.write(entry_line)
                f.write(local_link_line)
                f.writelines(content)
            
            logger.info(f"Created new Obsidian log entry #{new_index} for {stream_title}")
            print()
    
    def upload_to_server(self, source_dir, destination_dir):
        """Upload the most recently completed download and its chat file to the server"""
        # Check if server/NAS is available
        if not os.path.exists(destination_dir):
            logger.warning(f"Server destination not available: {destination_dir}")
            return None
        
        try:
            # Find the most recent video file
            video_files = glob.glob(os.path.join(source_dir, "*.mp4"))
            if not video_files:
                logger.warning("No video files found to upload to server")
                return None
                
            newest_video = max(video_files, key=os.path.getctime)
            video_basename = os.path.basename(newest_video)
            
            # Check if file already exists on server
            video_dest_path = os.path.join(destination_dir, video_basename)
            if os.path.exists(video_dest_path):
                logger.info(f"File already exists on server, removing local copy: {video_basename}")
                os.remove(newest_video)
                return video_dest_path
            
            # Find matching chat file (same base name, .json extension)
            video_base = os.path.splitext(video_basename)[0]
            chat_file_path = os.path.join(source_dir, f"{video_base}.json")
            chat_file = chat_file_path if os.path.exists(chat_file_path) else None
            
            # Upload video file
            logger.info(f"Uploading video to server: {video_basename}")
            if os.name == 'posix':  # macOS/Linux
                result = subprocess.run(["rsync", "-av", "--remove-source-files", newest_video, video_dest_path])
                if result.returncode != 0:
                    logger.error(f"Failed to upload {video_basename} using rsync")
                    return None
                logger.info(f"Successfully uploaded {video_basename} to server using rsync")
            else:  # Windows
                shutil.copy2(newest_video, video_dest_path)
                if os.path.exists(video_dest_path) and os.path.getsize(video_dest_path) == os.path.getsize(newest_video):
                    os.remove(newest_video)
                    logger.info(f"Successfully uploaded {video_basename} to server using shutil")
                else:
                    logger.error(f"Failed to verify file upload to server: {video_dest_path}")
                    return None
            
            # Upload chat file if it exists
            if chat_file:
                chat_dest_filename = f"{video_base}.json"
                chat_dest_path = os.path.join(destination_dir, chat_dest_filename)
                
                # Check if chat file already exists on server
                if os.path.exists(chat_dest_path):
                    logger.info("Chat file already exists on server, removing local copy")
                    os.remove(chat_file)
                else:
                    logger.info(f"Uploading chat file to server: {chat_dest_filename}")
                    if os.name == 'posix':  # macOS/Linux
                        result = subprocess.run(["rsync", "-av", "--remove-source-files", chat_file, chat_dest_path])
                        if result.returncode == 0:
                            logger.info("Successfully uploaded chat file to server using rsync")
                        else:
                            logger.error("Failed to upload chat file using rsync")
                    else:  # Windows
                        shutil.copy2(chat_file, chat_dest_path)
                        if os.path.exists(chat_dest_path) and os.path.getsize(chat_dest_path) == os.path.getsize(chat_file):
                            os.remove(chat_file)
                            logger.info("Successfully uploaded chat file to server using shutil")
                        else:
                            logger.error("Failed to verify chat file upload to server")
            
            return video_dest_path
            
        except Exception as e:
            logger.error(f"Error uploading files to server: {str(e)}")
            return None

    def daily_cleanup(self):
        logger.info("Running daily cleanup routine...")
        
        try:
            # Find all straggler files
            video_files = glob.glob(os.path.join(self.config["output"], "*.mp4"))
            chat_files = glob.glob(os.path.join(self.config["output"], "*.json"))
            
            if not video_files and not chat_files:
                logger.info("No files to clean up.")
                return
            logger.info(f"Found {len(video_files)} video files and {len(chat_files)} chat files in temp directory")
            
            # Process video files
            for video_file in video_files:
                try:
                    video_basename = os.path.basename(video_file)
                    server_path = os.path.join(self.config["nas_path"], video_basename)
                    
                    if os.path.exists(server_path):
                        logger.info(f"File already exists on server, removing local copy: {video_basename}")
                        os.remove(video_file)
                    else:
                        logger.info(f"Attempting to upload straggler file: {video_basename}")
                        # Try to upload
                        if os.name == 'posix':
                            result = subprocess.run(["rsync", "-av", "--remove-source-files", video_file, server_path])
                            if result.returncode == 0:
                                logger.info(f"Successfully uploaded straggler file: {video_basename}")
                            else:
                                logger.error(f"Failed to upload straggler file: {video_basename}")
                        else:
                            shutil.copy2(video_file, server_path)
                            if os.path.exists(server_path) and os.path.getsize(server_path) == os.path.getsize(video_file):
                                os.remove(video_file)
                                logger.info(f"Successfully uploaded straggler file: {video_basename}")
                            else:
                                logger.error(f"Failed to upload straggler file: {video_basename}")
                                
                except Exception as e:
                    logger.error(f"Error processing stuck video file {video_file}: {str(e)}")
            
            # Process chat files
            for chat_file in chat_files:
                try:
                    chat_basename = os.path.basename(chat_file)
                    server_path = os.path.join(self.config["nas_path"], chat_basename)
                    
                    if os.path.exists(server_path):
                        logger.info(f"Chat file already exists on server, removing local copy: {chat_basename}")
                        os.remove(chat_file)
                    else:
                        logger.info(f"Attempting to upload stuck chat file: {chat_basename}")
                        # Try to upload
                        if os.name == 'posix':
                            result = subprocess.run(["rsync", "-av", "--remove-source-files", chat_file, server_path])
                            if result.returncode == 0:
                                logger.info(f"Successfully uploaded stuck chat file: {chat_basename}")
                            else:
                                logger.error(f"Failed to upload stuck chat file: {chat_basename}")
                        else:
                            shutil.copy2(chat_file, server_path)
                            if os.path.exists(server_path) and os.path.getsize(server_path) == os.path.getsize(chat_file):
                                os.remove(chat_file)
                                logger.info(f"Successfully uploaded stuck chat file: {chat_basename}")
                            else:
                                logger.error(f"Failed to upload stuck chat file: {chat_basename}")
                                
                except Exception as e:
                    logger.error(f"Error processing stuck chat file {chat_file}: {str(e)}")
        
            logger.info("Cleanup complete.")
                    
        except Exception as e:
            logger.error(f"Error during daily cleanup: {str(e)}")
    
    def run(self):
        """Main loop to check for livestreams and manage recordings"""
        logger.info("Starting Enhanced Livestream Recorder")
        logger.info(f"Checking every {self.config['check_interval']} seconds")
        logger.info(f"Daily cleanup scheduled for {self.config['cleanup_hour']}:00")
        print('-' * 100)
        
        try:
            while True:
                # Check if we should run daily cleanup
                now = datetime.datetime.now()
                if (now.hour == self.config["cleanup_hour"] and self.last_cleanup_date != now.date()):
                    self.daily_cleanup()
                    self.last_cleanup_date = datetime.datetime.now().date()
                
                # If we have active streams, show status
                if self.active_streams:
                    self.first_void_ping = True
                    active_count = len(self.active_streams)
                    logger.info(f"Active streams: {active_count}")
                else:
                    if self.first_void_ping:
                        logger.info("No active streams, checking for new livestreams")
                        print()
                        self.first_void_ping = False
                    
                    # Reset status line count to ensure proper overwriting
                    self.last_status_line_count = 1
                    
                    # Check for new streams (these run async so won't block)
                    self.check_youtube_livestreams()
                    self.check_twitch_livestreams()
                    
                    if not self.active_streams:
                        current_time = datetime.datetime.now().strftime("%H:%M:%S")
                        next_check = (datetime.datetime.now() 
                                      + datetime.timedelta(seconds=self.config["check_interval"])).strftime("%H:%M:%S")
                        final_status = f"[{current_time}] No active livestreams detected. Next check at {next_check}."
                        
                        # Update the status line (overwrite previous status)
                        self.print_status(final_status, overwrite=True)
                
                # Check disk space periodically (every hour)
                if datetime.datetime.now().minute == 0 and datetime.datetime.now().second < 10:
                    self.check_disk_space()
                
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