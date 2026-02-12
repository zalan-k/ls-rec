#!/usr/bin/env python3
"""
ls-check - CLI tool to send commands to the running livestream recorder.

Usage:
    ls-check                    # Check both platforms for live streams
    ls-check status             # Show recorder status and active streams
    ls-check youtube            # Check YouTube only
    ls-check twitch             # Check Twitch only
    ls-check record youtube     # Force-start YouTube recording
    ls-check record twitch      # Force-start Twitch recording
    ls-check record youtube <url>  # Record a specific YouTube URL
    ls-check help               # Show available commands
"""

import socket
import sys
import os

SOCKET_PATH = "/tmp/livestream-recorder.sock"

def send_command(command):
    """Send a command to the recorder and print the response"""
    if not os.path.exists(SOCKET_PATH):
        print("ERROR: Livestream recorder is not running.")
        print("  Start it with: sudo systemctl start livestream-recorder.service")
        print("  Or manually:   ls-rec")
        sys.exit(1)
    
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(35)  # Slightly longer than yt-dlp's 30s timeout
        sock.connect(SOCKET_PATH)
        sock.sendall(command.encode('utf-8'))
        
        # Read full response
        chunks = []
        while True:
            try:
                data = sock.recv(4096)
                if not data:
                    break
                chunks.append(data)
            except socket.timeout:
                break
        
        sock.close()
        response = b''.join(chunks).decode('utf-8')
        print(response)
        
    except ConnectionRefusedError:
        print("ERROR: Could not connect to recorder. It may have crashed.")
        print("  Check status: sudo systemctl status livestream-recorder.service")
        sys.exit(1)
    except socket.timeout:
        print("ERROR: Command timed out. The recorder may be busy.")
        sys.exit(1)

def main():
    args = sys.argv[1:]
    
    if not args:
        # Default: check both platforms
        send_command("check all")
    elif args[0] in ("youtube", "twitch"):
        # ls-check youtube / ls-check twitch
        send_command(f"check {args[0]}")
    elif args[0] == "record":
        # ls-check record youtube [url]
        send_command(" ".join(["record"] + args[1:]))
    elif args[0] == "status":
        send_command("status")
    elif args[0] in ("help", "--help", "-h"):
        send_command("help")
    else:
        send_command(" ".join(args))

if __name__ == "__main__":
    main()
