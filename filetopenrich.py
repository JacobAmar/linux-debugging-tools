#!/usr/bin/env python3
import subprocess
import time
import signal
import sys
from collections import defaultdict

def parse_filetop_line(line):
    try:
        parts = line.strip().split()
        if len(parts) >= 8 and parts[-1].endswith('.log'):
            return {
                'tid': parts[0],
                'comm': parts[1],
                'reads': int(parts[2]),
                'writes': int(parts[3]),
                'r_kb': float(parts[4]),
                'w_kb': float(parts[5]),
                'type': parts[6],
                'filename': parts[7]
            }
    except (ValueError, IndexError):
        return None
    return None

def find_kafka_file(filename):
    cmd = f"find /mnt/kafka-disks/ -name {filename} 2>/dev/null"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except:
        return None

def cleanup_and_exit(signum, frame):
    print("\nCleaning up...")
    # Kill any running filetop processes
    try:
        subprocess.run(["pkill", "-f", "filetop"], check=False)
    except subprocess.SubprocessError:
        pass
    sys.exit(0)

def monitor_filetop():
    # Set up signal handlers
    signal.signal(signal.SIGINT, cleanup_and_exit)
    signal.signal(signal.SIGTERM, cleanup_and_exit)

    cmd = ["filetop", "-C"]
    file_stats = defaultdict(lambda: {'reads': 0, 'writes': 0, 'r_kb': 0, 'w_kb': 0, 'comm': set()})
    
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    try:
        while True:
            line = process.stdout.readline()
            if not line:
                break

            # Skip header lines
            if 'loadavg' in line or 'TID' in line or not line.strip():
                continue

            parsed = parse_filetop_line(line)
            if parsed:
                filename = parsed['filename']
                file_stats[filename]['reads'] += parsed['reads']
                file_stats[filename]['writes'] += parsed['writes']
                file_stats[filename]['r_kb'] += parsed['r_kb']
                file_stats[filename]['w_kb'] += parsed['w_kb']
                file_stats[filename]['comm'].add(parsed['comm'])

                # Clear screen and show top 10 files by total KB (read + write)
                print("\033[2J\033[H")  # Clear screen and move cursor to top
                print("Top files by I/O:")
                print("-" * 80)
                
                sorted_stats = sorted(
                    file_stats.items(),
                    key=lambda x: (x[1]['r_kb'] + x[1]['w_kb']),
                    reverse=True
                )[:10]

                for filename, stats in sorted_stats:
                    print(f"\nFile: {filename}")
                    full_path = find_kafka_file(filename)
                    if full_path:
                        print(f"Path: {full_path}")
                    print(f"Processes: {', '.join(stats['comm'])}")
                    print(f"Reads: {stats['reads']}, Read KB: {stats['r_kb']:.2f}")
                    print(f"Writes: {stats['writes']}, Write KB: {stats['w_kb']:.2f}")
                    print(f"Total KB: {stats['r_kb'] + stats['w_kb']:.2f}")
                    print("-" * 40)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Make sure to kill the filetop process
        process.terminate()
        try:
            process.wait(timeout=1)  # Wait for process to terminate
        except subprocess.TimeoutExpired:
            process.kill()  # Force kill if it doesn't terminate
        
        # Clean up any remaining filetop processes
        try:
            subprocess.run(["pkill", "-f", "filetop"], check=False)
        except subprocess.SubprocessError:
            pass

if __name__ == "__main__":
    try:
        monitor_filetop()
    except KeyboardInterrupt:
        cleanup_and_exit(None, None)
