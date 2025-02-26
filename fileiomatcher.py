#!/usr/bin/env python3
import sys
import subprocess
import time
from datetime import datetime
from collections import defaultdict

def run_filetop():
    cmd = ["filetop", "-C", "-r", "50", "-d", "1"]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return process

def parse_filetop_line(line):
    try:
        parts = line.strip().split()
        if len(parts) >= 7 and parts[-2] == 'R':  # Regular file
            pid, comm, reads, writes, r_kb, w_kb, ftype, filename = (
                parts[0], parts[1], int(parts[2]), int(parts[3]), 
                float(parts[4]), float(parts[5]), parts[6], parts[7]
            )
            return {
                'pid': pid,
                'comm': comm,
                'reads': reads,
                'r_kb': r_kb,
                'filename': filename
            }
    except (ValueError, IndexError):
        pass
    return None

def find_full_path(segment_name, kafka_path="/mnt/kafka-disks"):
    cmd = f"find {kafka_path} -name {segment_name}"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.stdout.strip()
    except subprocess.SubprocessError:
        return None

def analyze_peaks(device_name, threshold):
    threshold = float(threshold)
    waiting_for_zero = True
    start_zero_time = None
    peak_start_time = None
    in_peak = False
    filetop_process = None
    file_stats = defaultdict(lambda: {'reads': 0, 'r_kb': 0})
    
    while True:
        line = sys.stdin.readline()
        if not line:
            break
            
        if not line.strip():
            continue
            
        fields = line.strip().split()
        
        if len(fields) > 0 and fields[0] == device_name:
            try:
                reads = float(fields[2])
                current_time = datetime.now()
                
                # Wait for initial zero
                if waiting_for_zero:
                    if reads == 0:
                        waiting_for_zero = False
                        start_zero_time = current_time
                        print(f"Found initial zero at {current_time.strftime('%H:%M:%S')}")
                    continue

                # After finding initial zero
                if not in_peak:
                    if reads >= threshold:
                        in_peak = True
                        peak_start_time = current_time
                        time_to_peak = (peak_start_time - start_zero_time).total_seconds()
                        print(f"\nPeak started at {current_time.strftime('%H:%M:%S')}")
                        print(f"Time from zero to peak: {time_to_peak:.2f} seconds")
                        
                        # Start filetop when peak begins
                        if filetop_process:
                            filetop_process.terminate()
                        filetop_process = run_filetop()
                        file_stats.clear()
                        
                else:  # We're in a peak
                    if reads == 0:
                        in_peak = False
                        time_to_zero = (current_time - peak_start_time).total_seconds()
                        print(f"Returns to zero at {current_time.strftime('%H:%M:%S')}")
                        print(f"Time from peak to zero: {time_to_zero:.2f} seconds")
                        
                        # Stop filetop and analyze results
                        if filetop_process:
                            filetop_process.terminate()
                            print("\nMost read files during peak:")
                            # Sort by reads and print top 10
                            sorted_stats = sorted(file_stats.items(), 
                                               key=lambda x: x[1]['r_kb'], 
                                               reverse=True)[:10]
                            for filename, stats in sorted_stats:
                                full_path = find_full_path(filename)
                                path_info = f" -> {full_path}" if full_path else ""
                                print(f"File: {filename}{path_info}")
                                print(f"Total reads: {stats['reads']}, Total KB read: {stats['r_kb']:.2f}")
                            
                        print("-" * 50)
                        start_zero_time = current_time

                # Process filetop output during peak
                if in_peak and filetop_process:
                    while True:
                        filetop_out = filetop_process.stdout.readline()
                        if not filetop_out:
                            break
                        parsed = parse_filetop_line(filetop_out)
                        if parsed:
                            filename = parsed['filename']
                            file_stats[filename]['reads'] += parsed['reads']
                            file_stats[filename]['r_kb'] += parsed['r_kb']
                            
            except (ValueError, IndexError):
                continue

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: iostat -x 1 | ./script.py <device_name> <threshold>")
        sys.exit(1)
        
    device_name = sys.argv[1]
    threshold = sys.argv[2]
    
    try:
        analyze_peaks(device_name, threshold)
    except KeyboardInterrupt:
        print("\nAnalysis terminated by user")
    finally:
        # Cleanup any running filetop process
        subprocess.run(["pkill", "-f", "filetop"])
