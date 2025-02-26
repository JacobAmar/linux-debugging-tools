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
        # Check if line matches expected format
        if len(parts) >= 8 and parts[-1].endswith('.log'):
            tid, comm, reads, writes, r_kb, w_kb, ftype, filename = (
                parts[0], parts[1], int(parts[2]), int(parts[3]), 
                float(parts[4]), float(parts[5]), parts[6], parts[7]
            )
            return {
                'tid': tid,
                'comm': comm,
                'reads': reads,
                'writes': writes,
                'r_kb': r_kb,
                'w_kb': w_kb,
                'filename': filename
            }
    except (ValueError, IndexError):
        pass
    return None

def find_kafka_files(segment_name, kafka_paths=["/mnt/kafka-disks"]):
    found_files = []
    for base_path in kafka_paths:
        cmd = f"find {base_path} -name {segment_name} 2>/dev/null"
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.stdout.strip():
                found_files.extend(result.stdout.strip().split('\n'))
        except subprocess.SubprocessError:
            continue
    return found_files

def analyze_peaks(device_name, threshold):
    threshold = float(threshold)
    waiting_for_zero = True
    start_zero_time = None
    peak_start_time = None
    in_peak = False
    filetop_process = None
    file_stats = defaultdict(lambda: {'reads': 0, 'writes': 0, 'r_kb': 0, 'w_kb': 0, 'comm': set()})
    
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
                
                if waiting_for_zero:
                    if reads == 0:
                        waiting_for_zero = False
                        start_zero_time = current_time
                        print(f"Found initial zero at {current_time.strftime('%H:%M:%S')}")
                    continue

                if not in_peak:
                    if reads >= threshold:
                        in_peak = True
                        peak_start_time = current_time
                        time_to_peak = (peak_start_time - start_zero_time).total_seconds()
                        print(f"\nPeak started at {current_time.strftime('%H:%M:%S')}")
                        print(f"Time from zero to peak: {time_to_peak:.2f} seconds")
                        
                        if filetop_process:
                            filetop_process.terminate()
                        filetop_process = run_filetop()
                        file_stats.clear()
                        
                else:  # In peak
                    if reads == 0:
                        in_peak = False
                        time_to_zero = (current_time - peak_start_time).total_seconds()
                        print(f"Returns to zero at {current_time.strftime('%H:%M:%S')}")
                        print(f"Time from peak to zero: {time_to_zero:.2f} seconds")
                        
                        if filetop_process:
                            filetop_process.terminate()
                            print("\nMost accessed files during peak:")
                            # Sort by total KB (read + write)
                            sorted_stats = sorted(file_stats.items(), 
                                               key=lambda x: (x[1]['r_kb'] + x[1]['w_kb']), 
                                               reverse=True)[:10]
                            for filename, stats in sorted_stats:
                                print(f"\nFile: {filename}")
                                paths = find_kafka_files(filename)
                                if paths:
                                    print(f"Full path(s):")
                                    for path in paths:
                                        print(f"  -> {path}")
                                print(f"Processes: {', '.join(stats['comm'])}")
                                print(f"Reads: {stats['reads']}, Read KB: {stats['r_kb']:.2f}")
                                print(f"Writes: {stats['writes']}, Write KB: {stats['w_kb']:.2f}")
                            
                        print("-" * 50)
                        start_zero_time = current_time

                # Process filetop output during peak
                if in_peak and filetop_process:
                    filetop_out = filetop_process.stdout.readline()
                    if filetop_out and not filetop_out.startswith('TID') and 'loadavg' not in filetop_out:
                        parsed = parse_filetop_line(filetop_out)
                        if parsed:
                            filename = parsed['filename']
                            file_stats[filename]['reads'] += parsed['reads']
                            file_stats[filename]['writes'] += parsed['writes']
                            file_stats[filename]['r_kb'] += parsed['r_kb']
                            file_stats[filename]['w_kb'] += parsed['w_kb']
                            file_stats[filename]['comm'].add(parsed['comm'])
                            
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
