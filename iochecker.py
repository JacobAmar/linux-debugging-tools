#!/usr/bin/env python3
import sys
from datetime import datetime

def analyze_peaks(device_name, threshold):
    threshold = float(threshold)
    waiting_for_zero = True
    start_zero_time = None
    peak_start_time = None
    in_peak = False
    
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
                print (f"Reads: {reads}")
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
                        print(f"Peak started at {current_time.strftime('%H:%M:%S')}")
                        print(f"Time from zero to peak: {time_to_peak:.2f} seconds")
                else:  # We're in a peak
                    if reads == 0:
                        in_peak = False
                        time_to_zero = (current_time - peak_start_time).total_seconds()
                        print(f"Returns to zero at {current_time.strftime('%H:%M:%S')}")
                        print(f"Time from peak to zero: {time_to_zero:.2f} seconds")
                        print("-" * 50)
                        # Reset start_zero_time for next cycle
                        start_zero_time = current_time
                        
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
