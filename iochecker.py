#!/usr/bin/env python3
import sys
import time
from datetime import datetime

def analyze_peaks(device_name, threshold):
    threshold = float(threshold)
    last_peak_time = None
    in_peak = False
    
    while True:
        line = sys.stdin.readline()
        if not line:
            break
            
        # Skip empty lines
        if not line.strip():
            continue
            
        # Split the line into fields
        fields = line.strip().split()
        
        # Check if this is a device line and if it's our target device
        if len(fields) > 0 and fields[0] == device_name:
            # r/s is in the 3rd column (index 2)
            try:
                reads = float(fields[2])
                current_time = datetime.now()
                
                # Check if we've hit a peak
                if reads >= threshold:
                    if not in_peak:
                        in_peak = True
                        if last_peak_time:
                            time_diff = (current_time - last_peak_time).total_seconds()
                            print(f"Peak detected at {current_time.strftime('%H:%M:%S')}")
                            print(f"Time since last peak: {time_diff:.2f} seconds")
                        else:
                            print(f"First peak detected at {current_time.strftime('%H:%M:%S')}")
                        last_peak_time = current_time
                else:
                    in_peak = False
                    
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
