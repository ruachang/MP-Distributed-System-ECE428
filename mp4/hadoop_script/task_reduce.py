#!/usr/bin/env python
import sys

current_detection = None
current_count = 0
total_count = 0
detection_counts = {}

# Input comes from STDIN
for line in sys.stdin:
    line = line.rstrip()
    detection, count = line.split('\t', 1)

    # Convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        continue

    if current_detection == detection:
        current_count += count
    else:
        if current_detection is not None:
            # Write result to STDOUT
            detection_counts[current_detection] = current_count
            total_count += current_count
        current_detection = detection
        current_count = count

# Output the last detection type if needed
if current_detection == detection:
    detection_counts[current_detection] = current_count
    total_count += current_count

# Calculate and print the percentage composition
for detection, count in detection_counts.items():
    percentage = (count / total_count) * 100
    print(f"{detection}\t{percentage:.2f}%")
