#!/usr/bin/env python
import sys
import csv
from io import StringIO
# Get the 'Interconne' type from the command line arguments
interconne_type = sys.argv[1]

# Function to manually parse a CSV line
def parse_csv_line(line):
    # Handle escaped quotes and split the line by comma
    fields = []
    field = ''
    in_quotes = False
    for char in line:
        if char == '"':
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            fields.append(field)
            field = ''
        else:
            field += char
    fields.append(field)  # add the last field
    return fields

for line_number, line in enumerate(sys.stdin, start=2):  # Start counting lines from 2 (after the header)
    line = line.rstrip()
    # Assuming CSV header on the first line and comma-separated values
    if line:
        fields = parse_csv_line(line)
        interconne = fields[10]  # Replace with actual column index for 'Interconne'
        detection = fields[9]  # Replace with actual column index for 'Detection_'
        #if detection == "\'\'":
        #    print(f"{detection}\t1")
        if interconne == interconne_type:
            print(f"{detection}\t1")
