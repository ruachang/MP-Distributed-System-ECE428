#!/usr/bin/env python
import sys

# The second argument is the name of the key column
dataset, key_column_name = sys.argv[1], sys.argv[2]

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

# Read the header
header_line = sys.stdin.readline().rstrip()
header = parse_csv_line(header_line)
# print(header)
# Find the index of the key column
if key_column_name in header:
    # raise ValueError(f"Key column '{key_column_name}' not found in header: {header}")
    key_index = header.index(key_column_name)

# # Process each line
for line_number, line in enumerate(sys.stdin, start=2):  # Start counting lines from 2 (after the header)
    line = line.rstrip()
    if line:
        try: 
            fields = parse_csv_line(line)
            # Check if the line has enough fields
            if key_index >= len(fields):
#                 sys.stderr.write(f"ERROR: Line {line_number} does not contain key index {key_index}: {line}\n")
                continue  # Skip this line
            join_key_value = fields[key_index]
            print(f"{str(join_key_value)}\t{str(dataset)},{str(line)}", flush=True)
        except Exception as e:
            sys.stderr.write('ERROR: %s\n' % str(e))