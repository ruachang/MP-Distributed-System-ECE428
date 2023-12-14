#!/usr/bin/env python
import sys
dataset1, dataset2 = sys.argv[1], sys.argv[2]
# Create empty DataFrame to store data
df_D1 = []
df_D2 = []
current_key = None
# Read input key-value pairs
# header_line = sys.stdin.readline().strip()

def print_current_key_value(data_lst1, data_lst2):
    # print("lst1: ", data_lst1)
    # print("lst2: ", data_lst2)
    for value_d1 in data_lst1:
        for value_d2 in data_lst2:
            print(f"{value_d1}{value_d2}")

for line in sys.stdin:
    # print("d1: ", df_D1, "d2: ", df_D2)
    if line:
        # print(line)
        try: 
            line = line.rstrip()
            length = len(line.split('\t'))
            if length == 1: 
                continue
            else:
                # print("line ", line)
                # try:
                join_key, record = line.split('\t', 1)
                identifier, data = record.split(',', 1)
                # print("key ", join_key)
                # print("data ", data)
                if current_key != join_key:
                    if current_key is not None:
                #     # One key finished
                        # print("current key: ", current_key)
                        print_current_key_value(df_D1, df_D2)
                    current_key = join_key
                    df_D1 = []
                    df_D2 = []
                # Append to corresponding DataFrame based on identifier
                if identifier == dataset1:
                    df_D1.append(data)
                    # print("append 1", df_D1)
                elif identifier == dataset2:
                    df_D2.append(data)
                        # print("append 2", df_D2)
                # except Exception as e:
                #     print("error: ", str(e))
                #     print("data ", len(line.split('\t')), line)
        except Exception as e:
            print("error: ", str(e))
            length = len(line.split('\t'))
            print(f"line: {length}, {line}")
if current_key is not None:
    print_current_key_value(df_D1, df_D2)