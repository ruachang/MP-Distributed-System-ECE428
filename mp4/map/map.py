import csv
from collections import Counter
import hashlib
import sys 
import os
import re
import socket
import sys
sys.path.append("/home/changl25/mp4")
MAP_OUTPUT_PATH = ("/home/changl25/mp4/map_output")
DFS_PATH = ("/home/changl25/mp4/dfs")
from put import send_query_to_server

def map(filename, partition_num, interconne_type, batch_size=100):
    # The input parameter 'Interconne' type X.
    # File path to the CSV file
    file_path = os.path.join(DFS_PATH, filename)
    # Total count of intersections with 'Interconne' type X
    total_intersections = 0
    counter = {}
    # Read the CSV file
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        headers = next(csv_reader)  # Assume the first row is the header
        
        # Find the index for 'Interconne' and 'Detection_' columns
        interconne_idx = headers.index('Interconne')
        detection_idx = headers.index('Detection_')

        # Iterate over the rows in the CSV file
        for row in csv_reader:
            i = 0
            interconne_value = row[interconne_idx]
            detection_value = row[detection_idx]
            # Check if 'Interconne' is of type X
            if interconne_value == interconne_type:
                counter[detection_value] = counter.get(detection_value, 0) + 1
                if i == batch_size:
                    write_batch_to_files(counter, filename, partition_num)
                    counter = {}
                    i = 0
    if counter:
        write_batch_to_files(counter, filename, partition_num)
    # # Calculate the percent composition of 'Detection_'
    # percent_composition = {key: (value / total_intersections) * 100 for key, value in detection_counter.items()}

def write_batch_to_files(data, filename, partition_num):
    for word, count in data.items():
        hash_value = int(hashlib.md5(word.encode()).hexdigest(), 16)
        partition_index = hash_value % int(partition_num)
        with open(os.path.join(MAP_OUTPUT_PATH, f"{filename}_{partition_index}"), "a") as output_file:
            output_file.write(f"{word}\t{count}\n")

def main():
    _, sdfs_inter_prefix, sdfs_src_dir_partition, partition_num, interconne_type = sys.argv
    print(f"argument: {sdfs_inter_prefix} {sdfs_src_dir_partition} {partition_num} {interconne_type}")    
    map(sdfs_src_dir_partition, partition_num, interconne_type)
    filtered_files = [file for file in os.listdir(MAP_OUTPUT_PATH) if file.startswith(sdfs_src_dir_partition)]
    host = socket.gethostname()
    port = 5006
    for file in filtered_files:
        filename = sdfs_inter_prefix + "_" + file.split("_")[-1]
        send_query_to_server(os.path.join(MAP_OUTPUT_PATH, file), filename)
    
if __name__ == "__main__":
    main()
