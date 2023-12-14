import hashlib
import os
import re
import socket
import sys
sys.path.append("/home/changl25/mp4")
MAP_OUTPUT_PATH = ("/home/changl25/mp4/map_output")

from put import send_query_to_server

def word_count(filename, partition_num, batch_size=100):
    # Create the map_output directory if it doesn't exist
    if not os.path.exists(MAP_OUTPUT_PATH):
        os.makedirs(MAP_OUTPUT_PATH)

    word_counts = {}
        # Open the file
    with open(os.path.join("dfs", filename), "r") as file:
        # Loop through each line in the file
        i = 0
        for line in file:
            i += 1
            # Split the line into words
            words = line.rstrip().split()
            # Loop through each word and update the word count
            for word in words:
                word_counts[word] = word_counts.get(word, 0) + 1
                # Check if it's time to write to the output files
                if i == batch_size:
                    write_batch_to_files(word_counts, filename, partition_num)
                    word_counts = {}
                    i = 0
    # Write any remaining word counts to the output files
    if word_counts:
        write_batch_to_files(word_counts, filename, partition_num)

def write_batch_to_files(word_counts, filename, partition_num):
    for word, count in word_counts.items():
        hash_value = int(hashlib.md5(word.encode()).hexdigest(), 16)
        partition_index = hash_value % int(partition_num)
        with open(os.path.join(MAP_OUTPUT_PATH, f"{filename}_{partition_index}"), "a") as output_file:
            output_file.write(f"{word}\t{count}\n")

def main():
    _, sdfs_inter_prefix, sdfs_src_dir_partition, partition_num = sys.argv
    print(f"argument: {sdfs_inter_prefix} {sdfs_src_dir_partition} {partition_num}")    
    word_count(sdfs_src_dir_partition, partition_num)
    filtered_files = [file for file in os.listdir(MAP_OUTPUT_PATH) if file.startswith(sdfs_src_dir_partition)]
    host = socket.gethostname()
    port = 5006
    for file in filtered_files:
        filename = sdfs_inter_prefix + "_" + file.split("_")[-1]
        send_query_to_server(os.path.join(MAP_OUTPUT_PATH, file), filename)
    
if __name__ == "__main__":
    main()