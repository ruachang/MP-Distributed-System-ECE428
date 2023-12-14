import hashlib
import os
import re
import socket
import sys
import time
sys.path.append("/home/changl25/mp4")
MAP_OUTPUT_PATH = ("/home/changl25/mp4/map_output")

from put import send_query_to_server

def grep(filename, condition, batch_size=100):
    # Create the map_output directory if it doesn't exist
    if not os.path.exists(MAP_OUTPUT_PATH):
        os.makedirs(MAP_OUTPUT_PATH)

    grep_res = {1: []}
        # Open the file
    with open(os.path.join("dfs", filename), "r") as file:
        # Loop through each line in the file
        i = 0
        for line in file:
            i += 1
            if re.search(condition, line):
                grep_res[1].append(line)
            # Loop through each word and update the word count
                if i == batch_size:
                    write_batch_to_files(grep_res, filename, 1)
                    grep_res = {1: []}
                    i = 0
    # Write any remaining word counts to the output files
    if grep_res:
        write_batch_to_files(grep_res, filename, 1)

def write_batch_to_files(grep_res, filename, partition_num):
    for word, count in grep_res.items():
        hash_value = int(hashlib.md5(str(word).encode()).hexdigest(), 16)
        partition_index = hash_value % int(partition_num)
        for line in count:
            with open(os.path.join(MAP_OUTPUT_PATH, f"{filename}_{partition_index}"), "a") as output_file:
                output_file.write(f"{line}")

def main():
    _, sdfs_des_file, sdfsfilename, _, condition  = sys.argv
    print(condition)
    if condition[0] == "\"" and condition[-1] == "\"":
        condition = condition[1:-1] 
    grep(sdfsfilename, condition)
    host = socket.gethostname()
    port = 5006
    filtered_files = [file for file in os.listdir(MAP_OUTPUT_PATH) if file.startswith(sdfsfilename)]
    for file in filtered_files:
        filename = sdfs_des_file + "_" + file.split("_")[-1]
        send_query_to_server(os.path.join(MAP_OUTPUT_PATH, file), filename, False)
    
if __name__ == "__main__":
    main()