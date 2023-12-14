""" 
csv reading and spliting is different from txt
the upload for csv
The chunked csv file is uploaded just like the txt file
"""
import os
import socket
import subprocess
import sys
import pandas as pd
host = socket.gethostname()
port = 5006
from put import send_query_to_server

def break_file_into_chunks(file_path, directory_name, sdfsname):
    # Create the directory if it doesn't exist
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
    with open(file_path, 'r') as f:
        content = f.readlines()
        lines = len(content)
    
    chunk_line = max(lines // 10, 10000)
    file_list = []
    counter = 0
    for chunk in pd.read_csv(file_path, chunksize=chunk_line):
        # chunk_filename = f"{file_path.split('/')[-1]}_{counter}"
        chunk_filename = f"{sdfsname}_{counter}"
        file_list.append(chunk_filename)
        chunk.to_csv(os.path.join(chunk_filename), index=False)
        counter += 1
    for file in file_list:
        print(file)
        send_query_to_server(file, file.split('/')[-1], False)

def main():
    sdfsname, file_name = sys.argv[1], sys.argv[2]
    subprocess.run("rm /home/changl25/mp4/upload/* 2>/dev/null", shell=True)
    break_file_into_chunks(file_name, "/home/changl25/mp4/upload", sdfsname)
    
if __name__ == "__main__":
    main()
