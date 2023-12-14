""" 
input: dataset key
output: hidden file in dfs(append in the dfs) 
the file is uploaded to the dfs through this file 
so the file is both putting and mapping(merge two dataset together into partitions)
"""
import datetime
import hashlib
import os
import socket
import sys
sys.path.append("/home/changl25/mp4")
import pandas as pd
from put import send_query_to_server
DFS = "/home/changl25/mp4/dfs"
JOIN_PATH = "/home/changl25/mp4/join_output"
# Define the number of splits
num_splits = 5
def hash_csv(dataset_name, col, sdfs_dest_name):
    # Read the entire CSV file
    df = pd.read_csv(os.path.join(DFS, dataset_name))
    # Compute hash values of the specified column and take modulo with num_splits
    df['hash_val'] = df[col].apply(lambda x: int(hashlib.md5(str(x).encode()).hexdigest(), 16) % num_splits)

    file_list = []
    # Split the DataFrame based on hash_val
    for i in range(num_splits):
        split_df = df[df['hash_val'] == i].drop('hash_val', axis=1) 
        # split_df = split_df.append(boundary_row, ignore_index=True)
        split_file_name = os.path.join(JOIN_PATH, f'{dataset_name}_{i}')
        file_list.append(split_file_name)
        split_df.to_csv(split_file_name, index=False)
        # boundary_row = f'--- End of File {datetime.datetime.now()}'
        with open(split_file_name, 'a') as file:
            file.write("\n")
    for file in file_list:
        print(file)
        send_query_to_server(file, sdfs_dest_name+"_"+file[-1])

def main():
    _, dataset_name, col, sdfs_dest_name = sys.argv
    print(f"argument: {dataset_name} {col} {sdfs_dest_name}")    
    hash_csv(dataset_name, col, sdfs_dest_name)
if __name__ == "__main__":
    main()