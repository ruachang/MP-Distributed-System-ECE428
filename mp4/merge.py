import os
import socket
import sys
import pandas as pd
import re
from put import send_query_to_server
from io import StringIO
SAVE_PATH = "/home/changl25/mp4/join_output"
DFS_PATH = "/home/changl25/mp4/dfs"
def merge(file_path, filter_key1, filter_key2):

    with open(os.path.join(DFS_PATH,file_path), 'r') as file:
        file_content = file.read()
    datasets = re.split(r"\n{2,}", file_content)[:-1]
    dic = {}
    for dataset in datasets:
        df = pd.read_csv(StringIO(dataset))
        col = tuple(df.columns)
        if col not in dic.keys():
            dic[col] = df 
        else:
            dic[col] = pd.concat([dic[col], df])
    key1, key2 = dic.keys()
    joined_data = pd.merge(dic[key1], dic[key2], left_on=filter_key1, right_on=filter_key2, how="outer")
    joined_file_path = os.path.join(SAVE_PATH, file_path)
    joined_data.to_csv(joined_file_path)
    return joined_file_path
def main():
    _, file_path, filter_key1, filter_key2 = sys.argv
    joined_file_path = merge(file_path, filter_key1, filter_key2)
    send_query_to_server(joined_file_path, file_path, False)
    
if __name__ == "__main__":
    main()