import os
import sys
sys.path.append("/home/changl25/mp4")
import socket
from put import send_query_to_server

REDUCE_INPATH = "/home/changl25/mp4/dfs"
REDUCE_OUTPATH = "/home/changl25/mp4/reduce_output"
def send_delete_to_server(sdfs_filename):
    server_address, server_port = socket.gethostname(), 5006
    # Create a socket object
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Connect to the server
    client_socket.connect((server_address, server_port))
    query = f"delete {sdfs_filename}"
    # Send the query to the server
    client_socket.send(query.encode('utf-8'))
    # Close the socket
    client_socket.close()
def word_count(sdfs_dest_file, sdfs_juice_file):
    # Create the map_output directory if it doesn't exist
    if not os.path.exists("reduce_output"):
        os.makedirs("reduce_output")
    word_counts = {}
    with open(os.path.join(REDUCE_INPATH, sdfs_juice_file), "r") as input_file:
        for line in input_file:
            word, count = line.rstrip().split("\t")
            word_counts[word] = word_counts.get(word, 0) + int(count)
    with open(os.path.join(REDUCE_OUTPATH, f"wordCount_{sdfs_juice_file}"), "w") as output_file:
        for word, count in word_counts.items():
            output_file.write(f"{word}\t{count}\n")
    send_query_to_server(os.path.join(REDUCE_OUTPATH, f"wordCount_{sdfs_juice_file}"), sdfs_dest_file)

def main():
    _, sdfs_juice_file, sdfs_dest_file, delete_input = sys.argv
    print(f"argument: {sdfs_juice_file} {sdfs_dest_file}")
    word_count(sdfs_dest_file, sdfs_juice_file)
    if delete_input:
        send_delete_to_server(sdfs_juice_file)
if __name__ == "__main__":
    main()