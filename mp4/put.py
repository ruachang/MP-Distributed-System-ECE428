import os
import socket
import subprocess
import sys
import threading


def send_query_to_server(file_name, sdfsfilename, append = True):
    # Create a socket object
    server_address = socket.gethostname()
    server_port = 5003
    leader_port = 5002
    DFS_PATH = "/home/changl25/mp4/dfs"
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Connect to the server
    client_socket.connect((server_address, server_port))
    query = "leaderAddr"
    # Send the query to the server
    client_socket.send(query.encode('utf-8'))
    leader = client_socket.recv(1024).decode('utf-8')
    # Close the socket
    client_socket.close()
    leaderAddr = (leader, leader_port)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(leaderAddr)
    queryMessage = f"put {file_name} {sdfsfilename}"
    s.send(queryMessage.encode('utf-8'))
    print("sending query")
    peers = s.recv(4096).decode('utf-8').split()
    print("received peers: ", peers)
    scp_processes = []
    # * Append version
    # Create and start a new thread for each transfer
    def ssh_cat(filename, clientAddress, localfilename):
        subprocess.run(
            f'cat {filename} | ssh -o StrictHostKeyChecking=no changl25@{clientAddress} "cat >> {localfilename}"',
            shell=True
        )
        
    if append == True:
        for serverAddress in peers:     
            scp_thread = threading.Thread(target=ssh_cat, args=(file_name, serverAddress, os.path.join(DFS_PATH, sdfsfilename)))
            scp_processes.append(scp_thread)
            scp_thread.start()
        # Wait for all threads to finish
        for thread in scp_processes:
            thread.join()
    else:
        scp_processes = []
        # * Overwrite version
        for serverAddress in peers:     
            scp_process = subprocess.Popen(['scp','-o', 'StrictHostKeyChecking=no',file_name,f"changl25@{serverAddress}:{os.path.join(DFS_PATH, sdfsfilename)}"])
            scp_processes.append(scp_process)
        for scp_process in scp_processes:
            scp_process.wait()
    os.remove(file_name)
    for serverAddress in peers:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as p:
                p.connect((serverAddress,server_port))
                p.send(f"{queryMessage} {os.path.join(DFS_PATH, sdfsfilename)}".encode('utf-8'))
        except Exception as e:
            print(f"Error sending to {serverAddress}: {e}")
    s.send("Finish".encode("utf-8"))
    print("Finish {}".format(queryMessage))
    s.close()
    return
    
def break_file_into_chunks(file_path, directory_name, sdfsname):
    # Create the directory if it doesn't exist
    if not os.path.exists(directory_name):
        os.makedirs(directory_name)
    # Open the file in binary mode
    with open(file_path, 'r') as f:
        file_list = []
        file_line_lst = f.readlines()
        lines = len(file_line_lst)
        chunk_line = max(lines // 10, 10000)
        group_num = len(file_line_lst) // chunk_line + 1
        for group in range(group_num):
            splited_file = file_line_lst[group * chunk_line : min((group + 1) * chunk_line , len(file_line_lst))]
            written_file = "".join(splited_file)
            chunk_filename = f"{sdfsname}_{group}"
            # chunk_filename = f"{file_path.split('/')[-1]}_{group}"
            with open(os.path.join(directory_name, chunk_filename), 'w') as chunk_file:
                chunk_file.write(written_file)
                file_list.append(os.path.join(directory_name, chunk_filename))
    for file in file_list:
        print(file)
        send_query_to_server(file, file.split('/')[-1], False)

def main():
    sdfsname, file_name = sys.argv[1], sys.argv[2]
    subprocess.run("rm /home/changl25/mp4/upload/* 2>/dev/null", shell=True)
    break_file_into_chunks(file_name, "/home/changl25/mp4/upload", sdfsname)
    
if __name__ == "__main__":
    main()
