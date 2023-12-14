import copy
from datetime import datetime
import logging
import socket
import subprocess
import json
import random
import threading
import os
import time
from collections import defaultdict
from node import MembershipList
HOST_NAME_LIST = [
    'fa23-cs425-8001.cs.illinois.edu',
    'fa23-cs425-8002.cs.illinois.edu',
    'fa23-cs425-8003.cs.illinois.edu',
    'fa23-cs425-8004.cs.illinois.edu',
    'fa23-cs425-8005.cs.illinois.edu',
    'fa23-cs425-8006.cs.illinois.edu',
    'fa23-cs425-8007.cs.illinois.edu',
    'fa23-cs425-8008.cs.illinois.edu',
    'fa23-cs425-8009.cs.illinois.edu',
    'fa23-cs425-8010.cs.illinois.edu',
]

logging.basicConfig(level=logging.DEBUG,
                    # filename='time.log',
                    datefmt='%Y/%m/%d %H:%M:%S',
                    format='%(asctime)s - %(levelname)s - %(message)s')
timeLogger = logging.getLogger("time_logger")
timeHandler = logging.FileHandler("time.log")
timeLogger.addHandler(timeHandler)
LEADER_UDP_PORT = 12345
UDP_PORT = 5000
FD_PORT = 5005
LEADER_PORT = 5002
LEADER_IP = "fa23-cs425-8002.cs.illinois.edu"
PORT = 5003
# DFS_PATH = '/home/anphan2/mp3/dfs/'
DFS_PATH = '/home/changl25/mp3/dfs/'
USER = 2
class Server:
    def __init__(self):
        self.ip = socket.gethostname()
        self.port = PORT
        self.MembershipList = set(HOST_NAME_LIST)
        self.num_copy = 4
        self.fileToServer = defaultdict(list)
        self.serverToFile = defaultdict(list)
        self.fileToLock = {}
        self.failureDetectionServer = MembershipList(introducor=LEADER_IP, port_num=FD_PORT)
        self.localFile = {} #sdfsname : localfilename
        self.leaderAddr = (None, None)
        self.addr = (self.ip, self.port)
        self.isLeader = False
        self.leaderFailed = False
        self.user = "anphan2" if USER == 1 else "changl25"
        self.rlock = threading.RLock()
        self.udplock = threading.Lock()
        self.refresh_dfs(DFS_PATH)
    def refresh_dfs(self, dir_path):
        file_list = os.listdir(dir_path)
        for file in file_list:
            file_path = os.path.join(dir_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
        file_path = os.path.join(os.path.dirname(__file__), "output.log")
        os.remove(file_path)
    def store(self):
        print(self.localFile)
    def UDP_server(self):
        # Create a UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Bind the socket to the specified IP and port
        self.sock.bind((self.ip, UDP_PORT))

        while True:
            # Receive a message and the client's address
            data, client_address = self.sock.recvfrom(1024)

            # Decode the received data
            message = data.decode('utf-8')

            # Check if the received message is "ping"
            if message.lower() == "ping":
                # Send "pong" back to the client
                response_message = "pong"
                self.sock.sendto(response_message.encode('utf-8'), (client_address[0],LEADER_UDP_PORT))

    # Update membershiplist and dictionary of file to server and server to file
    def updateMembershipList(self):
        while True:
            self.leaderAddr = (self.failureDetectionServer.returnLeader(), LEADER_PORT)
            if self.leaderAddr[0] == self.ip:
                self.isLeader = True
            with self.rlock:
                prev_membershipList = set([key for key in self.serverToFile.keys()])
                self.MembershipList = set(self.failureDetectionServer.returnDetectedKey())
            # If it is a leader, delete the fail servers and renew the dictionary
            if self.isLeader:
                failedMembers = prev_membershipList.difference(self.MembershipList)
                for failedMember in failedMembers:
                    hold_file_list = copy.deepcopy(self.serverToFile[failedMember])
                    del self.serverToFile[failedMember]
                    for file in hold_file_list:
                        self.fileToServer[file].remove(failedMember)
                self.checkLostCopy()
            # If it is not a leader and the leader fail, re-elect a leader
            else:
                if self.leaderAddr[0] not in self.MembershipList:
                    self.leaderFailed = True
                    # If it is the new leader
                    if self.ip == max(self.MembershipList):
                        # send to every one in the member that new leader is re elected
                        self.MembershipList = set(self.failureDetectionServer.returnDetectedKey())
                        for member in self.MembershipList:
                            try:
                                # ask every member to send back the file name and sdfsname
                                # Then the leader can remake the dictionary
                                query = "leader " + self.ip
                                clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                # clientSocket.settimeout(5)
                                print("Sent ip to get local file: ", member)
                                clientSocket.connect((member, self.port))
                                clientSocket.send(query.encode('utf-8'))
                                localFile = clientSocket.recv(8192).decode('utf-8')
                                localFile = json.loads(localFile)
                                for filename in localFile.keys():
                                    self.serverToFile[member].append(filename)
                                    self.fileToServer[filename].append(member)
                                clientSocket.close()
                            except Exception as e:
                                print(f"Error connection to {member}")
                                continue
                        for sdfsfilename in self.fileToServer.keys():
                            self.fileToLock[sdfsfilename] = [threading.Lock(),threading.Lock(),0]
                        self.checkLostCopy()
                        # renew the information of leader
                        self.leaderAddr = (self.ip,LEADER_PORT)
                        self.isLeader == True
                        self.leaderFailed = False
                        leader_thread = threading.Thread(target=self.leaderServer)
                        leader_thread.daemon = True
                        leader_thread.start()
                        self.failureDetectionServer.leader = self.ip
                        self.failureDetectionServer.presidency += 1
                        for member in self.MembershipList:
                            query = "Finish Leader re-elect"
                            try:
                                clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                # clientSocket.settimeout(5)
                                print("Sent ip: ", member)
                                clientSocket.connect((member, self.port))
                                clientSocket.send(query.encode('utf-8'))
                                clientSocket.close()
                            except Exception as e:
                                print("Error connecting {}".format(self.leaderAddr[0]))
                
            time.sleep(0.5)

    # Check for is there any lost server
    def checkLostCopy(self):
        with self.rlock:
            fileToServer_copy = copy.deepcopy(self.fileToServer)
            ipList = list(self.MembershipList)
            for fileName, servers in fileToServer_copy.items():
                for i in range(min(self.num_copy - len(servers), len(ipList) - len(servers))):
                    print("fileToServer in renew", self.fileToServer)    
                    sender = random.choice(servers)
                    valid_ip_list = [ip for ip in ipList if ip not in self.fileToServer[fileName]]
                    receiver = random.choice(valid_ip_list)
                    print("choose {} to send {} to {}".format(sender, fileName, receiver))
                    self.fileToServer[fileName].append(receiver)
                    self.serverToFile[receiver].append(fileName)
                    query = "renew " + fileName + " _ " + receiver
                    try:
                        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        clientSocket.connect((sender, self.port))
                        clientSocket.send(query.encode('utf-8'))
                        clientSocket.close()
                    except Exception as e:
                        print("Error connecting {}".format(self.leaderAddr[0]))
                        continue
                
    def ping_servers(self, server_addresses):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client_socket:
            for server_address in server_addresses:
                # Send "ping" to each server in the list
                client_socket.sendto("ping".encode('utf-8'), (server_address, UDP_PORT))
    
    # The leader function    
    def handle_leader(self,clientSocket,clientAddress):
        clientHostname, _, _ = socket.gethostbyaddr(clientAddress[0])
        queryMessage = clientSocket.recv(4096).decode('utf-8') # First receive the query
        print("receving query from user", queryMessage)
        query = queryMessage.split()
        # query: put localfilename sdfsfilename, get sdfsfilename localfilename clientaddress, delete sdfsfilename
        c = query[0]
        if c == "put":
            print("putting from leader")
            _, localfilename, sdfsfilename = query
            if sdfsfilename in self.fileToServer:
            # if it is already in the file system, update
                with self.fileToLock[sdfsfilename][0]:
                    while self.fileToLock[sdfsfilename][2] != 0:
                        time.sleep(0.1)
                    candidates = self.fileToServer[sdfsfilename]
                    clientSocket.send(" ".join(candidates).encode('utf-8'))
                    clientSocket.recv(1024)
            else:
            # randomly select three machines to copy to and 
            # send back the candidate list to sender
                self.fileToLock[sdfsfilename] = [threading.Lock(),threading.Lock(),0]
                with self.fileToLock[sdfsfilename][0]:
                    while self.fileToLock[sdfsfilename][2] != 0:
                        time.sleep(0.1)
                    membersIp = list(self.MembershipList) #send 4 random machine address
                    # membersIp = [ip for ip in membersIps if ip != clientHostname]
                    print("MembersIP:", membersIp)
                    random.shuffle(membersIp)
                    # ? send to 3 of them and add itself in the list?
                    candidates = membersIp[:self.num_copy]
                    # * add itself to the list
                    # candidates.append(clientAddress[0])
                    clientSocket.send(" ".join(candidates).encode('utf-8'))
                    # candidates.append(clientHostname)
                    self.fileToServer[sdfsfilename] = candidates
                    for c in candidates:
                        self.serverToFile[c].append(sdfsfilename)
                    clientSocket.recv(1024)
            print(clientSocket.recv(1024).decode('utf-8'))
            print("Candidate" ,candidates)
            
        elif c == "get":
            _, sdfsfilename, _, _ = query
            if sdfsfilename not in self.fileToServer.keys():
                clientSocket.send("There isn't such file in the system.".encode("utf-8"))
            else:
                # ping to the servers in the list and send to the one that give feedback
                with self.fileToLock[sdfsfilename][0]:
                    with self.fileToLock[sdfsfilename][1]:
                        self.fileToLock[sdfsfilename][2] += 1
                with self.udplock:
                    server_addresses = self.fileToServer[sdfsfilename].copy()
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s1:       
                        s1.bind((self.ip,LEADER_UDP_PORT))
                        ping_thread = threading.Thread(target=self.ping_servers, args=(server_addresses,))
                        ping_thread.start()
                        data, server_address = s1.recvfrom(4096)
                clientSocket.send(f"The {socket.gethostbyaddr(server_address[0])[0]} will send back the file.".encode("utf-8"))
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((server_address[0], PORT))
                    s.send(queryMessage.encode('utf-8'))
                    finish_info = s.recv(1024).decode('utf-8')
                    clientSocket.send(finish_info.encode('utf-8'))
                    with self.fileToLock[sdfsfilename][1]:
                        self.fileToLock[sdfsfilename][2] -= 1
                    s.close()
                except Exception as e:
                    print("Error connecting {}".format(self.leaderAddr[0]))
                
        elif c == "delete":
        # delete the file from all of the system
            _, sdfsfilename = query
            if sdfsfilename not in self.fileToServer:
                clientSocket.send(f"{sdfsfilename} is not in the system".encode("utf-8"))
            else:
                with self.fileToLock[sdfsfilename][0]:
                    while self.fileToLock[sdfsfilename][2] != 0:
                        time.sleep(0.1)
                    clientSocket.send(f"{sdfsfilename} deleted".encode("utf-8"))
                    candidates = self.fileToServer[sdfsfilename]
                    del self.fileToServer[sdfsfilename]
                    del self.fileToLock[sdfsfilename]
                    finish_info = "Successfully deleted from the system."
                    clientSocket.send(finish_info.encode('utf-8'))
                    for serverAddress in candidates:
                        self.serverToFile[serverAddress].remove(sdfsfilename)
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                                s.connect((serverAddress,PORT))
                                s.send(queryMessage.encode('utf-8'))
                        except Exception as e:
                            print(f"Error sending to {serverAddress}: {e}")

        elif c == "ls":
            sdfs_file = query[1]
            print("send the servers that has the file {} to {}".format(sdfs_file, clientAddress))
            if sdfs_file in self.fileToServer.keys():
                clientSocket.send(str(self.fileToServer[sdfs_file]).encode("utf-8"))
            else:
                clientSocket.send(f"The {sdfs_file} is not stored in the system".encode('utf-8'))
        elif c == "print":
            print("Send the whole list to {}".format(clientAddress))
            clientSocket.send(json.dumps(self.fileToServer).encode("utf-8"))
        clientSocket.close()
        
    def leaderServer(self):
        print("starting leader server")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serverSocket:
            serverSocket.bind((self.ip, LEADER_PORT))
            serverSocket.listen()
            while True:
                clientSocket, clientAddress = serverSocket.accept()
                threading.Thread(target=self.handle_leader,args=(clientSocket,clientAddress,)).start()
    # The thread to deal with the building connection
    def handle_receive(self,clientSocket,clientAddress):
        queryMessage = clientSocket.recv(4096).decode('utf-8') # First receive the query
        query = queryMessage.split() # query: put localfilename sdfsfilename filename, get sdfsfilename localfilename clientAddress, delete sdfsfilename
        c = query[0]
        # renew the local file of the copied machine
        if c == "put":
            _, localfilename, sdfsfilename, filename = query
            self.localFile[sdfsfilename] = filename
            print("Local file after put: ", self.localFile)
        # ask the server to send the file to the queryer
        elif c == "get":
            _, sdfsfilename, localfilename, clientAddress = query
            filename = self.localFile[sdfsfilename]
            print(['scp',filename,f"{self.user}@{clientAddress}:{localfilename}"])
            scp_process = subprocess.Popen(['scp',filename,f"{self.user}@{clientAddress}:{localfilename}"])
            scp_process.wait()
            clientSocket.send(("Finish " + queryMessage).encode('utf-8'))
            timestamp = time.time()
            timeLogger.info("[GET]  - {} Send to {} -      end - {}".format(sdfsfilename, clientAddress, datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') + ":{}".format(int((timestamp * 1000) % 1000))))
        elif c == "delete":
            _, sdfsfilename = query
            localfile = self.localFile[sdfsfilename]
            subprocess.Popen(['rm', localfile])
            del self.localFile[sdfsfilename]
            print(self.localFile)
        elif c == "renew":
            _, sdfsfilename, localfilename, clientAddress = query
            # * if is query msg sent from the server
            if localfilename == "_":
                filename = self.localFile[sdfsfilename]
                print(['scp',filename,f"{self.user}@{clientAddress}:{filename}"])
                scp_process = subprocess.Popen(['scp',filename,f"{self.user}@{clientAddress}:{filename}"])
                scp_process.wait()
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((clientAddress,PORT))
                        queryMessage = "renew " + sdfsfilename + " " + filename + " _"
                        s.send(f"{queryMessage}".encode('utf-8'))
                except Exception as e:
                    print(f"Error sending to {clientAddress}: {e}")
            # * if is the informing msg sent from the client
            elif clientAddress == "_":
                self.localFile[sdfsfilename] = localfilename
                timestamp = time.time()
                timeLogger.info("[RENEW]    end - {}".format(datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') + ":{}".format(int((timestamp * 1000) % 1000))))
                print("After renew, the local file after put: ", self.localFile)
        # if Multiread, reconstruct a "get" cmd and send to the leader server
        elif c == "Multiread":
            _, sdfsfilename, local = query
            print("Receive cmd to read {}".format(sdfsfilename))
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect(self.leaderAddr)
                s.send(f"get {sdfsfilename} {local} {self.ip}".encode('utf-8'))
                leader_info = s.recv(1024).decode("utf-8")
                # print("Multiread: " + leader_info)
                finish_info = s.recv(1024).decode("utf-8")
                print(finish_info)
                s.close()
            except Exception as e:
                print("Error connecting {}".format(self.leaderAddr[0]))
            # clientSocket.send("{} {}".format(self.ip, leader_info).encode('utf-8'))
        elif c == "leader":
            _, leader_ip = query
            self.leaderAddr = (leader_ip, LEADER_PORT)
            # if leader_ip != self.ip:
            clientSocket.send(json.dumps(self.localFile).encode('utf-8'))
        elif c == "Finish":
            if queryMessage == "Finish Leader re-elect":
                self.leaderFailed = False
            print("Finish " + queryMessage[3:])
            
    def receive(self):
        print("starting server")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serverSocket:
            serverSocket.bind(self.addr)
            serverSocket.listen()
            while True:
                clientSocket, clientAddress = serverSocket.accept()
                threading.Thread(target=self.handle_receive,args=(clientSocket,clientAddress,)).start()

    def querry(self):
        print("start receiving query")
        while True:
            queryMessage = input()
            threading.Thread(target=self.handle_querry,args=(queryMessage,)).start()
    # thread to deal with query            
    def handle_querry(self, queryMessage):
        query = queryMessage.split() # query: put localfilename sdfsfilename, get sdfsfilename localfilename, delete sdfsfilename
        while True:
            if len(query) >= 1:
                c = query[0]
                if c == "put":
                    # TEST
                    timestamp = time.time()
                    if len(query) != 3:
                        print("Wrong format. The put should be in the format of put [local_name] [sdfs_name]")
                        return
                    _, localfilename, sdfsfilename = query
                    timeLogger.info("[PUT]  - {}    start - {}".format(localfilename, datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') + ":{}".format(int((timestamp * 1000) % 1000))))
                    if not os.path.exists(localfilename):
                        print("There isn't such a file, please check the path of the local path")
                        return
                    print(query)
                    print("putting file")
                    # get the machine to copy to from the leader and send them the file
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect(self.leaderAddr)
                        print("connecting leader")
                        s.send(queryMessage.encode('utf-8'))
                        print("sending query")
                        peers = s.recv(4096).decode('utf-8').split()
                        print("received peers: ", peers)
                        scp_processes = []
                        for serverAddress in peers:     
                            scp_process = subprocess.Popen(['scp',localfilename,f"{self.user}@{serverAddress}:{os.path.join(DFS_PATH, sdfsfilename)}"])
                            scp_processes.append(scp_process)
                        for scp_process in scp_processes:
                            scp_process.wait()

                        for serverAddress in peers:
                            try:
                                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as p:
                                    p.connect((serverAddress,PORT))
                                    p.send(f"{queryMessage} {os.path.join(DFS_PATH, sdfsfilename)}".encode('utf-8'))
                            except Exception as e:
                                print(f"Error sending to {serverAddress}: {e}")
                        s.send("Finish".encode("utf-8"))
                        timestamp = time.time()
                        timeLogger.info("[PUT]  - {}      end - {}".format(localfilename, datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') + ":{}".format(int((timestamp * 1000) % 1000))))
                        print("Finish {}".format(queryMessage))
                        s.close()
                        return
                    except Exception as e:
                        print("Error connecting {}, retry in 5 seconde".format(self.leaderAddr[0]))
                        time.sleep(5)
                        continue
                
                elif c == "get":
                # tell the server there is a get cmd
                    timestamp = time.time()
                    if len(query) != 3:
                        print("Wrong format. The get should be get [sdfsname] [local_path]")
                        return
                    timeLogger.info("[GET]      start - {}".format(datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S') + ":{}".format(int((timestamp * 1000) % 1000))))
                    print("sending query message to leader", queryMessage)
                    try:
                    # ? get from single peer or from multiple peers and pick from the fastest ?
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect(self.leaderAddr)
                        # ? do we need to send four params?
                        s.send(f"{queryMessage} {self.ip}".encode('utf-8'))
                        leader_info = s.recv(1024).decode("utf-8")
                        print(leader_info)
                        finish_info = s.recv(1024).decode("utf-8")
                        print(finish_info)
                        s.close()
                        return
                    except Exception as e:
                        print("Error connecting to leader {}, retry in 5 second".format(self.leaderAddr[0]))
                        time.sleep(5)
                        continue
                
                elif c == "delete":
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect(self.leaderAddr)
                        s.send(queryMessage.encode('utf-8'))
                        finish_info = s.recv(4096).decode("utf-8")
                        print(finish_info)
                        s.close()
                        return
                    except Exception as e:
                        print("Error connecting to leader {}, retry in 5 seconde".format(self.leaderAddr[0]))
                        time.sleep(5)
                        continue
                # If an exact match is not confirmed, this last case will be used if provided
                elif c == "ls":
                    if len(query) != 2:
                        print("Wrong input. The ls should be ls [sdfsname]")
                        return
                    print("sending query message to leader about list info: ", query[1])
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect(self.leaderAddr)
                        s.send(f"{queryMessage}".encode('utf-8'))
                        print("receive from leader")
                        list_machine = s.recv(4096).decode('utf-8')
                        s.close()
                        print("file stored in: ", list_machine)
                        return
                    except Exception as e:
                        print("Error connecting to leader {}, retry in 5 seconde".format(self.leaderAddr[0]))
                        time.sleep(5)
                        continue
                elif c == "store":
                    print("Stored file: ", self.localFile)
                    return
                # Multiread vm1,vm2,vm3 sdfsname localpath
                # unzip the machine number from the cmd and send them the query
                elif c == "Multiread":
                    if len(query) != 4:
                        print("Wrong input. The ls should be Multiread [vm_list] [sdfsname] [local_path]")
                        return
                    _, vm_list, sdfsfilename, local_path = query
                    vm_list = vm_list.split(",")
                    # vm_ip_lst = []
                    current_list = self.failureDetectionServer.returnDetectedKey()
                    current_ord = [int(member.split(".")[0][-2:]) for member in current_list]
                    for vm in vm_list:
                        vm_ip = int(vm[2])
                        if vm_ip not in current_ord:
                        # if vm_ip >= len(self.MembershipList):
                            print("Not in the current membership list") 
                            continue
                        vm_ip = list(self.MembershipList)[current_ord.index(vm_ip)]
                        new_query = "Multiread {} {}".format(sdfsfilename, local_path)
                        try:
                            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as p:
                                p.connect((vm_ip ,PORT))
                                p.send(f"{new_query}".encode('utf-8'))
                                # end_info = p.recv(512).decode("utf-8")
                                # print(end_info)
                        except Exception as e:
                            print(f"Error sending to {vm_ip}: {e}")
                            continue
                        # vm_ip_lst.append(list(self.MembershipList)[vm_ip])
                    # print(vm_ip_lst)
                    return
                # other cmd about failure detection
                elif c == "list_mem":
                    print("MembershipList: ")
                    for i in self.MembershipList:
                        print(i)
                    return
                elif c == "list_self":
                    print(self.ip)
                    return
                elif c == "leave":
                    self.failureDetectionServer.enable_sending = False
                    return
                elif c == "enable_suspicion":
                    self.failureDetectionServer.gossipS = True
                    self.failureDetectionServer.presidencyS += 1 
                    print("Starting gossip S.")
                    return
                elif c == "disable_suspicion":
                    self.failureDetectionServer.gossipS = False
                    self.failureDetectionServer.presidencyS += 1 
                    print("Stoping gossip S.")
                    return
                elif c == "join":
                    self.failureDetectionServer.enable_sending = True 
                    print("Starting to send messages.")
                    self.failureDetectionServer.MembershipList = {
                    f"{ip}:{port}:{self.failureDetectionServer.timejoin}": {
                    "id": f"{ip}:{port}:{self.failureDetectionServer.timejoin}",
                    "addr": (ip, port),
                    "heartbeat": 0,
                    "status": "Alive",
                    "incarnation": 0,
                    "time": time.time(),
                    }
                    for ip, port in [(IP, self.failureDetectionServer.port) for IP in [self.ip, self.failureDetectionServer.introducor]]
                }
                    return
                elif c == "list_suspected":
                    self.failureDetectionServer.printSuspisionNode()
                    return
                # print out some debug element/ important value in the system
                elif c == "print":
                    _, obj = query
                    if obj == "dic":
                        try:
                            s3 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            s3.connect(self.leaderAddr)
                            s3.send(f"{queryMessage}".encode('utf-8'))
                            fileToServer_data = s3.recv(8192).decode('utf-8')
                            fileToServer = json.loads(fileToServer_data)
                            s3.close()
                            print("file on different server: ")
                            # print(serverToFile_data)
                            print(json.dumps(fileToServer, indent= 4))
                            # print(json.dumps(serverToFile, indent=4 ))
                        except Exception as e:
                            print(e)
                            return
                    if obj == "leader":
                        print("Leader addr: ", self.leaderAddr, self.isLeader)
                    if obj == "lock":
                        print("lock: ", self.fileToLock)
                    if obj == "S":
                        print("presidencyS: {}, {}".format(self.failureDetectionServer.gossipS, self.failureDetectionServer.presidencyS))
                    return
                else:
                    print("Wrong input format")
                    return
            else:
                print("Wrong Input Format")
                return 
    def run(self):
        receiver_thread = threading.Thread(target=self.receive)
        receiver_thread.daemon = True
        receiver_thread.start()
        failure_detection_thread = threading.Thread(target=self.failureDetectionServer.run)
        failure_detection_thread.daemon = True 
        failure_detection_thread.start()
        time.sleep(7)
        update_thread = threading.Thread(target=self.updateMembershipList)
        update_thread.daemon = True 
        update_thread.start()

        udp_thread = threading.Thread(target=self.UDP_server)
        udp_thread.daemon = True 
        udp_thread.start()
        time.sleep(1)

        user_thread = threading.Thread(target=self.querry)
        user_thread.daemon = True
        user_thread.start()
        if self.isLeader:
            leader_thread = threading.Thread(target=self.leaderServer)
            leader_thread.daemon = True
            leader_thread.start()
            leader_thread.join()

        udp_thread.join()
        receiver_thread.join()
        update_thread.join()
        user_thread.join()
        failure_detection_thread.join()

if __name__ == "__main__":
    server = Server()
    server.run()
