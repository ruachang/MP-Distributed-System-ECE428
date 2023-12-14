import socket
import sys
import threading
import time
import math

# TODO

# The host name of all of the vms
HOST_NAME_LIST = [
    'fa23-cs425-8001.cs.illinois.edu',
    'fa23-cs425-8002.cs.illinois.edu',
    'fa23-cs425-8003.cs.illinois.edu',
    'fa23-cs425-8004.cs.illinois.edu',
    # 'fa23-cs425-8005.cs.illinois.edu',
    # 'fa23-cs425-8006.cs.illinois.edu',
    # 'fa23-cs425-8007.cs.illinois.edu',
    # 'fa23-cs425-8008.cs.illinois.edu',
    # 'fa23-cs425-8009.cs.illinois.edu',
    # 'fa23-cs425-8010.cs.illinois.edu',
]
PORT = 12345
# Combine every address with the port number
SERVER_ADDRESSES = []
for i in HOST_NAME_LIST:
    SERVER_ADDRESSES.append((i, PORT))

# * total matched lines
totalMatchedLines = 0
# * Dictionary that stores the result of the query
resDic = {} 
# * Time dictionary that stores the time each query
totalTime = {}
# * All the connected machine
connectedMachine = 0
# * Function that sends query to server
def sendQuery2Server(serverAddress, query, opt=""):
    global totalMatchedLines
    global totalTime
    global connectedMachine
    try:
        # Initialize the socket descriptor of client
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect to the server through the given address list
        clientSocket.connect(serverAddress)
        print("Connected to {}".format(serverAddress))

        # Send query message
        queryMessage = "{} {}".format(query, opt)

        # Send the query and option to the server
        clientSocket.send(queryMessage.encode('utf-8'))

        startTime = time.time()
        # Receive the data of number of matched lines from the server
        numMatchedLinesStr = clientSocket.recv(1024).decode('utf-8')
        numMatchedLines = int(numMatchedLinesStr)

        # Send ack to the server for the number of matched lines ('ACK')
        clientSocket.send('ACK'.encode('utf-8'))

        # Receive the number of bytes for the data from the server
        dataSizeStr = clientSocket.recv(1024).decode('utf-8')
        dataSize = int(dataSizeStr)

        # Send ack to the server for the data size ('ACK')
        clientSocket.send('ACK'.encode('utf-8'))

        # Print the results received from server
        receivedData = b""
        # Keep on receive data until the bytes exceeds the dataSize
        while len(receivedData) < dataSize:
            receivedBytes = clientSocket.recv(dataSize)
            if not receivedBytes:
                break
            receivedData += receivedBytes

        result = receivedData.decode('utf-8')
        print(result)

        endTime = time.time()
        # Calculate the query time
        costTime = endTime - startTime

        # Calculate the total matched lines
        totalMatchedLines += numMatchedLines

        # Record the query time
        totalTime[serverAddress] = costTime
        connectedMachine += 1
        # print("Number of matched lines from {}: {}".format(serverAddress, numMatchedLines))
        resDic[serverAddress] = numMatchedLines
        print("Time cost for {}: {:.4f} seconds\n".format(serverAddress, costTime))

    except Exception as error:
        print("Error: {}".format(str(error)))

    finally:
        # Close the client socket
        clientSocket.close()

if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print('[NOTICE]: not enough input parameters')
    else:
        pattern = sys.argv[1]
        if len(sys.argv) == 3:
            opt = sys.argv[2]
        else:
            opt = None
        # Initialize threads for different servers
        tds = []
        startTime = time.time()
        for serverAddress in SERVER_ADDRESSES:
            td = threading.Thread(target=sendQuery2Server, args=(serverAddress, pattern, opt))
            tds.append(td)
        # Start all threads
        for td in tds:
            td.start()
        # Wait for all threads to finish
        for td in tds:
            td.join()
        endTime = time.time()
        
        print("==============================RESULT===============================")
        for serverAddress in totalTime.keys():
            print("Number of matched lines from {}: {}".format(serverAddress, resDic[serverAddress]))
            print("Cost time: {}\n".format(totalTime[serverAddress]))
        print("==============================SUMMARY==============================")
        print("Totoal connected machines: {}".format(connectedMachine))
        
        if connectedMachine == 0:
            print("Doesn't connected to any machine")
        else:
            print("Total matching lines from all servers: {}".format(totalMatchedLines))
            print("The time cost for query of the vms are {}".format(endTime - startTime))
            # totalTimes = 0
            # for i in totalTime:
            #     totalTimes += i
            # avgTime = totalTimes / connectedMachine
            # std = 0
            # for i in totalTime:
            #     std += (i - avgTime) * (i - avgTime) / connectedMachine
            # print("Total time taken for all connections: {:.4f} seconds".format(totalTimes))
            # print("Average time taken for all connections: {:.4f} seconds".format(avgTime))
            # print("Standard deviation between all connections: {:.8f}".format(math.sqrt(std)))
        
