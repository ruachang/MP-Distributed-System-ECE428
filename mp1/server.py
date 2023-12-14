import socket
import subprocess
import argparse
import threading
# HOST and PORT config
HOST = socket.gethostname()
PORT = 12345
# Server sockeet
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serverSocket.bind((HOST, PORT))
serverSocket.listen(1)
print(f"Server listens on {HOST}:{PORT}")
""" 
Functions for sending data to the client
"""
def procesClientQuery(clientSocket):
    try:
        # First receive the query from the client
        command = clientSocket.recv(4096).decode('utf-8')
        # devide it into the content of query and the options
        query, options = command.split(' ')
        if options == "None":
            options = ""
        logFile = "*.log"
        grepCommand = f"grep {options} '{query}' {logFile}"  # Replace with your log file path
        print(grepCommand) 
        # get grep result using the feature of subprocess to directly
        # apply grep in Python
        grepResult = subprocess.getoutput(grepCommand)
        # Count the number of line of the result
        if len(grepResult) == 0:
            numMatchingLines = 0
            resultMatchingLines = ""
        else:
            resultMatchingLines = grepResult.split('\n')
            numMatchingLines = len(resultMatchingLines)
        print(numMatchingLines)
        
        # Add HOSTNAME to the response for better comprehenssion
        HOSTNAME = socket.gethostname()

        response = ""
        for line in resultMatchingLines:
            response += f"{HOSTNAME}:{logFile}:{line}" + '\n'
        # Begin sending the data to the client, first send the number of matching line
        clientSocket.send(str(numMatchingLines).encode('utf-8'))
        ack = clientSocket.recv(1024).decode('utf-8')

        if ack == 'ACK':
            dataSize = len(response.encode('utf-8'))
            clientSocket.send(str(dataSize).encode('utf-8'))
            ack = clientSocket.recv(1024).decode('utf-8')

            if ack == 'ACK':
            # If recive acknowledghment from the client sent the data
                clientSocket.send(response.encode('utf-8'))
    except Exception as e:
        print("Error processing query: {}".format(str(e)))

    finally:
        clientSocket.close()

while True:
    clientSocket, clientAddress = serverSocket.accept()
    print("Accepted connection from {}".format(clientAddress))
    # Handle clients in separate thread for paralel
    clientThreads = threading.Thread(target=procesClientQuery, args=(clientSocket,))
    clientThreads.start()
