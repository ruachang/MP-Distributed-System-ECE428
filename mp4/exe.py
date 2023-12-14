import socket
import time
QUERRY_PORT = 5006 # querry server port

with open("mapleJuice.sh", 'r') as f:
    for line in f:
        line = line.strip()
        print(line)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((socket.gethostname(), QUERRY_PORT))
        s.send(line.encode('utf-8'))
        s.close()
        time.sleep(2)