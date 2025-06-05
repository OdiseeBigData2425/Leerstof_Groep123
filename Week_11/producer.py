import socket
import time
import json
import random
from datetime import datetime
 
BOOK_PATH = 'boek.txt'
HOST = 'localhost'
PORT = 19999
 
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    print(f"Producer listening on {HOST}:{PORT}")
 
    conn, addr = s.accept()
    with conn:
        print(f"Connection from {addr}")
        with open(BOOK_PATH, 'r', encoding='utf-8') as book:
            for line in book:
                line = line.strip()
                if not line:
                    continue
                obj = {
                    'timestamp': datetime.now().isoformat(),
                    'line': line
                }
                message = json.dumps(obj) + '\n'
                conn.sendall(message.encode('utf-8'))
                time.sleep(random.uniform(0.5, 1.5))