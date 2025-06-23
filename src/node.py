import json
import socket
from threading import Thread, Lock

class Node:
    def __init__(self, node_id, peers):
        self.node_id = node_id
        self.peers = peers
        self.data_store = {}
        self.vector_clock = {peer: 0 for peer in peers + [node_id]}
        self.buffer = []
        self.lock = Lock()

    def increment_clock(self):
        self.vector_clock[self.node_id] += 1

    def send_message(self, target, message):
        self.increment_clock()
        message['vector_clock'] = self.vector_clock.copy()
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target, 5000))
            s.send(json.dumps(message).encode())

    def receive_message(self, conn):
        data = json.loads(conn.recv(1024).decode())
        self.lock.acquire()
        if self.can_process(data['vector_clock']):
            self.apply_write(data)
        else:
            self.buffer.append(data)
        self.lock.release()

    def apply_write(self, data):
        self.vector_clock = {k: max(self.vector_clock[k], data['vector_clock'][k]) for k in self.vector_clock}
        self.data_store[data['key']] = data['value']
        self.process_buffer()

    def process_buffer(self):
        for msg in self.buffer:
            if self.can_process(msg['vector_clock']):
                self.apply_write(msg)
                self.buffer.remove(msg)

    def can_process(self, incoming_clock):
        return all(incoming_clock[k] <= self.vector_clock[k] + (1 if k == self.node_id else 0) for k in self.vector_clock)

def start_server(node):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', 5000))
    server.listen(5)
    while True:
        conn, addr = server.accept()
        Thread(target=node.receive_message, args=(conn,)).start()