import socket
import json

def send_write(node, key, value):
    message = {"key": key, "value": value}
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((node, 5000))
        s.send(json.dumps(message).encode())

# Simulate writes across nodes
send_write("node1", "x", 5)
send_write("node2", "x", 10)  # Should be delayed if causal dependency is unmet