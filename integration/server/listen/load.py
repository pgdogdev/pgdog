import socket
import sys
import time

connections = int(sys.argv[1])
duration = float(sys.argv[2])
ports = [int(port) for port in sys.argv[3:]]

sockets = []
failed = 0
for _ in range(connections):
    for port in ports:
        try:
            sockets.append(socket.create_connection(("127.0.0.1", port), timeout=2))
        except OSError:
            failed += 1

print(f"load: {len(sockets)} open, {failed} failed, ports {ports}", flush=True)
time.sleep(duration)

for sock in sockets:
    sock.close()
