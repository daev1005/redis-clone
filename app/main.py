import socket  # noqa: F401
import threading

##Takes in multiple clients and handles them concurrently
def handle_client(client: socket.socket):
    while True:
        input = client.recv(1024)
        if "ping" in input.decode().strip().lower():
            # Respond with PONG
            client.sendall(b"+PONG\r\n")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection,_ = server_socket.accept() # wait for client
        threading.Thread(target=handle_client, args=(connection,)).start()

if __name__ == "__main__":
    main()
