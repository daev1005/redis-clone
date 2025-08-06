import socket  # noqa: F401
import threading

##Takes in multiple clients and handles them concurrently
def handle_client(client: socket.socket):
    while True:
        #1024 is the bytesize of the input buffer
        input = client.recv(1024)
        cmd = input.decode().strip().split()
        if "ping" in cmd[0].lower():
            # Respond with PONG
            client.sendall(b"+PONG\r\n")
        elif "echo" in cmd[0].lower():
            message = " ".join(cmd[1:])
            response = f"${len(message)}\r\n{message}\r\n"
            client.sendall(response.encode())


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection,_ = server_socket.accept() # wait for client
        threading.Thread(target=handle_client, args=(connection,)).start()

if __name__ == "__main__":
    main()
