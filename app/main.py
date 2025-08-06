import socket  # noqa: F401
import threading

## Parses the command from the client input. INCOMPLETE
def parse_command(data: bytes):
    input = data.decode()
    lines = input.split("\r\n")
    if not lines[0].startswith("*"):
        raise ValueError("Invalid command format")
    num_elements = int(lines[0][1:])
    elements = []
    index = 1
    for _ in range(num_elements):
        if not lines[0].startswith("$"):
            raise ValueError("Invalid element format")
        lengthOfElement = int(lines[index][1:])
        index += 1
        element = lines[index]
        if (lengthOfElement != len(element)):
            raise ValueError("Element length mismatch. Expected {lengthOfElement}, got {len(element)}")
        elements.append(element)
        return elements
        
##Takes in multiple clients and handles them concurrently
def handle_client(client: socket.socket):
    while True:
        #1024 is the bytesize of the input buffer
        input = client.recv(1024)
        elements = parse_command(input)
        if "ping" in elements[0].lower():
            # Respond with PONG
            client.sendall(b"+PONG\r\n")

        ## does not work. Need to finish parser first
        elif "echo" in elements[0].lower():
            message = ""
            for i in range(1, len(elements)):
                msg = elements[i]
                message += message + "${len(msg)}\r\n{msg}\r\n"
            client.sendall(f"{message}")


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection,_ = server_socket.accept() # wait for client
        threading.Thread(target=handle_client, args=(connection,)).start()

if __name__ == "__main__":
    main()
