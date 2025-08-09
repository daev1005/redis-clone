import socket  # noqa: F401
import threading
import time

## Parses the command from the client input.
def parse_command(data: bytes):
    input = data.decode()
    lines = input.split("\r\n")
    if not lines[0].startswith("*"):
        raise ValueError("Invalid command format")
    num_elements = int(lines[0][1:])
    elements = []
    index = 1
    for _ in range(num_elements):
        if not lines[index].startswith("$"):
            raise ValueError("Invalid element format")
        lengthOfElement = int(lines[index][1:])
        ##Index of the actual element
        index += 1
        element = lines[index]
        if (lengthOfElement != len(element)):
            raise ValueError(f"Element length mismatch. Expected {lengthOfElement}, got {len(element)}")
        elements.append(element)

        ##Move to the next element
        index += 1
    return elements

blocked_clients = {}    
lists = {}  
##Takes in multiple clients and handles them concurrently
def handle_client(client: socket.socket):
    #Stores key-value pairs
    store = {}
    expiration_time = {}
    
    while True:
        #1024 is the bytesize of the input buffer (isn't fixed)
        input = client.recv(1024)
        elements = parse_command(input)
        if "ping" in elements[0].lower():
            # Respond with PONG
            client.sendall(b"+PONG\r\n")

        elif "echo" in elements[0].lower():
            message = ""
            # Respond with the expected message
            for i in range(1, len(elements)):
                msg = elements[i]
                message += f"${len(msg)}\r\n{msg}\r\n"
            client.sendall(message.encode())
        elif "set" in elements[0].lower():
            # Store the key-value pair in the store
            store[elements[1]] = elements[2]
            if len(elements) > 3:
                if elements[3].lower() == "px":
                    expiration_time[elements[1]] = time.time() + (int(elements[4]) / 1000.0)
            # Respond with OK
            client.sendall(b"+OK\r\n")

        elif "get" in elements[0].lower():
            # Retrieve the value for the given key
            # If the key does not exist, respond with $-1
            if elements[1] not in store:
                client.sendall(b"$-1\r\n")
            # If the key exists and has not expired, respond with the value
            if elements[1] in expiration_time and elements[1] in store:   
                if time.time() < expiration_time[elements[1]]:
                    msg = store[elements[1]]
                    message = f"${len(msg)}\r\n{msg}\r\n"
                    client.sendall(message.encode())
                else:
                    # If the key has expired, respond with $-1
                    del store[elements[1]]
                    del expiration_time[elements[1]]
                    client.sendall(b"$-1\r\n")
            else:
                msg = store[elements[1]]
                message = f"${len(msg)}\r\n{msg}\r\n"
                client.sendall(message.encode()) 








        elif "rpush" in elements[0].lower():
            # This list contains a key and a value of a list
            list = []
            #Adds all elements after the list name to the list
            for i in range(2, len(elements)):
                list.append(elements[i])
            if elements[1] in lists:
                lists[elements[1]].extend(list)
            else:
                lists[elements[1]] = list
            size = len(lists[elements[1]])
            client.sendall(f":{size}\r\n".encode())

            
            # if elements[1] in blocked_clients:
            #     list = blocked_clients[elements[1]]
            #     oldest_client = list.pop(0)
            #     item = lists[elements[1]].pop(0)
            #     message = f"*2\r\n${len(elements[1])}\r\n{elements[1]}\r\n${len(item)}\r\n{item}\r\n"
            #     oldest_client.sendall(message.encode())
            #     if len(list) == 0:
            #         del blocked_clients[elements[1]]

            if elements[1] in blocked_clients and blocked_clients[elements[1]]:
                blocked_client, event = blocked_clients[elements[1]].pop(0)
                item = lists[elements[1]].pop(0)
                message = f"*2\r\n${len(elements[1])}\r\n{elements[1]}\r\n${len(item)}\r\n{item}\r\n"
                blocked_client.sendall(message.encode())
                event.set()  # Wake up the waiting handler thread
                
                if not blocked_clients[elements[1]]:
                    del blocked_clients[elements[1]]





                
        elif "lrange" in elements[0].lower():
            list = lists.get(elements[1]) # Get the list for the given key
            first_index = int(elements[2])
            last_index = int(elements[3])
            message = ""
            #Checks if the list exists or if it's empty
            if elements[1] not in lists or len(list) == 0:
                client.sendall(b"*0\r\n")
                return
            
            # Handles negative indices. If indices exceed the size of the list, it sets them to 0
            if first_index < 0:
                new_first = len(list) + first_index
                if new_first < 0:
                    new_first = 0
                first_index = new_first
            if last_index < 0:
                new_last = len(list) + last_index
                if new_last < 0:
                    new_last = 0
                last_index = new_last

            message += f"*{len(list[first_index:last_index + 1])}\r\n"    
            for item in list[first_index:last_index + 1]:
                message += f"${len(item)}\r\n{item}\r\n"
            client.sendall(message.encode())    
        elif "lpush" in elements[0].lower():
            list = []
            #Adds all elements after the list name to the list
            for i in range(len(elements)-1, 1, -1):
                list.append(elements[i])
            if elements[1] in lists:
                list = list + lists[elements[1]]
            lists[elements[1]] = list
            client.sendall(f":{len(lists[elements[1]])}\r\n".encode())
        elif "llen" in elements[0].lower():
            if elements[1] not in lists:
                client.sendall(b":0\r\n")
            else:
                size = len(lists[elements[1]])
                client.sendall(f":{size}\r\n".encode())
        elif "lpop" in elements[0].lower():
            list_name = elements[1]
            # If the list does not exist or is empty, respond with $-1
            if list_name not in lists or len(lists[list_name]) == 0:
                client.sendall(b"$-1\r\n")
            else:
                # Removes and returns the first element of the list
                if len(elements) > 2:
                    message = ""
                    message += f"*{elements[2]}\r\n"
                    for _ in range(int(elements[2])):
                        item = lists[list_name].pop(0)
                        message += f"${len(item)}\r\n{item}\r\n"
                    client.sendall(message.encode())
                else:
                    item = lists[elements[1]].pop(0)
                    client.sendall(f"${len(item)}\r\n{item}\r\n".encode())









        elif "blpop" in elements[0].lower():
            list_name = elements[1]
            timeout = int(elements[2])
            event = threading.Event()
            # if list_name in lists and lists[list_name]:
            #     item = lists[list_name].pop(0)
            #     message = f"*2\r\n${len(list_name)}\r\n{list_name}\r\n${len(item)}\r\n{item}\r\n"
            #     client.sendall(message.encode())
            # else:
            #     blocked_clients[list_name] = []
            #     blocked_clients[list_name].append(client)

    

            if list_name in lists and lists[list_name]:
                item = lists[list_name].pop(0)
                message = f"*2\r\n${len(list_name)}\r\n{list_name}\r\n${len(item)}\r\n{item}\r\n"
                client.sendall(message.encode())
            else:
                if list_name not in blocked_clients:
                    blocked_clients[list_name] = []
                blocked_clients[list_name].append((client, event))
                
                # Block the current handler/thread until event is set or timeout expires
                event.wait(timeout if timeout > 0 else None)
                
                # After wake up, check if item is available to send or timeout occurred
                if list_name in lists and lists[list_name]:
                    item = lists[list_name].pop(0)
                    message = f"*2\r\n${len(list_name)}\r\n{list_name}\r\n${len(item)}\r\n{item}\r\n"
                    client.sendall(message.encode())
                else:
                    # Timeout happened, send nil response
                    client.sendall(b"$-1\r\n")
                



            

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection,_ = server_socket.accept() # wait for client
        threading.Thread(target=handle_client, args=(connection,)).start()

if __name__ == "__main__":
    main()

