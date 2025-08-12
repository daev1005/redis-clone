import socket  # noqa: F401
import threading
import time
from collections import defaultdict

blocked_clients = defaultdict(list)
blocked_streams = defaultdict(list)
lists = {}  

#Stores key-value pairs
store = {}
expiration_time = {}


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

##Takes in multiple clients and handles them concurrently
def handle_client(client: socket.socket):
    while True:
        #1024 is the bytesize of the input buffer (isn't fixed)
        input = client.recv(1024)
        elements = parse_command(input)
        cmd = elements[0].lower()
        if "ping" == cmd:
            # Respond with PONG
            client.sendall(b"+PONG\r\n")

        elif "echo" == cmd:
            message = ""
            # Respond with the expected message
            for i in range(1, len(elements)):
                msg = elements[i]
                message += f"${len(msg)}\r\n{msg}\r\n"
            client.sendall(message.encode())
        elif "set" == cmd:
            # Store the key-value pair in the store
            store[elements[1]] = elements[2]
            if len(elements) > 3:
                if elements[3].lower() == "px":
                    expiration_time[elements[1]] = time.time() + (int(elements[4]) / 1000.0)
            # Respond with OK
            client.sendall(b"+OK\r\n")

        elif "get" == cmd:
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
        elif "rpush" == cmd:
            # This list contains a key and a value of a list
            values = []
            #Adds all elements after the list name to the list
            for i in range(2, len(elements)):
                values.append(elements[i])
            if elements[1] in lists:
                lists[elements[1]].extend(values)
            else:
                lists[elements[1]] = values
            size = len(lists[elements[1]])
            client.sendall(f":{size}\r\n".encode())
            
            # If there are any blocked clients for this list, unblock the oldest one and send them the first item
            if blocked_clients[elements[1]]:
                oldest_client, event = blocked_clients[elements[1]].pop(0)
                if lists[elements[1]]:
                    item = lists[elements[1]].pop(0)
                    message = f"*2\r\n${len(elements[1])}\r\n{elements[1]}\r\n${len(item)}\r\n{item}\r\n"
                    oldest_client.sendall(message.encode())
                    event.set()
                    if not blocked_clients[elements[1]]:
                        del blocked_clients[elements[1]]   
        elif "lrange" == cmd:
            values = lists.get(elements[1]) # Get the list for the given key
            first_index = int(elements[2])
            last_index = int(elements[3])
            message = ""
            #Checks if the list exists or if it's empty
            if elements[1] not in lists or len(values) == 0:
                client.sendall(b"*0\r\n")
                return
            
            # Handles negative indices. If indices exceed the size of the list, it sets them to 0
            if first_index < 0:
                new_first = len(values) + first_index
                if new_first < 0:
                    new_first = 0
                first_index = new_first
            if last_index < 0:
                new_last = len(values) + last_index
                if new_last < 0:
                    new_last = 0
                last_index = new_last

            message += f"*{len(values[first_index:last_index + 1])}\r\n"    
            for item in values[first_index:last_index + 1]:
                message += f"${len(item)}\r\n{item}\r\n"
            client.sendall(message.encode())    
        elif "lpush" == cmd:
            values = []
            #Adds all elements after the list name to the list
            for i in range(len(elements)-1, 1, -1):
                values.append(elements[i])
            if elements[1] in lists:
                values = values + lists[elements[1]]
            lists[elements[1]] = values
            client.sendall(f":{len(lists[elements[1]])}\r\n".encode())
        elif "llen" == cmd:
            if elements[1] not in lists:
                client.sendall(b":0\r\n")
            else:
                size = len(lists[elements[1]])
                client.sendall(f":{size}\r\n".encode())
        elif "lpop" == cmd:
            list_name = elements[1]
            # If the list does not exist or is empty, respond with $-1
            if list_name not in lists or len(lists[list_name]) == 0:
                client.sendall(b"-1\r\n")
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
        elif "blpop"== cmd:
            list_name = elements[1]
            timeout = float(elements[2])
            event = threading.Event()
            if list_name in lists and lists[list_name]:
                item = lists[list_name].pop(0)
                message = f"*2\r\n${len(list_name)}\r\n{list_name}\r\n${len(item)}\r\n{item}\r\n"
                client.sendall(message.encode())
            else:
                if list_name not in blocked_clients:
                    blocked_clients[list_name] = []
                blocked_clients[list_name].append((client, event))
                if not event.wait(timeout if timeout > 0 else None):
                    # Timeout expired without push event
                    client.sendall(b"$-1\r\n")
        elif "type" == cmd:
            if elements[1] in store:
                value = store[elements[1]]
                if isinstance(value, list) and value and isinstance(value[0], tuple):
                    client.sendall(b"+stream\r\n")
                else:
                    client.sendall(f"+string\r\n".encode())
            else:
                client.sendall(b"+none\r\n")
        elif "xadd" == cmd:
            stream_name = elements[1]
            entry_id = elements[2]
            entries = elements[3:]
            if entry_id == "*":
                entry_id = f"{int(time.time() * 1000)}-*"
            new_ms, new_seq = entry_id.split("-")
            
            # When the stream does not exist, create it and add the entry
            if stream_name not in store:
                store[stream_name] = []

                # Handle special cases for the entry ID
                if new_ms == "0" and new_seq == "*":
                    entry_id = f"{new_ms}-1"
                elif new_seq == "*":
                    entry_id = f"{new_ms}-0"
            else: 
                if store[stream_name]:
                    last_id = store[stream_name][-1][0]
                    last_ms, last_seq = map(int, last_id.split("-"))
                    new_ms = int(new_ms)

                    # When the ms is the same but different sequence number, increment the sequence number
                    if last_ms == new_ms and new_seq == "*":
                        new_seq = last_seq + 1
                        entry_id = f"{new_ms}-{new_seq}"
                    # When ms is different but sequence number is "*", set sequence number to 0
                    elif new_seq == "*":
                        new_seq = 0
                        entry_id = f"{new_ms}-{new_seq}"
                    else:
                        new_seq = int(new_seq)


                    if new_ms == 0 and new_seq == 0:
                        client.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                        continue
                    elif (new_ms < last_ms) or (new_ms == last_ms and new_seq <= last_seq):
                        client.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                        continue
            store[stream_name].append((entry_id, entries))
            client.sendall(f"${len(entry_id)}\r\n{entry_id}\r\n".encode())


            if blocked_streams[stream_name]:
                event, start_id, blocked_client = blocked_streams[stream_name].pop(0)
                unblock_stream(stream_name, start_id, entry_id, entries, blocked_client)
                if not blocked_streams[stream_name]:
                    del blocked_streams[stream_name]


        elif "xrange" == cmd:
            stream_name = elements[1]
            start_id = elements[2]
            end_id = elements[3]
            if start_id == "-":
                start_id = "0-0"
            if len(start_id.split("-")) != 2:
                start_id = f"{start_id}-0"
            if end_id == "+":
                # Use the maximum possible values for ms and seq
                end_id = "18446744073709551615-18446744073709551615"
            elif len(end_id.split("-")) != 2:
                # default seq = max possible
                end_id = f"{end_id}-18446744073709551615"
            end_ms, end_seq = map(int, end_id.split("-"))
            start_ms, start_seq = map(int, start_id.split("-"))
            count = 0
            message = ""
            for current_id, current_entries in store[stream_name]:
                current_ms, current_seq = map(int, current_id.split("-"))
                if (current_ms, current_seq) >= (start_ms, start_seq) and (current_ms, current_seq) <= (end_ms, end_seq):
                    inner = get_entries(current_entries)
                    count += 1
                    message += f"*2\r\n${len(current_id)}\r\n{current_id}\r\n{inner}"
            final = f"*{count}\r\n{message}"
            client.sendall(final.encode())     
        elif "xread" == cmd:
            streams_start = 2
            blocked = False
            if elements[1].lower() == "block":
                # Streams keyword expected at elements[3]
                if elements[3].lower() != "streams":
                    client.sendall(b"-ERR syntax error\r\n")
                # The streams start at index 4
                streams_start = 4
                blocked = True
            else:
                if elements[1].lower() != "streams":
                    client.sendall(b"-ERR syntax error\r\n")


            num_streams = (len(elements) - streams_start) // 2
            stream_names = elements[streams_start : streams_start + num_streams]
            entry_ids = elements[streams_start + num_streams : ]
            key_to_value = dict(zip(stream_names, entry_ids))
                


            if blocked:
                timeout = int(elements[2]) / 1000
                event = threading.Event()
                blocked_streams[stream_names[0]].append((event, entry_ids[0], client))
                if not event.wait(timeout if timeout > 0 else None):
                    client.sendall(b"$-1\r\n")


            else:
                final = f"*{len(key_to_value)}\r\n"
                for stream_name in key_to_value:
                    entry_id = key_to_value[stream_name]
                    start_ms, start_seq = map(int, entry_id.split("-"))
                
                    message = ""
                    for current_id, current_entries in store[stream_name] :
                        current_ms, current_seq = map(int, current_id.split("-"))
                        if (current_ms, current_seq) > (start_ms, start_seq):
                            inner = get_entries(current_entries)
                            
                            message += f"*2\r\n${len(current_id)}\r\n{current_id}\r\n{inner}"
                    final += f"*2\r\n${len(stream_name)}\r\n{stream_name}\r\n*1\r\n{message}"
                client.sendall(final.encode())
    
def get_entries(current_entries: list):
    if current_entries:
        inner = f"*{len(current_entries)}\r\n"
        for entry in current_entries:
            inner += f"${len(entry)}\r\n{entry}\r\n"
        return inner
    else:
        return "*0\r\n"

def unblock_stream(stream_name, start_id, current_id, current_entries, client):
    if start_id == "$":
        start_id = "0-0"
    start_ms, start_seq = map(int, start_id.split("-"))
    current_ms, current_seq = map(int, current_id.split("-"))
    if (current_ms, current_seq) > (start_ms, start_seq):
        entries = get_entries(current_entries)
        id_and_entries = f"*2\r\n${len(current_id)}\r\n{current_id}\r\n{entries}"
        final = f"*1\r\n*2\r\n${len(stream_name)}\r\n{stream_name}\r\n*1\r\n{id_and_entries}"
        client.sendall(final.encode())
        



                        
            
                
                    
                    
                    
                

        
                        



            

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection,_ = server_socket.accept() # wait for client
        thread = threading.Thread(target=handle_client, args=(connection,))
        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    main()

