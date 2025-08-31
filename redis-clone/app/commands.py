import socket
import threading
import time
from app.utils import make_resp, make_resp_command, get_entries, unblock_stream
from app.state import store, lists, list_locks, blocked_clients, expiration_time, server_status, rdb_configs, blocked_streams, sorted_sets, client_subscribed


# Returns "pong" when pinged
def ping_cmd(client: socket.socket, elements: list):
    #client.sendall(b"+PONG\r\n")
    return f"+PONG\r\n"

# Returns "pong" when pinged when under subscribed mode
def sping_cmd(client: socket.socket, elements: list):
    return make_resp("pong", "")

# accepts a single argument and returns it back as a RESP bulk string.
def echo_cmd(client: socket.socket, elements: list):
    message = ""
    # Respond with the expected message
    for i in range(1, len(elements)):
        msg = elements[i]
        message += f"${len(msg)}\r\n{msg}\r\n"
    #client.sendall(message.encode())  
    return message  

# used to set a key to a value
def set_cmd(client: socket.socket, elements: list):
# Store the key-value pair in the store
    store[elements[1]] = elements[2]
    if len(elements) > 3:
        if elements[3].lower() == "px":
            expiration_time[elements[1]] = time.time() + (int(elements[4]) / 1000.0)
    # Respond with OK
    #client.sendall(b"+OK\r\n")
    return f"+OK\r\n"  

# used to get the value of a key
def get_cmd(client: socket.socket, elements: list):
    # Retrieve the value for the given key
    # If the key does not exist, respond with $-1
    if elements[1] not in store:
        #client.sendall(b"$-1\r\n")
        return f"$-1\r\n"
    # If the key exists and has not expired, respond with the value
    if elements[1] in expiration_time and elements[1] in store:   
        if time.time() < expiration_time[elements[1]]:
            msg = store[elements[1]]
            message = f"${len(msg)}\r\n{msg}\r\n"
            #client.sendall(message.encode())
            return message
        else:
            # If the key has expired, respond with $-1
            del store[elements[1]]
            del expiration_time[elements[1]]
            #client.sendall(b"$-1\r\n")
            return f"$-1\r\n"
    else:
        msg = store[elements[1]]
        message = f"${len(msg)}\r\n{msg}\r\n"
        #client.sendall(message.encode()) 
        return message

# used to append elements to a list. If the list doesn't exist, it is created first.
def rpush_cmd(client: socket.socket, elements: list):
    # This list contains a key and a value of a list
    values = elements[2:]
    list_name = elements[1]

    with list_locks[list_name]:
        if list_name in lists:
            lists[elements[1]].extend(values)
        else:
            lists[elements[1]] = values
        size = len(lists[list_name])
        #client.sendall(f":{size}\r\n".encode())
    
        # If there are any blocked clients for this list, unblock the oldest one and send them the first item
        if blocked_clients[list_name]:
            oldest_client, event = blocked_clients[elements[1]].pop(0)
            if lists[elements[1]]:
                item = lists[elements[1]].pop(0)
                message = f"*2\r\n${len(elements[1])}\r\n{elements[1]}\r\n${len(item)}\r\n{item}\r\n"
                oldest_client.sendall(message.encode())
                if not blocked_clients[elements[1]]:
                    del blocked_clients[elements[1]]
            event.set()
        return f":{size}\r\n"
    
# used to list the elements in a list given a start index and end index. The index of the first element is 0. The end index is inclusive.    
def lrange_cmd(client: socket.socket, elements: list):
    values = lists.get(elements[1]) # Get the list for the given key
    first_index = int(elements[2])
    last_index = int(elements[3])
    message = ""
    #Checks if the list exists or if it's empty
    if elements[1] not in lists or len(values) == 0:
        #client.sendall(b"*0\r\n")
        return f"*0\r\n"
    
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
    #client.sendall(message.encode()) 
    return message

# inserts elements from the left rather than right. If a list doesn't exist, it is created first before prepending elements.
def lpush_cmd(client: socket.socket, elements: list):
    values = []
    # Adds all elements after the list name to the list
    for i in range(len(elements)-1, 1, -1):
        values.append(elements[i])
    if elements[1] in lists:
        values = values + lists[elements[1]]
    lists[elements[1]] = values
    #client.sendall(f":{len(lists[elements[1]])}\r\n".encode())
    return f":{len(lists[elements[1]])}\r\n"

# query a list's length
def llen_cmd(client: socket.socket, elements: list):
    if elements[1] not in lists:
        #client.sendall(b":0\r\n")
        return f":0\r\n"
    else:
        size = len(lists[elements[1]])
        #client.sendall(f":{size}\r\n".encode())
        return f":{size}\r\n"

# removes and returns the first element of the list. If the list is empty or doesn't exist, it returns a null bulk string ($-1\r\n).
def lpop_cmd(client: socket.socket, elements: list):
    list_name = elements[1]
    # If the list does not exist or is empty, respond with $-1
    if list_name not in lists or len(lists[list_name]) == 0:
        #client.sendall(b"-1\r\n")
        return f"$-1\r\n"
    else:
        # Removes and returns the first element of the list
        if len(elements) > 2:
            message = ""
            message += f"*{elements[2]}\r\n"
            for _ in range(int(elements[2])):
                item = lists[list_name].pop(0)
                message += f"${len(item)}\r\n{item}\r\n"
            #client.sendall(message.encode())
            return message
        else:
            item = lists[elements[1]].pop(0)
            #client.sendall(f"${len(item)}\r\n{item}\r\n".encode())
            return f"${len(item)}\r\n{item}\r\n"

# allows clients to wait for an element to become available on one or more lists. If an element is available, it is removed and returned to the client. 
# If no element is available, the client blocks until an element is pushed to one of the lists or until a specified timeout is reached.
def blpop_cmd(client: socket.socket, elements: list):
    list_name = elements[1]
    timeout = float(elements[2])
    event = threading.Event()
    with list_locks[list_name]:
        if list_name in lists and lists[list_name]:
            item = lists[list_name].pop(0)
            message = f"*2\r\n${len(list_name)}\r\n{list_name}\r\n${len(item)}\r\n{item}\r\n"
            #client.sendall(message.encode())
            return message
        else:
            if list_name not in blocked_clients:
                blocked_clients[list_name] = []
            blocked_clients[list_name].append((client, event))
    if not event.wait(timeout if timeout > 0 else None):
        # Timeout expired without push event
        #client.sendall(b"$-1\r\n")
        return f"$-1\r\n"
    
# returns the type of value stored at a given key. 
# It returns one of the following types: string, list, set, zset, hash, and stream.
def type_cmd(client: socket.socket, elements: list):
    if elements[1] in store:
        value = store[elements[1]]
        if isinstance(value, list) and value and isinstance(value[0], tuple):
            #client.sendall(b"+stream\r\n")
            return f"+stream\r\n"
        else:
            #client.sendall(f"+string\r\n".encode())
            return f"+string\r\n"
    else:
        client.sendall(b"+none\r\n")
        return f"+none\r\n"

#appends an entry to a stream. If a stream doesn't exist already, it is created.
def xadd_cmd(client: socket.socket, elements: list):
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
                #client.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
                return f"-ERR The ID specified in XADD must be greater than 0-0\r\n"
            elif (new_ms < last_ms) or (new_ms == last_ms and new_seq <= last_seq):
                #client.sendall(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                return f"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    store[stream_name].append((entry_id, entries))
    #client.sendall(f"${len(entry_id)}\r\n{entry_id}\r\n".encode())
    

    if blocked_streams[stream_name]:
        event, start_id, blocked_client = blocked_streams[stream_name].pop(0)
        unblock_stream(stream_name, start_id, entry_id, entries, blocked_client)
        event.set()
        if not blocked_streams[stream_name]:
            del blocked_streams[stream_name]
    return f"${len(entry_id)}\r\n{entry_id}\r\n"

# retrieves a range of entries from a stream
def xrange_cmd(client: socket.socket, elements: list):
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
    #client.sendall(final.encode())   
    return final  

# used to read data from one or more streams, starting from a specified entry ID.
def xread_cmd(client: socket.socket, elements: list):
    streams_start = 2
    blocked = False
    if elements[1].lower() == "block":
        # Streams keyword expected at elements[3]
        if elements[3].lower() != "streams":
            #client.sendall(b"-ERR syntax error\r\n")
            return f"-ERR syntax error\r\n"
        # The streams start at index 4
        streams_start = 4
        blocked = True
    else:
        if elements[1].lower() != "streams":
            #client.sendall(b"-ERR syntax error\r\n")
            return f"-ERR syntax error\r\n"

    num_streams = (len(elements) - streams_start) // 2
    stream_names = elements[streams_start : streams_start + num_streams]
    entry_ids = elements[streams_start + num_streams : ]
    key_to_value = dict(zip(stream_names, entry_ids))

    if blocked:
        timeout = int(elements[2]) / 1000
        event = threading.Event()
        blocked_streams[stream_names[0]].append((event, entry_ids[0], client))
        if not event.wait(timeout if timeout > 0 else None):
            #client.sendall(b"$-1\r\n")
            return f"$-1\r\n"
        else:
            return None
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
        #client.sendall(final.encode())
        return final

# used to increment the value of a key by 1.
def incr_cmd(client: socket.socket, elements: list):
    key = elements[1]
    if key in store:
        try:
            value = int(store[key])
        except (ValueError, TypeError):
            #client.sendall(b"-ERR value is not an integer or out of range\r\n")
            return f"-ERR value is not an integer or out of range\r\n"
        value += 1
        store[key] = str(value)
        #client.sendall(f":{store[key]}\r\n".encode())
        return f":{store[key]}\r\n"
    else:
        store[key] = "1"
        #client.sendall(f":1\r\n".encode())
        return f":1\r\n"

# returns information and statistics about a Redis server
def info_cmd(client: socket.socket, elements: list):
    type = elements[1].lower()
    if type == "replication":
        role = server_status["server_role"]
        repl_id = server_status["repl_id"]
        repl_offset = str(server_status["repl_offset"])
        response = f"role:{role}\r\nmaster_replid:{repl_id}\r\nmaster_repl_offset:{repl_offset}\r\n"
        return (
            f"${len(response)}\r\n{response}\r\n"
        )

def replconf_cmd(client: socket.socket, elements: list):
    if len(elements) > 1 and elements[1].lower() == "getack":
        offset = server_status["repl_offset"]  # replica’s current offset
        client.sendall(make_resp_command("REPLCONF", "ACK", str(offset)))
    elif len(elements) > 1 and elements[1].lower() == "ack":
        offset = int(elements[2])
        server_status["replica_offsets"][client] = offset
    else:
        print(f"[DEBUG] Non-GETACK REPLCONF command")
        return f"+OK\r\n"

def psync_cmd(client: socket.socket, elements: list):
    repl_id = elements[1]
    repl_offset = elements[2]
    if repl_id == "?" and repl_offset == "-1":
        client.sendall(f"+FULLRESYNC {server_status["repl_id"]} 0\r\n".encode())
        empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        client.sendall(b"$" + str(len(bytes.fromhex(empty_rdb_hex))).encode() + b"\r\n" + bytes.fromhex(empty_rdb_hex))
        server_status["replicas"].append(client)
        server_status["replica_offsets"][client] = 0
    return None

def wait_cmd(client: socket.socket, elements: list):
    num_replicas = int(elements[1])
    timeout_ms = int(elements[2])
    timeout_sec = timeout_ms / 1000
    target_offset = server_status["repl_offset"]  # offset after the write
    start_time = time.time()
     # Ask replicas for ACK
    for replica in server_status["replicas"]:
        try:
            replica.sendall(make_resp_command("REPLCONF", "GETACK", "*"))
        except Exception:
            pass

    while True:
        acknowledged = 0
        print(f"CURRENT OFFSET: {target_offset}")
        # Count replicas that have acknowledged this offset or more
        for offset in server_status["replica_offsets"].values():
            if offset >= target_offset:
                acknowledged += 1

        if acknowledged >= num_replicas:
            print(f"GOT ENOUGH REPLICAS")
            break  # required replicas reached

        if time.time() - start_time >= timeout_sec:
            print(f"RAN OUT OF TIME")
            break  # timeout reached

       

        time.sleep(0.05)  # small delay before checking again


    return f":{acknowledged}\r\n"

#Although CONFIG GET can fetch multiple parameters at a time, this will only read one parameter at a time.
def config_cmd(client: socket.socket, elements: list):
    parameter = elements[2].lower()
    if elements[1].lower() == "get":
        if parameter == "dir":
            return make_resp("dir", rdb_configs["dir"])
        elif parameter == "dbfilename":
            return make_resp("dbfilename", rdb_configs["dbfilename"])

# returns all the keys that match a given pattern, as a RESP array:
def keys_cmd(client: socket.socket, elements: list):
    target_key = elements[1].lower()
    if target_key == "*":
        return make_resp(*store.keys())
    elif "*" in target_key:
        filtered = ""
        k = []
        for char in target_key:
            if char != "*":
                filtered += char
        for key in store.keys():
            if filtered in key:
                k.append(key)
        return make_resp(*k)
    
    elif target_key in store.keys():
        return f"${len(store[target_key])}\r\n{store[target_key]}\r\n"

# registers the client as a subscriber to a channel.
def subscribe_cmd(client: socket.socket, elements: list):
    channel = elements[1].lower()
    subscribed = client_subscribed[client]
    if channel not in subscribed:
         subscribed.append(channel) 
    return f"*3\r\n${len("subscribe")}\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{len(subscribed)}\r\n"

# delivers a message to all clients subscribed to a channel
def publish_cmd(client: socket.socket, elements: list):
    channel = elements[1].lower()
    msg = elements[2]
    count = 0
    for c in client_subscribed:
        if channel in client_subscribed[c]:
            count += 1
            c.sendall(make_resp_command("message", channel, msg))
    return f":{count}\r\n"

# used to unsubscribe from a channel. It removes the client from the list of subscribers for that channel.
def unsubscribe_cmd(client: socket.socket, elements:list):
    channel = elements[1].lower()
    if channel in client_subscribed[client]:
        client_subscribed[client].remove(channel)
    return f"*3\r\n${len("unsubscribe")}\r\nunsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{len(client_subscribed[client])}\r\n"

def zadd_cmd(client: socket.socket, elements: list):
    key = elements[1]
    score = float(elements[2])
    member = elements[3]
    if key not in sorted_sets:
        sorted_sets[key] = {}
    if member not in sorted_sets[key]:
        sorted_sets[key][member] = score
        return f":1\r\n"
    else:
        sorted_sets[key][member] = score
        return f":0\r\n"

# used to query the rank of a member in a sorted set. It returns an integer, which is 0-based index of the member when the sorted set is ordered by increasing score. 
# If two members have same score, the members are ordered lexicographically.
def zrank_cmd(client: socket.socket, elements: list):
    key = elements[1]
    member = elements[2]
    if key not in sorted_sets or member not in sorted_sets[key]:
        return f"$-1\r\n"
    sorted_list = sorted(sorted_sets[key].items(), key=lambda x: (x[1], x[0]))
    for index, (m, score) in enumerate(sorted_list):
        if m == member:
            return f":{index}\r\n"

# used to list the members in a sorted set given a start index and an end index. The index of the first element is 0. The end index is inclusive.        
def zrange_cmd(client: socket.socket, elements: list):
    key = elements[1]
    start = int(elements[2])
    end = int(elements[3])
    if key not in sorted_sets:
        return f"*0\r\n"
    sorted_list = sorted(sorted_sets[key].items(), key=lambda x: (x[1], x[0]))
    array = f""
    
    if start < 0:
        start = len(sorted_list) + start
    if end < 0:
        end = len(sorted_list) + end
    if start < 0:
        start = 0
    if end < 0:
        end = 0  

    if start > len(sorted_list) - 1:
        return f"*0\r\n"
    if end > len(sorted_list) - 1:
        end = len(sorted_list) - 1
    if start > end:
        return f"*0\r\n"
    for i in range(start, end + 1, 1):
        member, score = sorted_list[i]
        array += f"${len(member)}\r\n{member}\r\n"
    return f"*{end - start + 1}\r\n{array}"

# used to query the cardinality (number of elements) of a sorted set. It returns an integer. The response is 0 if the sorted set specified does not exist.
def zcard_cmd(client: socket.socket, elements: list):
    key = elements[1]
    if key not in sorted_sets:
        return f":0\r\n"
    size = len(sorted_sets[key])
    return f":{size}\r\n"

# used to query the score of a member of a sorted set. If the sorted set and the member both exist, the score of the member is returned as a RESP bulk string.
def zscore_cmd(client: socket.socket, elements: list):
    key = elements[1]
    member = elements[2]
    if key not in sorted_sets or member not in sorted_sets[key]:
        return f"$-1\r\n"
    return f"${len(str(sorted_sets[key][member]))}\r\n{sorted_sets[key][member]}\r\n"

# used to remove a member from a sorted set given the member's name.
def zremove_cmd(client: socket.socket, elements: list):
    key = elements[1]
    member = elements[2]
    if key not in sorted_sets or member not in sorted_sets[key]:
        return f":0\r\n"
    del sorted_sets[key][member]
    return f":1\r\n"