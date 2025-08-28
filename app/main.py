import socket  # noqa: F401
import sys
import threading
import os
from app.state import *
from app.utils import *
from app.commands import *


command_map = {
    "ping": ping_cmd,
    "sping": sping_cmd,
    "echo": echo_cmd,
    "set": set_cmd,
    "get": get_cmd,
    "rpush": rpush_cmd,
    "lpush": lpush_cmd,
    "lpop": lpop_cmd,
    "blpop": blpop_cmd,
    "llen": llen_cmd,
    "lrange": lrange_cmd,
    "type": type_cmd,
    "xadd": xadd_cmd,
    "xrange": xrange_cmd,
    "xread": xread_cmd,
    "incr": incr_cmd,
    "info": info_cmd,
    "replconf": replconf_cmd,
    "psync": psync_cmd,
    "wait": wait_cmd,
    "config": config_cmd,
    "keys": keys_cmd,
    "subscribe": subscribe_cmd,
    "publish": publish_cmd,
    "unsubscribe": unsubscribe_cmd,
    "zadd": zadd_cmd,
    "zrank": zrank_cmd,
    "zrange": zrange_cmd,
    "zcard": zcard_cmd,
    "zscore": zscore_cmd,
    "zrem": zremove_cmd
}

subscribed_mode = [
    "subscribe",
    "unsubscribe",
    "psubscribe",
    "ping",
    "quit"
]

# Parses the command from the client input.
def parse_command(data: bytes):
    # Find the start of a RESP command (starts with *)
    start_pos = 0
    while start_pos < len(data):
        if data[start_pos:start_pos+1] == b'*':
            break
        start_pos += 1
    
    if start_pos >= len(data):
        raise ValueError("No RESP command found in buffer")
    command_data = data[start_pos:]
    input_str = command_data.decode()
    lines = input_str.split("\r\n")

    if not lines[0].startswith("*"):
        raise ValueError("Invalid command format")

    num_elements = int(lines[0][1:])
    elements = []
    index = 1
    for _ in range(num_elements):
        if index >= len(lines) - 1:
            raise ValueError("Incomplete command")
        if not lines[index].startswith("$"):
            raise ValueError("Invalid element format")
        lengthOfElement = int(lines[index][1:])
        index += 1
        if index >= len(lines):
            raise ValueError("Incomplete command")
        element = lines[index]
        if lengthOfElement != len(element):
            raise ValueError("Element length mismatch")
        elements.append(element)
        index += 1

    # Figure out how many raw bytes we actually consumed
    raw = "\r\n".join(lines[:index]) + "\r\n"
    consumed = len(raw.encode())
    total_consumed = start_pos + consumed
    return elements, total_consumed

##Takes in multiple clients and handles them concurrently
def handle_client(client: socket.socket):
    multi_called = False
    is_subscribed = False
    while True:
        #1024 is the bytesize of the input buffer (isn't fixed)
        input = client.recv(1024)
        elements, _ = parse_command(input)
        cmd = elements[0].lower()
        if is_subscribed:
            if cmd not in subscribed_mode:
                client.sendall(f"-ERR Can't execute '{cmd}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n".encode())
            else:
                if cmd == "ping":
                    cmd = "sping"
                response = find_cmd(cmd, client, elements, command_map)
                if response is not None:
                    client.sendall(response.encode())
        elif "multi" == cmd:
                client.sendall(b"+OK\r\n")
                multi_called = True
                queued[client] = []
        elif "exec" == cmd:
            commands = queued.get(client, [])
            if multi_called:
                if commands:
                    responses = []
                    for command in commands:
                        cmd_key = command[0].lower()
                        resp = find_cmd(cmd_key, client, command, command_map)
                        if resp is not None:
                            responses.append(resp)
                        write_to_replicas(cmd_key, commands)
                    msg = f"*{len(responses)}\r\n"
                    for response in responses:
                        msg  += response
                    client.sendall(msg.encode())
                else:
                    client.sendall(b"*0\r\n")
                multi_called = False
                queued[client] = []
            else:
                client.sendall(b"-ERR EXEC without MULTI\r\n")
        elif "discard" == cmd:
            if multi_called:
                client.sendall(b"+OK\r\n")
                multi_called = False
            else:
                client.sendall(b"-ERR DISCARD without MULTI\r\n")
        elif not multi_called:
            response = find_cmd(cmd, client, elements, command_map)
            if "subscribe" == cmd:
                is_subscribed = True
            if response is not None:
                client.sendall(response.encode())
        else:
            if client not in queued:
                queued[client] = []
            queued[client].append(elements)
            client.sendall(b"+QUEUED\r\n")

# Sends commands from the master to the replica and handles incoming data
def handle_replica(master_socket: socket.socket):
    buffer = b""  # accumulate incoming data
    while True:
        try:
            data = master_socket.recv(1024)
            if not data:
                break  # connection closed
            buffer += data

            while True:
                try:
                    # Try to parse one command from the buffer
                    elements, consumed = parse_command(buffer)
                    cmd = elements[0].lower()
                    buffer = buffer[consumed:]  # remove parsed command
                    command_size = len(make_resp_command(*elements))
                    # Execute the command BEFORE updating offset for GETACK
                    if cmd == "replconf" and len(elements) > 1 and elements[1].lower() == "getack":
                        # Execute GETACK with current offset, then update offset
                        find_cmd(cmd, master_socket, elements, command_map)
                        server_status["repl_offset"] += command_size
                    else:
                        # For all other commands, update offset first then execute
                        server_status["repl_offset"] += command_size
                        find_cmd(cmd, master_socket, elements, command_map)
                        

                except ValueError:
                    # Incomplete command, wait for more data
                    break

        except Exception as e:
            print(f"Replica connection error: {e}")
            break


    

        
     


def main():
    global server_status
    PORT = 6379  # default

    #Parse through server start command and get the port
    if "--port" in sys.argv:
        port_index = sys.argv.index("--port") + 1
        if port_index < len(sys.argv):
            PORT = int(sys.argv[port_index])

    if "--dir" in sys.argv:
        dir_index = sys.argv.index("--dir") + 1
        if dir_index < len(sys.argv):
            rdb_configs["dir"] = sys.argv[dir_index]
    
    if "--dbfilename" in sys.argv:
        db_index = sys.argv.index("--dbfilename") + 1
        if db_index < len(sys.argv):
            rdb_configs["dbfilename"] = sys.argv[db_index]
    
    if "--replicaof" in sys.argv:
        master_info = sys.argv[sys.argv.index("--replicaof") + 1].split()
        server_status["server_role"] = "slave"
        master_host = master_info[0]
        master_port = int(master_info[1])
        master_socket = socket.create_connection((master_host, master_port))
        master_socket.sendall(make_resp_command("PING"))
        response = master_socket.recv(1024) #pauses code until the connection actually receives something
        master_socket.sendall(make_resp_command("REPLCONF", "listening-port", str(PORT)))
        response = master_socket.recv(1024)
        master_socket.sendall(make_resp_command("REPLCONF", "capa", "psync2"))
        response = master_socket.recv(1024)
        master_socket.sendall(make_resp_command("PSYNC", "?", "-1"))
        # DON'T read the response here - let the replica thread handle everything after PSYNC
        
        # Start the replica handler thread immediately
        replica_thread = threading.Thread(
            target=handle_replica,
            args=(master_socket, ),
            daemon=True,
        )
        replica_thread.start()
    if rdb_configs["dir"] and rdb_configs["dbfilename"]:    
        load_rdb_file(os.path.join(rdb_configs["dir"], rdb_configs["dbfilename"]))
    print(f"Starting server on port {PORT}")
    server_socket = socket.create_server(("localhost", PORT), reuse_port=True)
    while True:
        connection,_ = server_socket.accept() # wait for client
        thread = threading.Thread(target=handle_client, args=(connection,))
        thread.daemon = True
        thread.start()

if __name__ == "__main__":
    main()
