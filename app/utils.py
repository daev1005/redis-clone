import os
import struct
import socket
from app.main import command_map
from app.state import server_status, store, expiration_time

def make_resp_command(*parts: str):
    resp = f"*{len(parts)}\r\n"
    for p in parts:
        resp += f"${len(p)}\r\n{p}\r\n"
    return resp.encode()  

def make_resp(*parts: str):
    resp = f"*{len(parts)}\r\n"
    for p in parts:
        resp += f"${len(p)}\r\n{p}\r\n"
    return resp

##---------------------------------------------------------------------
### LOOK BACK ON THIS: VERY CONFUSING
def load_rdb_file(file_path):
    global store, expiration_time
    if not os.path.exists(file_path):
        return

    with open(file_path, "rb") as f:
        data = f.read()

    pos = 0
    expire_time = None

    # Skip header "REDIS0011"
    if data.startswith(b"REDIS"):
        pos = 9

    while pos < len(data):
        opcode = data[pos]
        pos += 1

        if opcode == 0xFA:  # AUX field
            key_len, pos = read_length(data, pos)
            pos += key_len
            val_len, pos = read_length(data, pos)
            if isinstance(val_len, int):
                pos += val_len

        elif opcode == 0xFE:  # DB selector
            _, pos = read_length(data, pos)

        elif opcode == 0xFB:  # Hash table sizes
            _, pos = read_length(data, pos)
            _, pos = read_length(data, pos)

        elif opcode == 0xFC:  # Expire in ms
            expire_time = struct.unpack('<Q', data[pos:pos+8])[0]
            pos += 8

        elif opcode == 0xFD:  # Expire in sec
            expire_sec = struct.unpack('<I', data[pos:pos+4])[0]
            expire_time = expire_sec * 1000
            pos += 4

        elif opcode == 0x00:  # String type
            if pos >= len(data): break

            # Read key length
            key_len, pos = read_length(data, pos)
            if isinstance(key_len, tuple) and key_len[0] == "ENC":
                raise ValueError("Special encoding for keys not supported")
            key = data[pos:pos+key_len].decode('utf-8', errors='ignore')
            pos += key_len

            if pos >= len(data): break

            # Read value length or encoding
            val_len, pos = read_length(data, pos)
            if isinstance(val_len, tuple) and val_len[0] == "ENC":
                encoding = val_len[1]
                if encoding == 0:  # Integer 8-bit
                    value = str(data[pos])
                    pos += 1
                elif encoding == 1:  # Integer 16-bit
                    value = str(struct.unpack('<H', data[pos:pos+2])[0])
                    pos += 2
                elif encoding == 2:  # Integer 32-bit
                    value = str(struct.unpack('<I', data[pos:pos+4])[0])
                    pos += 4
                elif encoding == 3:  # LZF compression
                    raise NotImplementedError("LZF compression not supported")
                else:
                    raise ValueError(f"Unknown encoding type: {encoding}")
            else:
                value = data[pos:pos+val_len].decode('utf-8', errors='ignore')
                pos += val_len

            store[key] = value
            if expire_time is not None:
                expiration_time[key] = expire_time / 1000.0  # ms → sec
            expire_time = None

        elif opcode == 0xFF:  # End of RDB
            break

        else:
            continue
    return

def read_length(data, pos):
    first = data[pos]
    pos += 1
    type_bits = first >> 6

    if type_bits == 0:  # 6-bit
        return first & 0x3F, pos
    elif type_bits == 1:  # 14-bit
        second = data[pos]
        pos += 1
        length = ((first & 0x3F) << 8) | second
        return length, pos
    elif type_bits == 2:  # 32-bit
        length = struct.unpack('>I', data[pos:pos+4])[0]
        pos += 4
        return length, pos
    elif type_bits == 3:  # Special encoding
        encoding_type = first & 0x3F  # last 6 bits
        return ("ENC", encoding_type), pos
    else:
        raise ValueError("Unsupported encoding")

##---------------------------------------------------------------------

# Helper function to find and execute the command
def find_cmd(cmd, client: socket.socket, elements: list):
    # Execute the command on the master first
    result = None
    if cmd in command_map:
        result = command_map[cmd](client, elements)
    else:
        client.sendall(f"-ERR unknown command '{cmd}'\r\n".encode())
        return None

    # Only replicate write commands if we're a master
    if server_status["server_role"] == "master":
        write_to_replicas(cmd, elements)
    return result

# Sends write commands to all connected replicas
def write_to_replicas(cmd, elements):
    write_commands = {"set", "rpush", "lpush", "lpop", "blpop", "incr", "xadd"}
    dead_replicas = []
    if cmd in write_commands:
        command_bytes = make_resp_command(*elements)
        command_size = len(command_bytes)
        server_status["repl_offset"] += command_size
        for replicated_client in server_status["replicas"]:
            try:
                replicated_client.sendall(command_bytes)
            except Exception:
                dead_replicas.append(replicated_client)
        # read replica's reply (if needed)
        
        # clear at the end
        for r in dead_replicas:
            server_status["replicas"].remove(r)
            # Also remove from replica_offsets
            if r in server_status["replica_offsets"]:
                del server_status["replica_offsets"][r]

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