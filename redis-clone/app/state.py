from collections import defaultdict
import threading

# Global data structures to hold server state
server_status = {
    "server_role": "master",
    "repl_id": "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
    "repl_offset": 0,
    "replicas": [],
    "replica_offsets": {}
    }
blocked_clients = defaultdict(list)
blocked_streams = defaultdict(list)
client_subscribed = defaultdict(list)
lists = {}
list_locks = defaultdict(threading.Lock)  
store = {}
expiration_time = {}
queued = {}
rdb_configs = {
    "dir": "",
    "dbfilename": ""
}
sorted_sets = {}