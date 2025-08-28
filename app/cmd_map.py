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