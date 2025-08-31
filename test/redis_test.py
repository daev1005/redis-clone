import time
from app.utils import send_command

def test_ping():
    resp = send_command(["PING"])
    assert resp.strip() == "+PONG"

def test_echo():
    resp = send_command(["ECHO", "hello"])
    assert resp.strip() == "$5\r\nhello"

def test_set_get():
    # Set key
    resp = send_command(["SET", "foo", "bar"])
    assert resp.strip() == "+OK"
    # Get key
    resp = send_command(["GET", "foo"])
    assert resp.strip() == "$3\r\nbar"

def test_incr():
    send_command(["SET", "counter", "10"])
    resp = send_command(["INCR", "counter"])
    assert resp.strip() == ":11"

def test_rpush_llen():
    send_command(["DEL", "mylist"])  # if you have DEL
    send_command(["RPUSH", "mylist", "a", "b", "c"])
    resp = send_command(["LLEN", "mylist"])
    assert resp.strip() == ":3"

def test_publish_subscribe():
    # subscription needs a persistent connection
    import socket
    from app.utils import make_resp_command

    sock = socket.create_connection(("localhost", 6379))
    sock.sendall(make_resp_command("SUBSCRIBE", "news"))
    resp = sock.recv(1024).decode()
    assert "subscribe" in resp.lower()

    # publish from another connection
    resp2 = send_command(["PUBLISH", "news", "hello"])
    assert resp2.strip().startswith(":")  # returns number of subs