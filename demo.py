"""
Demo code with an echo socket
"""

import logging
import multiprocessing
import time

import noobWS
from noobWS.constants import Keyword

logging.basicConfig(format="%(asctime)s|%(levelname)s: %(message)s", level=logging.INFO)


PORTS = {
    "multi1": {"cmd": "tcp://127.0.0.1:50000", "pub": "tcp://127.0.0.1:50001"},
    "multi2": {"cmd": "tcp://127.0.0.1:50002", "pub": "tcp://127.0.0.1:50003"},
    "single1": {"cmd": "tcp://127.0.0.1:50004", "pub": "tcp://127.0.0.1:50005"},
    "single2": {"cmd": "tcp://127.0.0.1:50006", "pub": "tcp://127.0.0.1:50007"},
}

URIS = [
    "wss://echo.websocket.org/",
    "wss://echo.websocket.org/",
    #  "wss://demos.kaazing.com/echo",  # not working for some reason
]


def start_single(idx: int, name: bytes = b""):
    """Start demo Single subscription NoobServer"""
    key = "single{:d}".format(idx + 1)
    ports = PORTS[key]

    ss = noobWS.NoobServer(URIS[idx], ports["cmd"], ports["pub"], name=name)

    # changed to show source socket
    ss.out_formatter = lambda msg, uri_name: [
        uri_name,
        "{}: {}".format(key, msg).encode(),
    ]
    ss.run()


def start_multi(idx: int, name: bytes = b""):
    """Start demo Multi subscription NoobServer"""
    key = "multi{:d}".format(idx + 1)
    ports = PORTS[key]
    ms = noobWS.NoobServer(
        dict(zip([b"echo1", b"echo2"], URIS)), ports["cmd"], ports["pub"], name=name
    )
    ms.run()


def demo_sc_ss():
    """Single Client x Single Socket"""

    logging.info("DEMO: Single Client x Single Socket echo server.")
    logging.info("Please ensure you can connect to %s", URIS[0])

    sock_name = b"single-socket"
    p = multiprocessing.Process(
        target=start_single, args=(0,), kwargs={"name": sock_name}, daemon=True
    )
    p.start()

    sleep_s = 3
    logging.info("Wait ~%.1f seconds for the socket to initialize", sleep_s)
    time.sleep(sleep_s)

    ports = PORTS["single1"]
    sc = noobWS.SingleClient(ports["cmd"], ports["pub"])
    sc.start()

    logging.info("SENDING>>> 'hello'")
    sc.send("hello", uri_name=sock_name)
    msg = sc.recv()
    assert msg == [sock_name, b"single1: hello"], f"received {msg}"
    logging.info(">>>RECEIVED [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> KILL_SIGNAL")
    sc.shutdown_server()
    msg = sc.recv()
    assert msg == [
        Keyword.command.value,
        f"single1: {Keyword.shutdown.value}".encode(),
    ], f"received {msg}"
    logging.info(">>>RECEIVED [%s]: %s", *msg)

    time.sleep(1)

    p.join()
    sc.stop()


def demo_sc_ms():
    """Single Client x Multi Socket"""

    logging.info("DEMO: Single Client x Multi Socket echo server.")
    logging.info("Please ensure you can connect to %s", URIS)

    p = multiprocessing.Process(
        target=start_multi, args=(0,), kwargs={"name": b"multi-socket"}, daemon=True
    )
    p.start()

    sleep_s = 3
    logging.info("Wait ~%.1f seconds for the socket to initialize", sleep_s)
    time.sleep(sleep_s)

    ports = PORTS["multi1"]
    sc = noobWS.SingleClient(ports["cmd"], ports["pub"])
    sc.start()

    logging.info("SENDING>>> 'hello' to echo1")
    sc.send("hello, echo1", uri_name="echo1")
    msg = sc.recv()
    assert msg == [b"echo1", b"hello, echo1"], f"received {msg}"
    logging.info(">>>RECEIVED [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to echo2")
    sc.send("hello, echo2", uri_name="echo2")
    msg = sc.recv()
    assert msg == [b"echo2", b"hello, echo2"], f"received {msg}"
    logging.info(">>>RECEIVED [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> KILL_SIGNAL")
    sc.shutdown_server()
    msg = sc.recv()
    assert msg == [Keyword.command.value, Keyword.shutdown.value], f"received {msg}"
    logging.info(">>>RECEIVED [%s]: %s", *msg)

    time.sleep(1)

    p.join()
    sc.stop()


def demo_mc_ss():
    logging.info("DEMO: Multi Client x Single Socket echo server.")
    logging.info("Please ensure you can connect to %s", URIS)

    procs = []
    for idx in range(2):
        # internal name assigned to identify itself in published messages
        socket_name = "single{:d}".format(idx + 1).encode()
        p = multiprocessing.Process(
            target=start_single, args=(idx,), kwargs={"name": socket_name}, daemon=True
        )
        p.start()
        procs.append(p)

    sleep_s = 3
    logging.info("Wait ~%.1f seconds for the socket to initialize", sleep_s)
    time.sleep(sleep_s)

    # corresponding NoobServers will be named based on kwargs (i.e. socket1, socket2)
    mc = noobWS.MultiClient(socket1=PORTS["single1"], socket2=PORTS["single2"])
    mc.start()

    logging.info("SENDING>>> 'hello' to single1")
    mc.send("socket1", "hello", uri_name="single1")
    msg = mc.recv(None)
    assert msg == [
        "socket1",
        b"single1",
        b"single1: hello",
    ], f"received {msg}"
    logging.info(">>>RECEIVED %s, [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to single2")
    # since `socket2` is only has one external socket, it will default to the only socket
    mc.send("socket2", "hello")
    msg = mc.recv(None)
    assert msg == [
        "socket2",
        b"single2",
        b"single2: hello",
    ], f"received {msg}"
    logging.info(">>>RECEIVED %s, [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> KILL_SIGNAL to all sockets")
    for idx, socket_name in enumerate(mc.socket_dict):
        ns_name = "socket{:d}".format(idx + 1)
        mc.shutdown_server(ns_name)
        msg = mc.recv(None)
        assert msg == [
            ns_name,
            Keyword.command.value,
            f"single{idx + 1:d}: {Keyword.shutdown.value}".encode(),
        ], f"received {msg}"
        logging.info(">>>RECEIVED from %s, [%s]: %s", *msg)

    logging.info("Wait a few seconds before joining")
    time.sleep(5)

    logging.info("joining")
    for p in procs:
        p.join()

    logging.info("stopping client")
    mc.stop()


def demo_mc_ms():
    logging.info("DEMO: Multi Client x Multi Socket echo server.")
    logging.info("Please ensure you can connect to %s", URIS)

    procs = []

    for idx in range(2):
        # internal name assigned to identify itself in published messages
        socket_name = "mc{:d}".format(idx + 1).encode()
        p = multiprocessing.Process(
            target=start_multi, args=(idx,), kwargs={"name": socket_name}, daemon=True
        )
        p.start()
        procs.append(p)

    sleep_s = 3
    logging.info("Wait ~%.1f seconds for the socket to initialize", sleep_s)
    time.sleep(sleep_s)

    # corresponding NoobServers will be named based on kwargs (i.e. socket1, socket2)
    mc = noobWS.MultiClient(socket1=PORTS["multi1"], socket2=PORTS["multi2"])
    mc.start()

    logging.info("SENDING>>> 'hello' to multi1.echo1")
    mc.send("socket1", "hello to m1e1", uri_name="echo1")
    msg = mc.recv(None)
    assert msg == ["socket1", b"echo1", b"hello to m1e1"], f"received {msg}"
    logging.info(">>>RECEIVED %s, [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to multi1.echo2")
    mc.send("socket1", "hello to m1e2", uri_name="echo2")
    msg = mc.recv(None)
    assert msg == ["socket1", b"echo2", b"hello to m1e2"], f"received {msg}"
    logging.info(">>>RECEIVED %s, [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to multi2.echo1")
    mc.send("socket2", "hello to m2e1", uri_name="echo1")
    msg = mc.recv(None)
    assert msg == ["socket2", b"echo1", b"hello to m2e1"], f"received {msg}"
    logging.info(">>>RECEIVED %s, [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to multi2.echo2")
    mc.send("socket2", "hello to m2e2", uri_name="echo2")
    msg = mc.recv(None)
    assert msg == ["socket2", b"echo2", b"hello to m2e2"], f"received {msg}"
    logging.info(">>>RECEIVED %s, [%s]: %s", *msg)

    time.sleep(1)

    logging.info("SENDING>>> KILL_SIGNAL to all sockets")
    for idx, socket_name in enumerate(mc.socket_dict):
        ns_name = "socket{:d}".format(idx + 1)
        mc.shutdown_server(ns_name)
        msg = mc.recv(None)
        assert msg == [
            ns_name,
            Keyword.command.value,
            Keyword.shutdown.value,
        ], f"received {msg}"
        logging.info(">>>RECEIVED from %s, [%s]: %s", *msg)

    logging.info("Wait a few seconds before joining")
    time.sleep(5)

    logging.info("joining")
    for p in procs:
        p.join()

    logging.info("stopping client")
    mc.stop()


if __name__ == "__main__":
    demo_sc_ss()
    demo_sc_ms()
    demo_mc_ss()
    demo_mc_ms()
