"""
Demo code with an echo socket
"""

import logging
import multiprocessing
import time

import noobWS

logging.basicConfig(format="%(asctime)s|%(levelname)s: %(message)s", level=logging.INFO)


PORTS = {
    "multi1": {"cmd": "tcp://127.0.0.1:50000", "pub": "tcp://127.0.0.1:50001"},
    "multi2": {"cmd": "tcp://127.0.0.1:50002", "pub": "tcp://127.0.0.1:50003"},
    "single1": {"cmd": "tcp://127.0.0.1:50004", "pub": "tcp://127.0.0.1:50005"},
    "single2": {"cmd": "tcp://127.0.0.1:50006", "pub": "tcp://127.0.0.1:50007"},
}

URIS = ["wss://echo.websocket.org/", "ws://demos.kaazing.com/echo"]

KILL_SIGNAL = b"may you rest in a deep and dreamless slumber"


def start_single(idx: int, name: bytes = b""):
    """Start demo Single subscription NoobServer"""
    key = "single{:d}".format(idx + 1)
    ports = PORTS[key]

    ss = noobWS.NoobServer(
        URIS[idx], ports["cmd"], ports["pub"], name=name, kill_signal=KILL_SIGNAL
    )

    # changed to show source socket
    ss.out_formatter = lambda msg, ns_socket: [
        ns_socket,
        "{}: {}".format(key, msg).encode(),
    ]
    ss.run()


def start_multi(idx: int, name: bytes = b""):
    """Start demo Multi subscription NoobServer"""
    key = "multi{:d}".format(idx + 1)
    ports = PORTS[key]
    ms = noobWS.NoobServer(
        dict(zip([b"echo1", b"echo2"], URIS)),
        ports["cmd"],
        ports["pub"],
        name=name,
        kill_signal=KILL_SIGNAL,
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
    sc.send("hello", ns_socket=sock_name)
    logging.info(">>>RECEIVED [%s]: %s", *sc.recv())

    time.sleep(1)

    logging.info("SENDING>>> KILL_SIGNAL")
    sc.send(KILL_SIGNAL)
    logging.info(">>>RECEIVED [%s]: %s", *sc.recv())

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
    sc.send("hello, echo1", ns_socket="echo1")
    logging.info(">>>RECEIVED [%s]: %s", *sc.recv())

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to echo2")
    sc.send("hello, echo2", ns_socket="echo2")
    logging.info(">>>RECEIVED [%s]: %s", *sc.recv())

    time.sleep(1)

    logging.info("SENDING>>> KILL_SIGNAL")
    sc.send(KILL_SIGNAL)
    logging.info(">>>RECEIVED [%s]: %s", *sc.recv())

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
    mc.send("socket1", "hello", ns_socket="single1")
    logging.info(">>>RECEIVED %s, [%s]: %s", *mc.recv(None))

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to single2")
    # since `socket2` is only has one external socket, it will default to the only socket
    mc.send("socket2", "hello")
    logging.info(">>>RECEIVED %s, [%s]: %s", *mc.recv(None))

    time.sleep(1)

    logging.info("SENDING>>> KILL_SIGNAL to all sockets")
    for idx, socket_name in enumerate(mc.socket_dict):
        socket_client = "socket{:d}".format(idx + 1)
        mc.send(socket_client, KILL_SIGNAL, ns_socket=b"")
        logging.info(">>>RECEIVED from %s, [%s]: %s", *mc.recv(None))

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
        socket_name = "doesnt_matter_for_multiclient{:d}".format(idx + 1).encode()
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
    mc.send("socket1", "hello to m1e1", ns_socket="echo1")
    logging.info(">>>RECEIVED %s, [%s]: %s", *mc.recv(None))

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to multi1.echo2")
    mc.send("socket1", "hello to m1e2", ns_socket="echo2")
    logging.info(">>>RECEIVED %s, [%s]: %s", *mc.recv(None))

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to multi2.echo1")
    mc.send("socket2", "hello to m2e1", ns_socket="echo1")
    logging.info(">>>RECEIVED %s, [%s]: %s", *mc.recv(None))

    time.sleep(1)

    logging.info("SENDING>>> 'hello' to multi2.echo2")
    mc.send("socket2", "hello to m2e2", ns_socket="echo2")
    logging.info(">>>RECEIVED %s, [%s]: %s", *mc.recv(None))

    time.sleep(1)

    logging.info("SENDING>>> KILL_SIGNAL to all sockets")
    for idx, socket_name in enumerate(mc.socket_dict):
        socket_client = "socket{:d}".format(idx + 1)
        mc.send(socket_client, KILL_SIGNAL, ns_socket=b"")
        logging.info(">>>RECEIVED from %s, [%s]: %s", *mc.recv(None))

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
