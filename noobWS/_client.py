"""NoobClient Module"""

import logging
import queue
import threading
from typing import List

import zmq


class MultiClient:
    """MultiClient - a client subscribed to multiple `NoobServer`'s

    Designed to work with a synchronous code.

    Note: `ns` in parameters is supposed to stand for `NoobServer`
    """

    def __init__(
        self,
        topics: List[bytes] = None,
        qmaxsize: int = 50,
        timeout: float = 3.0,
        cmd_stype: int = None,
        pub_stype: int = None,
        **kwargs
    ):
        """
        Initialize MultiClient instance

        Parameters
        ----------
        topics: list of bytes, optional
            list of topics to subscribe to; defaults to `[b""]`
            This will act as the default/fallback for topics if not specified

        qmaxsize: int, optional
            number of most recent messages stored; defaults to `50`
        timeout: float, optional
            defaults to `3.0`
        cmd_stype, pub_stype: int
            ZMQ socket types; defaults to `zmq.PUSH` and `zmq.SUB` respectively
            These sockets will `connect` to their respective addresses
        **kwargs, dict
            extra keyword arguments serve as the specification for the socket addresses
            parameter = internal name for corresponding `NoobServer`
            value = Dict[str, str]
                `{"cmd": cmd_address, "sub": sub_address, "topics": topic_list}`
                "topics" is an optional key, used to specify topics at the socket level.
                The value must be a list of bytes, defaults to the `topics` parameter.

        Notes
        ----------
        The `timeout` affects the frequency of polling for the shutdown parameter.
        Practically, it doesn't really change much other than responsiveness of `.shutdown()`
        Side Note: I'd expect a longer timeout would lead to better performance?
        """
        # validate kwargs
        for name, kwarg_val in kwargs.items():
            assert set(kwarg_val.keys()) >= {
                "cmd",
                "pub",
            }, "Invalid value for {}. dict must have {'cmd', 'pub'}".format(name)

        self.socket_dict = kwargs
        self.cmd_addresses = {key: val["cmd"] for key, val in kwargs.items()}
        self.sub_addresses = {key: val["pub"] for key, val in kwargs.items()}

        self.cmd_stype = cmd_stype or zmq.PUSH  # pylint: disable=no-member
        self.pub_stype = pub_stype or zmq.SUB  # pylint: disable=no-member

        self.ctx = None
        self.cmd_queue = queue.Queue()
        self.msg_queue = queue.Queue(maxsize=qmaxsize)
        self.topics = topics or [b""]
        self.timeout = timeout

        self.shutdown_event = threading.Event()
        self.threads = {}

    def send(self, server_name: str, msg: bytes, ns_socket: bytes = b""):
        """Send message to socket

        Parameters
        ----------
        server_name: str
            internal server name as defined during initialization
        msg: bytes
        ns_socket: bytes
            `NoobServer` socket name
        """
        self.__send__(server_name, ns_socket, msg)

    def __send__(self, server_name: str, ns_socket: bytes, msg: bytes):
        """Internal method to send message"""

        if isinstance(msg, str):
            msg = msg.encode()

        if isinstance(ns_socket, str):
            ns_socket = ns_socket.encode()

        self.cmd_queue.put([server_name, ns_socket, msg])

    def recv(self, timeout: float = None):
        """Receive message to socket"""
        return self.__recv__(timeout=timeout)

    def __recv__(self, timeout: float = None):
        """Internal method to receive message"""
        return self.msg_queue.get(timeout=timeout)

    def _recv_formatter(self, msg, topic, ns_socket):
        """Format incoming message"""
        return [ns_socket, topic, msg]

    def start(self):
        """Start Client"""
        self.shutdown_event.clear()
        self.ctx = zmq.Context().instance()
        self.threads = {
            "cmd": threading.Thread(
                target=self._command_thread,
                name="cmd",
                kwargs=self.cmd_addresses,
                daemon=True,
            )
        }

        for sub_name, sub_addr in self.sub_addresses.items():
            self.threads["sub_{}".format(sub_name)] = threading.Thread(
                target=self._subscriber_thread,
                name="sub",
                args=(sub_addr, self.socket_dict[sub_name].get("topics", self.topics)),
                kwargs={"ns_socket": sub_name},
                daemon=True,
            )

        for name, thread in self.threads.items():
            logging.info("[client] Starting %s thread", name)
            thread.start()

        logging.info("[client] Ready")

    def stop(self):
        """Stop client"""
        logging.info(
            "[client] Initiating shutdown. Please wait for ~%.1f second(s)...", self.timeout
        )
        self.shutdown_event.set()
        for thread in self.threads.values():
            thread.join()

        self.ctx.term()
        logging.info("[client] shutdown complete")

    def _subscriber_thread(self, address, topics, ns_socket: str = ""):
        subscriber = self.ctx.socket(self.pub_stype)
        subscriber.RCVTIMEO = int(self.timeout * 1000)

        for topic in topics:
            logging.info("[client] Adding topic '%s'", topic)
            subscriber.subscribe(topic)

        subscriber.connect(address)

        logging.info(
            "[client] Subscribing to %s%s", "{}@".format(ns_socket) if ns_socket else "", address
        )
        while not self.shutdown_event.is_set():
            try:
                topic, msg = subscriber.recv_multipart()
                logging.info("[client] Received message %s|%s", topic, msg)
                if self.msg_queue.full():
                    val = self.msg_queue.get()
                    logging.warning("[client] Queue full. Throwing away '%s'", val)
                else:
                    fmt_msg = self._recv_formatter(msg, topic, ns_socket=ns_socket)
                    self.msg_queue.put(fmt_msg)
            except zmq.Again:
                pass

        logging.info("[client] _subscriber_thread closed")

    def _command_thread(self, **kwargs):
        cmd_sockets = {}
        for ns_socket, address in kwargs.items():
            cmd_sockets[ns_socket] = self.ctx.socket(self.cmd_stype)
            cmd_sockets[ns_socket].connect(address)

        logging.info("[client] Sending commands to: %s", kwargs)
        while not self.shutdown_event.is_set():
            try:
                server_name, ns_socket, cmd = self.cmd_queue.get(timeout=self.timeout)
                cmd_sockets[server_name].send_multipart([ns_socket, cmd])
            except queue.Empty:
                pass
            except KeyError:
                logging.warning("[client] Did not find '{}' in sockets".format(server_name))

        logging.info("[client] _command_thread closed")


class SingleClient(MultiClient):
    """SingleClient - a MultiClient with only ONE subscription

    A slightly different interface that is meant to be simpler.
    """

    SOCKETNAME = "singleclient"

    def __init__(
        self,
        cmd_addr: str,
        pub_addr: str,
        topics: List[bytes] = None,
        qmaxsize: int = 50,
        timeout: float = 3.0,
    ):
        """
        Initialize BaseClient instance

        Parameters
        ----------
        cmd_addr: str
            socket listener address; corresponds to Socket().cmd_addr
        pub_addr: str
            socket publisher address; corresponds to Socket().pub_addr
        topics: list of bytes, optional
            list of topics to subscribe to; defaults to `[b""]`
        qmaxsize: int, optional
            number of most recent messages stored; defaults to `50`
        timeout: float, optional
            defaults to `3.0`

        Notes
        ----------
        The `timeout` affects the frequency of polling for the shutdown parameter.
        Practically, it doesn't really change much other than responsiveness of `.shutdown()`
        Side Note: I'd expect a longer timeout would lead to better performance?
        """
        self.cmd_addr = cmd_addr
        self.pub_addr = pub_addr
        super().__init__(
            topics=topics,
            qmaxsize=qmaxsize,
            timeout=timeout,
            **{self.SOCKETNAME: {"cmd": cmd_addr, "pub": pub_addr}}
        )

    def send(
        self, msg: bytes or str, ns_socket: bytes or str = b""
    ):  # pylint: disable=arguments-differ
        """Send message to socket

        Parameters
        ----------
        msg: bytes
            message to be sent
        ns_socket: bytes, optional
            socket name, if subscribed to a `MultiSocket`; defaults to `b""`
        """
        self.__send__(self.SOCKETNAME, ns_socket, msg)

    def recv(self, timeout: float = None):
        _, topic, msg = self.__recv__(timeout=timeout)
        return [topic, msg]


