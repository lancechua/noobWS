"""NoobClient Module"""
from functools import wraps
import logging
import queue
import threading
from typing import List, Union

import zmq

from .constants import Keyword


class MultiClient:
    """MultiClient - a client subscribed to one or more `NoobServer`(s)

    Designed to work with a synchronous code.

    Notes
    -----
    `ns` in parameters is supposed to stand for `NoobServer`
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
                The value must be a list of bytes, defaults to `topics` parameter value

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

        self.running = False
        self.shutdown_event = threading.Event()
        self.threads = {}

    def send(self, ns_name: str, msg: bytes, uri_name: bytes = b""):
        """Send message to socket

        Parameters
        ----------
        ns_name: str
            NoobServer name, as defined during initialization
        msg: bytes
        uri_name: bytes
            destination URI in ns_name NoobServer
        """
        self.cmd_queue.put(self.send_formatter(ns_name, uri_name, msg))

    def send_formatter(self, ns_name: str, uri_name: bytes, msg: bytes):
        """Format message before sending"""
        try:
            ns_name = ns_name.decode()
        except AttributeError:
            pass

        try:
            msg = msg.encode()
        except AttributeError:
            pass

        try:
            uri_name = uri_name.encode()
        except AttributeError:
            pass

        return [ns_name, uri_name, msg]

    def recv(self, timeout: float = None):
        """Receive message to socket"""
        return self.msg_queue.get(timeout=timeout)

    def recv_formatter(self, msg: bytes, topic: bytes, uri_name: str):
        """Format received message"""
        return [uri_name, topic, msg]

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
                kwargs={"uri_name": sub_name},
                daemon=True,
            )

        for name, thread in self.threads.items():
            logging.info("[client] Starting %s thread", name)
            thread.start()

        logging.info("[client] Ready")
        self.running = True

    def stop(self):
        """Stop client"""
        logging.info(
            "[client] Initiating shutdown. Please wait for ~%.1f second(s)...",
            self.timeout,
        )
        self.shutdown_event.set()
        for thread in self.threads.values():
            thread.join()

        self.ctx.term()
        logging.info("[client] shutdown complete")
        self.running = False

    def _subscriber_thread(self, address, topics, uri_name: str = ""):
        subscriber = self.ctx.socket(self.pub_stype)
        subscriber.RCVTIMEO = int(self.timeout * 1000)

        for topic in topics:
            logging.info("[client] Adding topic '%s'", topic)
            subscriber.subscribe(topic)

        subscriber.connect(address)

        logging.info(
            "[client] Subscribing to %s%s",
            "{}@".format(uri_name) if uri_name else "",
            address,
        )
        while not self.shutdown_event.is_set():
            try:
                topic, msg = subscriber.recv_multipart()
                logging.debug("[client] Received message %s|%s", topic, msg)
                if self.msg_queue.full():
                    val = self.msg_queue.get()
                    logging.warning("[client] Queue full. Throwing away '%s'", val)
                else:
                    fmt_msg = self.recv_formatter(msg, topic, uri_name=uri_name)
                    self.msg_queue.put(fmt_msg)
            except zmq.Again:
                pass

        logging.info("[client] _subscriber_thread closed")

    def _command_thread(self, **kwargs):
        cmd_sockets = {}
        for ns_name, address in kwargs.items():
            cmd_sockets[ns_name] = self.ctx.socket(self.cmd_stype)
            cmd_sockets[ns_name].connect(address)

        logging.info("[client] Sending commands to: %s", kwargs)
        while not self.shutdown_event.is_set():
            try:
                ns_name, uri_name, cmd = self.cmd_queue.get(timeout=self.timeout)
                cmd_sockets[ns_name].send_multipart([uri_name, cmd])
            except queue.Empty:
                pass
            except KeyError:
                logging.warning("[client] Did not find '{}' in sockets".format(ns_name))

        logging.info("[client] _command_thread closed")

    # COMMANDS
    def ping(self, ns_name: str):
        """Send ping to NoobServer

        Notes
        -----
        Expected reply: [Keyword.command.value, b"pong"]
        Keyword defined in noobWS.constants
        """
        self.send(ns_name, Keyword.ping.value, Keyword.command.value)

    def shutdown_server(self, ns_name: str):
        """Send shutdown signal to NoobServer"""
        self.send(ns_name, Keyword.shutdown.value, Keyword.command.value)


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

    def send(self, msg: bytes, uri_name: str = b""):  # pylint: disable=arguments-differ
        """Send message to socket

        Parameters
        ----------
        msg: bytes
            message to be sent
        uri_name: bytes, optional, defaults to b""
            socket name, required if NoobServer has multiple sockets
        """
        super().send(self.SOCKETNAME, msg, uri_name)

    def shutdown_server(self):
        self.send(Keyword.shutdown.value, Keyword.command.value)

    def ping(self):
        self.send(Keyword.ping.value, Keyword.command.value)

    def recv_formatter(self, msg: bytes, topic: bytes, uri_name: str):
        return [topic, msg]
