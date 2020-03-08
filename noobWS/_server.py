"""NoobServer Module"""

import asyncio
import logging
from typing import Dict, List

import websockets
import zmq
from zmq.asyncio import Context


class NoobServer:
    """
    NoobServer class

    Has the following channels:
    * Websockets
        * Internal websockets connected to one or more URIs provided
        * If single socket, socket name = `self.name` attribute
        * If multi socket, socket name = corresponding key in `uri` attribute
    * Command listener (zmq.PULL by default)
        * listens to incoming messages, then sent to the appropriate websocket
        * messages must be multipart, i.e. `[socket_name, message]`
    * Message publisher (zmq.PUB  by default)
        * publishes formatted messages received by the websocket
        * messages will be multipart, i.e. `[topic, message]`

    It is recommended to override the `in_formatter` and `out_formatter` methods
    to match your desired input / output formats
    """

    def __init__(
        self,
        uri: Dict[bytes, str] or str,
        cmd_addr: str,
        pub_addr: str,
        kill_signal: bytes = None,
        name: bytes = None,
        cmd_stype: int = None,
        pub_stype: int = None,
    ):
        """
        Initialize a NoobServer with multiple websockets

        Parameters
        ----------
        uri: dict[bytes, str] or str
            Socket URI dictionary
            * key = name for socket
            * value = socket URI
            If `uri` is type `str`, connects to a single socket.
            Single socket name will be based on the `name` parameter.
        cmd_addr: str
            command listener address
        pub_addr: str
            message publisher address
        kill_signal: bytes, optional
            Triggers shutdown when received by command listener; defaults to `b"shutdown"`
        name : bytes, optional
            default socket name for single socket, defaults to `b""`
        cmd_stype, pub_stype: int
            ZMQ socket types; defaults to `zmq.PULL` and `zmq.PUB` respectively
            These sockets will `bind` to their respective addresses
        """

        self.name = name or b""
        if isinstance(self.name, str):
            self.name = self.name.encode()

        if isinstance(uri, str):
            uri = {self.name: uri}

        assert all(
            isinstance(key, bytes) for key in uri
        ), "all `uri` keys must be type `bytes`"

        self.uri = uri
        self.cmd_addr = cmd_addr
        self.pub_addr = pub_addr
        self.kill_signal = kill_signal or b"shutdown"
        self.cmd_stype = cmd_stype or zmq.PULL  # pylint: disable=no-member
        self.pub_stype = pub_stype or zmq.PUB  # pylint: disable=no-member

        self.ctx = Context.instance()
        self.loop = None
        self.msg_queue = None
        self.ws = None
        self.connected = False

    async def listener(self):
        """
        Listener Coroutine

        Handles incoming messages

        Notes
        ---------
        Messages should come in the following format:
            `[socket_name: bytes, message: bytes]`

        `socket_name` should be valid; i.e. in `uri`
        For the `kill_signal`, socket_name is still required, but is ignored.
        """
        receiver = self.ctx.socket(self.cmd_stype)  # pylint: disable=no-member
        receiver.bind(self.cmd_addr)

        logging.info("[socket] Receiving messages on: %s", self.cmd_addr)
        while True:
            try:
                socket_name, msg = await receiver.recv_multipart()
            except ValueError:
                logging.error(
                    "Invalid value received. "
                    "Please make sure format is `[socket_name, msg]`"
                )
                continue

            logging.info("[socket] LISTENER received %s|%s", socket_name, msg)
            if msg == self.kill_signal:
                logging.info("[socket] LISTENER received KILL signal")
                break
            elif msg == b"ping":
                await self.msg_queue.put([socket_name, b"pong"])
            else:
                # formatter here
                fmt_msg = self.in_formatter(msg, socket_name)
                try:
                    await self.ws[socket_name].send(fmt_msg)
                except KeyError:
                    logging.warning(
                        "[socket] '%s' is not in `self.ws`.keys(): %s",
                        socket_name,
                        self.ws.keys(),
                    )
                    if len(self.ws) == 1:
                        logging.warning(
                            "[socket] Ignoring socket_name '%s', sending message to ONLY socket",
                            socket_name,
                        )
                        await next(iter(self.ws.values())).send(fmt_msg)

        receiver.close()
        logging.info("[socket] LISTENER closed")
        await self.shutdown(False)

    async def data_feed(
        self, ws: websockets.WebSocketClientProtocol, socket_name: bytes = b""
    ):
        """
        Websocket Coroutine

        Handles the connection to websockets as defined in `uri`

        Parameters
        ----------
        ws: websockets.WebSocketClientProtocol
        socket_name: bytes, optional
            will be supplied to data sent to publisher

        Notes
        ----------
        Messages received are passed to the publisher with the following format:
            `[socket_name: bytes, message: bytes]`
        """
        log_prefix = "{}| ".format(socket_name) if socket_name else ""
        error_caught = False
        try:
            async for msg in ws:
                logging.debug("[client] %sReceived %s", log_prefix, msg)
                await self.msg_queue.put([socket_name, msg])
        except websockets.ConnectionClosedError:
            logging.error("websocket closed unexpectedly %s", ws.remote_address)
            error_caught = True
        except Exception:
            logging.error("websocket error: %s", repr(Exception))
            error_caught = True
        finally:
            if error_caught:
                await self.shutdown(True)

    async def publisher(self):
        """
        Publisher Coroutine

        Processes messages from `data_feed` coroutines and publishes it
        """
        broadcaster = self.ctx.socket(self.pub_stype)  # pylint: disable=no-member
        broadcaster.setsockopt(zmq.LINGER, 3000)
        broadcaster.bind(self.pub_addr)

        logging.info("[socket] Publishing messages on: %s", self.pub_addr)
        while True:
            socket_name, msg = await self.msg_queue.get()
            topic, fmt_msg = self.out_formatter(msg, socket_name)
            logging.debug("[socket] PUBLISHER sending %s|%s", topic, fmt_msg)
            await broadcaster.send_multipart([topic, fmt_msg])

            if msg == self.kill_signal:
                break

        broadcaster.close()
        logging.info("[socket] PUBLISHER closed")

    async def shutdown(self, internal):
        """Shutdown Coroutine"""
        logging.warning("[socket] shutdown initiated")

        # close listener
        if internal:
            grim_reaper = self.ctx.socket(zmq.PUSH)  # pylint: disable=no-member
            grim_reaper.connect(self.cmd_addr)
            await grim_reaper.send_multipart([b"__internal__", self.kill_signal])
            grim_reaper.close()

        # close publisher
        await self.msg_queue.put([self.name, self.kill_signal])

        # close sockets
        logging.info("Closing socket(s). This might take ~10 seconds...")
        await asyncio.gather(*[ws.close() for ws in self.ws.values()])

        self.connected = False

    def run(self, addl_coros: list = None):
        """Method to start Socket. Blocking

        Parameters
        ----------
        addl_coros: list
            additional coroutines / tasks to run
        """

        addl_coros = addl_coros or []

        logging.info("[socket] Starting")
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.msg_queue = asyncio.Queue()
        self.ws = {
            key: self.loop.run_until_complete(websockets.connect(uri))
            for key, uri in self.uri.items()
        }
        coros = (
            [self.listener(), self.publisher()]
            + [self.data_feed(ws, key) for key, ws in self.ws.items()]
            + addl_coros
        )

        logging.info("[socket] starting tasks")
        try:
            self.connected = True
            self.loop.run_until_complete(asyncio.gather(*coros))
        except KeyboardInterrupt:
            logging.warning("[socket] KeyboardInterrupt caught!")
            self.loop.run_until_complete(self.shutdown(True))
        finally:
            self.ctx.term()
            self.loop.close()

        logging.info("[socket] Closed.")

    def restart(self, addl_coros: list = None):
        self.loop.run_until_complete(self.shutdown(True))
        self.run(addl_coros=addl_coros)

    def in_formatter(self, msg: bytes, socket_name: bytes = b""):
        """Formatter for incoming messages

        Parameters
        ----------
        msg: bytes
        socket_name: bytes, optional
            defaults to `b""`

        Returns
        ----------
        bytes or string
            Output will be sent to the corresponding websocket

        Notes
        ----------
        Only formats the `msg` part of the multi-part message received by the listener.
        The `socket_name` part of the message will always be required,
        as it is used to identify sockets.
        """
        return msg.decode()

    def out_formatter(self, msg, socket_name: bytes = b""):
        """Formatter for outgoing multi-part messages

        Parameters
        ----------
        msg: bytes
        socket_name: bytes, optional
            used as the topic; defaults to `b""`

        Returns
        ----------
        tuple[bytes, bytes]
            `[topic, message]` to be broadcast
        """
        try:
            fmsg = msg.encode()
        except AttributeError:
            fmsg = msg

        return [socket_name, fmsg]
