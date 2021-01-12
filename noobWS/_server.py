"""NoobServer Module"""

import asyncio
import logging
from typing import Dict, List

import websockets
import zmq
from zmq.asyncio import Context

from .constants import Keyword


class NoobServer:
    """
    NoobServer class

    Has the following channels:
    * Websockets
        * Socket(s) connected to external URI(s)
        * If single socket, socket name = `self.name`
        * If multi socket, socket name = key in `uri` param
    * Command listener (zmq.PULL by default)
        * listens to incoming messages; passed to the appropriate websocket
        * messages must be multipart, i.e. `[uri_name, message]`
    * Publisher (zmq.PUB  by default)
        * publishes formatted messages received from the external websocket(s)
        * messages will be multipart, i.e. `[topic, message]`

    It is recommended to override the `in_formatter` and `out_formatter` methods
    to match your desired input / output formats
    """

    def __init__(
        self,
        uri: Dict[bytes, str] or str,
        cmd_addr: str,
        pub_addr: str,
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
        name : bytes, optional
            default socket name for single socket, defaults to `b""`
        cmd_stype, pub_stype: int
            ZMQ socket types; defaults to `zmq.PULL` and `zmq.PUB` respectively
            These sockets will `bind` to their respective addresses
        """

        self.name = name or b""
        if isinstance(self.name, str):
            self._name_str = self.name
            self.name = self.name.encode()
        else:
            self._name_str = self.name.decode()

        if isinstance(uri, str):
            uri = {self.name: uri}

        assert all(
            isinstance(key, bytes) for key in uri
        ), "all `uri` keys must be type `bytes`"

        self.uri = uri
        self.cmd_addr = cmd_addr
        self.pub_addr = pub_addr
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

        Handles incoming messages from a NoobClient

        Notes
        ---------
        Messages should come in the following format:
            `[uri_name: bytes, message: bytes]`

        `uri_name` should be valid; i.e. in `uri` or `Keyword.command.value`
        """
        receiver = self.ctx.socket(self.cmd_stype)  # pylint: disable=no-member
        receiver.bind(self.cmd_addr)

        solo_ws = next(iter(self.ws.values())) if len(self.ws) == 1 else None

        logging.info(
            f"[NoobServer|{self._name_str}] Receiving NoobClient messages on: %s",
            self.cmd_addr,
        )
        while True:
            try:
                uri_name, msg = await receiver.recv_multipart()
            except ValueError:
                logging.error(
                    "Invalid value received. "
                    "Please make sure format is `[uri_name, msg]`"
                )
                continue

            logging.info(
                f"[NoobServer|{self._name_str}] LISTENER received %s|%s",
                uri_name,
                msg,
            )
            if uri_name == Keyword.command.value:
                if msg == Keyword.shutdown.value:
                    logging.info(
                        f"[NoobServer|{self._name_str}] LISTENER received KILL signal"
                    )
                    break
                elif msg == Keyword.ping.value:
                    await self.msg_queue.put([uri_name, Keyword.pong.value])
            else:
                # formatter here
                fmt_msg = self.in_formatter(msg, uri_name)
                try:
                    await self.ws[uri_name].send(fmt_msg)
                except KeyError:
                    logging.warning(
                        f"[NoobServer|{self._name_str}] '%s' is not in provided uri(s): %s",
                        uri_name,
                        self.ws.keys(),
                    )
                    if len(self.ws) == 1:
                        logging.warning(
                            (
                                f"[NoobServer|{self._name_str}] Ignoring uri_name '%s', "
                                "sending message to ONLY socket"
                            ),
                            uri_name,
                        )
                        await solo_ws.send(fmt_msg)

        receiver.close()
        logging.info(f"[NoobServer|{self._name_str}] LISTENER closed")
        await self.shutdown(False)

    async def data_feed(
        self, ws: websockets.WebSocketClientProtocol, uri_name: bytes = b""
    ):
        """
        Websocket Coroutine

        Handles the connection to external websockets

        Parameters
        ----------
        ws: websockets.WebSocketClientProtocol
        uri_name: bytes, optional
            will be supplied to data sent to NoobServer publisher

        Notes
        ----------
        Messages received are passed to the publisher with the following format:
            `[uri_name: bytes, message: bytes]`
        """
        log_prefix = "{}| ".format(uri_name.decode()) if uri_name else ""
        error_caught = False
        try:
            async for msg in ws:
                logging.debug(
                    f"[NoobServer|{self._name_str}] %sReceived %s", log_prefix, msg
                )
                await self.msg_queue.put([uri_name, msg])
        except websockets.ConnectionClosedError:
            logging.error("websocket closed unexpectedly %s", ws.remote_address)
            error_caught = True
        except Exception as err:
            logging.error("websocket error: %s", repr(err))
            error_caught = True
        finally:
            if error_caught:
                await self.shutdown(True)

    async def publisher(self):
        """
        Publisher Coroutine

        Processes messages from `data_feed` coroutine(s) and publishes for NoobClients
        """
        broadcaster = self.ctx.socket(self.pub_stype)  # pylint: disable=no-member
        broadcaster.setsockopt(zmq.LINGER, 3000)
        broadcaster.bind(self.pub_addr)

        logging.info(
            f"[NoobServer|{self._name_str}] Publishing messages on: %s",
            self.pub_addr,
        )
        while True:
            uri_name, msg = await self.msg_queue.get()
            topic, fmt_msg = self.out_formatter(msg, uri_name)
            logging.debug(
                f"[NoobServer|{self._name_str}] PUBLISHER sending %s|%s",
                topic,
                fmt_msg,
            )
            await broadcaster.send_multipart([topic, fmt_msg])

            if (uri_name == Keyword.command.value) and (msg == Keyword.shutdown.value):
                break

        broadcaster.close()
        logging.info(f"[NoobServer|{self._name_str}] PUBLISHER closed")

    async def shutdown(self, internal: bool):
        """Shutdown Coroutine

        Parameters
        ----------
        internal: bool
            flag if shutdown triggered internally; (e.g. due to internal error)
        """
        logging.warning(f"[NoobServer|{self._name_str}] shutdown initiated")

        # close listener
        if internal:
            grim_reaper = self.ctx.socket(zmq.PUSH)  # pylint: disable=no-member
            grim_reaper.connect(self.cmd_addr)
            await grim_reaper.send_multipart(
                [Keyword.command.value, Keyword.shutdown.value]
            )
            grim_reaper.close()

        # close publisher
        await self.msg_queue.put([Keyword.command.value, Keyword.shutdown.value])

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

        logging.info(f"[NoobServer|{self._name_str}] Starting")
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

        logging.info(f"[NoobServer|{self._name_str}] starting tasks")
        try:
            self.connected = True
            self.loop.run_until_complete(asyncio.gather(*coros))
        except KeyboardInterrupt:
            logging.warning(f"[NoobServer|{self._name_str}] KeyboardInterrupt caught!")
            self.loop.run_until_complete(self.shutdown(True))
        finally:
            self.ctx.term()
            self.loop.close()

        logging.info(f"[NoobServer|{self._name_str}] Closed.")

    def restart(self, addl_coros: list = None):
        """Restart NoobServer; calls `shutdown` first, then `run`"""
        self.loop.run_until_complete(self.shutdown(True))
        self.run(addl_coros=addl_coros)

    def in_formatter(self, msg: bytes, uri_name: bytes = b""):
        """Formatter for incoming messages

        Parameters
        ----------
        msg: bytes
        uri_name: bytes, optional
            defaults to `b""`

        Returns
        ----------
        bytes or string
            Output will be sent to the corresponding websocket

        Notes
        ----------
        Only formats the `msg` part of the multi-part message received by the listener.
        `uri_name` is always required since it identifies the destination URI.
        """
        return msg.decode()

    def out_formatter(self, msg, uri_name: bytes = b""):
        """Formatter for outgoing multi-part messages

        Parameters
        ----------
        msg: bytes
        uri_name: bytes, optional
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

        return [uri_name, fmsg]
