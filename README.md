# NoobWS
Websockets for noobs.  
  
## Why

I personally had a hard time getting started with websockets, especially dealing with multiple subscriptions.  
I've noticed that issues mostly have to do with concurrency. Do I use `threading`, `asyncio`, `concurrent.futures`, ...?
  
The goal of this library is to avoid concurrency issues usually associated with websockets, hopefully preventing some poor sap from losing sleep.  
  
## How

This is done by splitting key tasks into two parts:
1. A local server handles interaction with the websocket(s).  
1. A local client, designed to work with synchronous code, will collect messages.  
  
The local server and client communicate via `ZMQ` sockets, by default:  
* client: `zmq.PUSH` -> server: `zmq.PULL*`  
* server: `zmq.PUB*` -> client: `zmq.SUB`  
  
Notes:
* Two separate addresses are used, one for each direction (i.e. client -> server, v.v.)
* Addresses are always bound on the server side.
* Socket types can be configured (e.g. `zmq.PAIR` -> `zmq.PAIR`)
* A server can interact with multiple sockets
* A client can connect to multiple servers
  
## What
  
Sample usage can be found in the `demo.py` file, which features the following cases:
* single client to single server
* single client to multi server
* multi client to single server
* multi client to multi server
  
I recognize the API isn't as clean as it can be, but is necessary to enable the use cases above. I'm very open to suggestions.  
Documentation is available within the classes and functions, accessible through `help(...)`.
  
## Disclaimer
  
Code may be buggy. Use at your own risk.