"""
noobWS  
Websockets for noobs  
  
Has the following classes:  
  
`NoobServer`: server that interacts with websocket(s)  
`MultiClient`: client that can connect to multiple servers  
`SingleClient`: client that connects to only one server, with a simpler interface  
"""

from ._client import MultiClient, SingleClient
from ._server import NoobServer