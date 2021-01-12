"""
noobWS  
Websockets for noobs  
  
Has the following classes:  
  
`NoobServer`: server that interacts with external websocket(s)  
`MultiClient`: client that can connect to multiple NoobServers  
`SingleClient`: client that connects to only one NoobServer; has a simpler interface  
"""

from ._client import MultiClient, SingleClient
from ._server import NoobServer
from . import constants
