from enum import Enum


class Keyword(Enum):
    # tasks
    command = b"__command__"

    # commands
    shutdown = b"__shutdown__"
    ping = b"__ping__"
    pong = b"__pong__"
