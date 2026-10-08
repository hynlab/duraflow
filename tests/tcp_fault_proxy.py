"""A test-owned TCP link: real wire delay and connection loss, without dependencies."""

import asyncio
from contextlib import suppress


class TCPFaultProxy:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.blocked = False
        self.delay = 0
        self.forwarded_bytes = 0
        self.connections = set()
        self.handlers = set()

    async def __aenter__(self):
        self.server = await asyncio.start_server(self.accept, "127.0.0.1", 0)
        self.local_port = self.server.sockets[0].getsockname()[1]
        return self

    async def accept(self, reader, writer):
        task = asyncio.current_task()
        self.handlers.add(task)
        upstream = None
        pumps = []
        try:
            if self.blocked:
                return
            remote, upstream = await asyncio.open_connection(self.host, self.port)
            self.connections.update((writer, upstream))
            if self.blocked:
                return

            async def pump(source, target):
                while data := await source.read(65536):
                    if self.delay:
                        await asyncio.sleep(self.delay)
                    target.write(data)
                    await target.drain()
                    self.forwarded_bytes += len(data)

            pumps = [asyncio.create_task(pump(reader, upstream)), asyncio.create_task(pump(remote, writer))]
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        except (ConnectionError, OSError):
            pass
        finally:
            for pump_task in pumps:
                pump_task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            for connection in (writer, upstream):
                if connection is not None:
                    self.connections.discard(connection)
                    connection.close()
                    with suppress(ConnectionError, OSError):
                        await connection.wait_closed()
            self.handlers.discard(task)

    def disconnect(self):
        self.blocked = True
        for connection in tuple(self.connections):
            connection.transport.abort()

    async def __aexit__(self, *args):
        self.disconnect()
        self.server.close()
        await self.server.wait_closed()
        handlers = list(self.handlers)
        for task in handlers:
            task.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)
