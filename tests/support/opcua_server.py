"""Local OPC UA fixture. Install asyncua in a venv before starting it."""
import asyncio
from asyncua import Server, ua

async def main():
    server = Server()
    await server.init()
    server.set_endpoint('opc.tcp://127.0.0.1:12686/moqui')
    namespace = await server.register_namespace('urn:moqui:gateway:regression')
    value = await server.nodes.objects.add_variable(ua.NodeId('review_value', namespace), 'ReviewValue', 300.03)
    await value.set_writable()
    async with server:
        print('OPC UA fixture ready', flush=True)
        await asyncio.Event().wait()

if __name__ == '__main__':
    asyncio.run(main())
