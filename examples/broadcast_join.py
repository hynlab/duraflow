"""One publication, three distinct subscriptions, one all-success join."""
import asyncio

from application import OUTPUT, broadcasts, product, registry
from duraflow.testing import TestEnvironment


async def main() -> None:
    async with TestEnvironment(registry, broadcasts=broadcasts) as env:
        handle = await env.client.start(product, 7, request_id="broadcast-demo")
        print("Publication receipt:", await env.run(handle))
        print("Final payload:", [data.decode() for topic, data, _ in env.transport.publications if topic == OUTPUT.name])


if __name__ == "__main__":
    asyncio.run(main())
