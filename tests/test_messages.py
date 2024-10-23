from aiocarrot import Carrot, Consumer

import asyncio


async def test_messages(carrot_client: Carrot) -> None:
    counter = 0
    consumer = Consumer()

    @consumer.message(name='increase')
    async def increase_message(value: int = 1) -> None:
        nonlocal counter
        counter += value

    @consumer.message(name='decrease')
    async def decrease_message(value: int = 1) -> None:
        nonlocal counter
        counter -= value

    carrot_client.setup_consumer(consumer)

    await asyncio.sleep(1)

    await carrot_client.send('increase', value=15)
    await carrot_client.send('decrease', value=3)
    await carrot_client.send('increase')
    await carrot_client.send('decrease')

    await asyncio.sleep(1)  # Wait for tasks completed

    assert counter == 12
