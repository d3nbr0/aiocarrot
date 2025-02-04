from aiocarrot import Carrot, Consumer

from typing import Optional

import asyncio


async def test_scheduler(carrot_client: Carrot) -> None:
    first_reply, second_reply = None, None
    consumer = Consumer()

    @consumer.message(name='test.scheduler_first', schedule='* * * * *')
    async def scheduler_first_message() -> None:
        nonlocal first_reply
        first_reply = 'success'

    @consumer.message(name='test.scheduler_second', schedule='* * * * *')
    async def scheduler_second_message(message: Optional[str] = None) -> None:
        nonlocal second_reply

        if message is None:
            message = 'success'

        second_reply = message

    carrot_client.setup_consumer(consumer)

    await asyncio.sleep(60)

    carrot_client._scheduler.stop()
    carrot_client._scheduler.clear()

    assert first_reply == 'success'
    assert second_reply == 'success'
