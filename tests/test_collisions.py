from aiocarrot import Consumer
from aiocarrot.exceptions import MessageExistsError


def test_collision() -> None:
    primary_consumer = Consumer()

    @primary_consumer.message(name='unique')
    async def unique_message() -> None:
        return

    @primary_consumer.message(name='wrong')
    async def wrong_message() -> None:
        return

    test_consumer = Consumer()

    @test_consumer.message(name='wrong')
    async def wrong_duplicate_message() -> None:
        return

    try:
        primary_consumer.include_consumer(test_consumer)
    except MessageExistsError:
        pass
    else:
        assert False, 'Collision not detected.'
