from aiocarrot import Carrot, Consumer

from contextlib import asynccontextmanager

import pytest, asyncio


@asynccontextmanager
async def get_test_client(carrot_configuration: dict[str, str]) -> Carrot:
    """ Returns the Carrot test client """

    carrot = Carrot(**carrot_configuration)

    async def run_carrot_loop() -> None:
        nonlocal carrot

        carrot.setup_consumer(Consumer())

        await carrot.run()

    task = asyncio.create_task(run_carrot_loop())

    yield carrot

    task.cancel()

    while not task.cancelled():
        await asyncio.sleep(0.1)


@pytest.fixture
def carrot_configuration() -> dict[str, str]:
    """ Standard configuration for connecting to RabbitMQ """

    return {
        'url': 'amqp://guest:guest@localhost:5672/',
        'queue_name': 'aiocarrot/test',
    }


@pytest.fixture
async def carrot_client(carrot_configuration: dict[str, str]) -> Carrot:
    """ Carrot test client for working with RabbitMQ """

    async with get_test_client(carrot_configuration) as carrot:
        yield carrot
