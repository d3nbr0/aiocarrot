from aiocarrot import Carrot, Consumer

from pydantic import BaseModel

from typing import Optional

import asyncio


async def test_validation(carrot_client: Carrot) -> None:
    success_counts = 0
    consumer = Consumer()

    class SampleModel(BaseModel):
        number: int
        optional_number: Optional[int] = None

    @consumer.message(name='full_model')
    async def full_model_message(model: SampleModel) -> None:
        nonlocal success_counts

        if model.number != 10:
            raise ValueError('<number> must be 10')

        if model.optional_number != 20:
            raise ValueError('<optional_number> must be 20')

        success_counts += 1

    @consumer.message(name='empty_model')
    async def empty_model_message(model: SampleModel) -> None:
        nonlocal success_counts

        if model.number != 12:
            raise ValueError('<number> must be 12')

        if model.optional_number is not None:
            raise ValueError('<optional_number> must be None')

        success_counts += 1

    @consumer.message(name='int_variables')
    async def int_variables_message(number: int, optional_number: Optional[int] = None) -> None:
        nonlocal success_counts

        if number != 15:
            raise ValueError('<number> must be 15')

        if optional_number is not None and optional_number != 0:
            raise ValueError('<optional_number> must be empty or 0')

        success_counts += 1

    @consumer.message(name='boolean_variables')
    async def boolean_variables_message(value: bool) -> None:
        nonlocal success_counts

        if value is True:
            raise ValueError('<value> must be False')

        success_counts += 1

    carrot_client.setup_consumer(consumer)

    await asyncio.sleep(1)

    await carrot_client.send('full_model', model=SampleModel(number=10, optional_number=20))
    await carrot_client.send('empty_model', model=SampleModel(number=12))
    await carrot_client.send('int_variables', number=15, optional_number=0)
    await carrot_client.send('int_variables', number=15, optional_number=None)
    await carrot_client.send('int_variables', number=15)
    await carrot_client.send('boolean_variables', value=False)
    await carrot_client.send('boolean_variables', value=True)
    await carrot_client.send('boolean_variables', value=None)
    await carrot_client.send('boolean_variables', value=254)

    await asyncio.sleep(1)  # Wait for tasks completed

    assert success_counts == 6
