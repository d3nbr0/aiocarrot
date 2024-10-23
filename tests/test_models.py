from aiocarrot import Carrot, Consumer

from pydantic import BaseModel

import asyncio


class CaseModel(BaseModel):
    text: str
    value: int
    flag: bool = False


async def test_models(carrot_client: Carrot) -> None:
    text, value, flag = None, None, None
    consumer = Consumer()

    @consumer.message(name='test_model')
    async def increase_message(model: CaseModel) -> None:
        nonlocal text, value, flag
        text, value, flag = model.text, model.value, model.flag

    carrot_client.setup_consumer(consumer)

    await asyncio.sleep(1)

    await carrot_client.send('test_model', model=CaseModel(text='test', value=12))

    await asyncio.sleep(1)  # Wait for tasks completed

    assert text == 'test'
    assert value == 12
    assert flag is False
