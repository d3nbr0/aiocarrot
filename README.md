# aiocarrot

**aiocarrot** is fully asynchronous framework for working with <a href="https://www.rabbitmq.com/">RabbitMQ</a>

Accelerate development with a message broker at times with **aiocarrot**

## Installation

Create and activate virtual environment and then install **aiocarrot**:

```commandline
pip install aiocarrot
```

## Example

Create a file `main.py` with:

```python
from aiocarrot import Carrot, Consumer

import asyncio


consumer = Consumer()


@consumer.message(name='multiply')
async def multiply(first_number: int, second_number: int) -> None:
    print('Result is:', first_number * second_number)


async def main() -> None:
    carrot = Carrot(url='amqp://guest:guest@127.0.0.1/', queue_name='sample')
    carrot.setup_consumer(consumer)
    await carrot.run()


if __name__ == '__main__':
    asyncio.run(main())
```

Then run it with:

```commandline
python main.py
```

Now you have created a consumer with the ability to receive a **"multiply"** task

### Produce message

If you want to send a message, use this:

```python
from aiocarrot import Carrot

import asyncio


async def main() -> None:
    carrot = Carrot(url='amqp://guest:guest@127.0.0.1:5672/', queue_name='sample')
    
    await carrot.send('multiply', first_number=10, second_number=20)


if __name__ == '__main__':
    asyncio.run(main())
```

It's very simple to use. Enjoy!
