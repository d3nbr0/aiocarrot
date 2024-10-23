# aiocarrot

**aiocarrot** is a fully asynchronous framework for working with the <a href="https://www.rabbitmq.com/">RabbitMQ</a> message broker

<a href="https://github.com/d3nbr0/aiocarrot/actions?query=branch%3Amain+event%3Apush">
    <img src="https://github.com/d3nbr0/aiocarrot/workflows/tests/badge.svg" alt="Tests">
</a>

<a href="https://pypi.org/project/aiocarrot">
    <img src="https://img.shields.io/pypi/v/aiocarrot?color=%2334D058&label=pypi%20package" alt="Version">
</a>

___

**Source Code: https://github.com/d3nbr0/aiocarrot**

___

The key features are:

* **Completely asynchronous** - aiocarrot has the <a href="https://pypi.org/project/aio-pika/">aiopika</a> library running under the hood
* **Fast to code** - the framework allows you to reduce the amount of code in your project, as well as speed up its development
* **Fields validation** - aiocarrot supports field validation using <a href="https://pypi.org/project/pydantic/">pydantic</a>

## Requirements

The following dependencies are required for **aiocarrot** to work:

* <a href="https://pypi.org/project/aio-pika/">aio-pika</a> for working with RabbitMQ
* <a href="https://pypi.org/project/pydantic/">pydantic</a> for fields validation
* <a href="https://pypi.org/project/ujson/">ujson</a> for sending and receiving messages
* <a href="https://pypi.org/project/loguru/">loguru</a> for logging :)

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

**You can find more examples <a href="https://github.com/d3nbr0/aiocarrot/tree/main/examples">here</a>**

It's very simple to use. Enjoy!
