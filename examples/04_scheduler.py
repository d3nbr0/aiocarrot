# This is an example of creating tasks that are executed by the scheduler

from aiocarrot import Carrot, Consumer


consumer = Consumer()
sample_value = 0


@consumer.message(name='increase_value', schedule='* * * * *')
async def increase_value_message() -> None:
    """ Example of a message to increase the number """

    global sample_value
    sample_value += 1


async def main() -> None:
    """ Entrypoint of example application """

    carrot = Carrot(url='amqp://guest:guest@localhost:5672/', queue_name='scheduler')
    carrot.setup_consumer(consumer)

    await carrot.run()


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
