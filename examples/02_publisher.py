# In this example, we are sending messages so that the code from the first example can process them

from aiocarrot import Carrot


async def main() -> None:
    """ Entrypoint of example application """

    carrot = Carrot(url='amqp://guest:guest@localhost:5672/', queue_name='users')

    await carrot.send('register_user', user_id=1)
    await carrot.send('add_points', user_id=1, amount=1000)
    await carrot.send('add_points', user_id=1)

    # If you look at the example 01_consumer.py then you will see that the amount argument
    # is optional and by default the user is awarded 100 points

if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
