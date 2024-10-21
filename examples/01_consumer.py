# This is an example of a consumer whose task is to create new users and add points to their balance
#
# In a real project, you can use your own database instead of a dictionary.
# For example, you can use SQLAlchemy ORM to work with SQL databases.


from aiocarrot import Carrot, Consumer


consumer = Consumer()
users: dict[int, int] = {}


@consumer.message(name='register_user')
async def register_user_message(user_id: int) -> None:
    """
    Example of a message handler for registering a new user

    :param user_id: User identifier (this field is required and must be of the int type)
    :return:
    """

    if user_id in users:
        raise ValueError('This user is already registered')

    users[user_id] = 0


@consumer.message(name='add_points')
async def add_points_message(user_id: int, amount: int = None) -> None:
    """
    Adds the number of points to the user

    :param user_id: User identifier
    :param amount: The number of points to be added
    :return:
    """

    if user_id not in users:
        raise ValueError('This user is not registered')

    if amount is None:
        amount = 100

    users[user_id] += amount


async def main() -> None:
    """ Entrypoint of example application """

    carrot = Carrot(url='amqp://guest:guest@localhost:5672/', queue_name='users')
    carrot.setup_consumer(consumer)

    await carrot.run()


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
