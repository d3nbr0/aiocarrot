# This example shows an example of the code for sending data to a queue using FastAPI


from fastapi import FastAPI, Body
from aiocarrot import Carrot


app = FastAPI()
carrot = Carrot(url='amqp://guest:guest@localhost:5672/', queue_name='users')


@app.post('/signup')
async def register_user_view(user_id: int = Body(embed=True)):
    """ An example of an API method for creating a new user """

    await carrot.send('register_user', user_id=user_id)

    return {'status': True}


@app.post('/reward')
async def reward_view(user_id: int = Body(embed=True), amount: int = Body(embed=True)):
    """
    An example of an API method for receiving a reward in
    the amount of the number of points passed in the request
    """

    await carrot.send('add_points', user_id=user_id, amount=amount)

    return {'status': True}
