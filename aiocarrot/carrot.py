from pydantic import BaseModel

from loguru import logger

from typing import Optional, TYPE_CHECKING

import asyncio, aio_pika, ujson, uuid, copy, signal

if TYPE_CHECKING:
    from aiormq.abc import ConfirmationFrameType

    from .consumer import Consumer


class Carrot:
    """ Carrot framework entrypoint class """

    _url: str
    _queue_name: str
    _tasks: list[asyncio.Task]
    _is_consumer_alive: bool = False
    _consumer: Optional['Consumer'] = None
    _connection: Optional['aio_pika.abc.AbstractConnection'] = None
    _channel: Optional['aio_pika.abc.AbstractChannel'] = None
    _queue: Optional['aio_pika.abc.AbstractQueue'] = None

    def __init__(self, url: str, queue_name: str) -> None:
        """
        aiocarrot is an asynchronous framework for working with the RabbitMQ message broker

        :param url: RabbitMQ connection url
        :param queue_name: The name of the queue for further work
        """

        self._url = url
        self._tasks = []
        self._queue_name = queue_name

    async def send(self, _cnm: str, **kwargs) -> 'ConfirmationFrameType':
        """
        Send a message with the specified type and the specified payload

        :param _cnm: The name of the message (used to determine the type of message being sent)
        :param kwargs: The payload transmitted in the message body
        :return:
        """

        channel = await self._get_channel()

        message_id = str(uuid.uuid4())
        message_body = {
            '_cid': message_id,
            '_cnm': _cnm,
            **kwargs,
        }

        message_body = {
            key: (value.model_dump() if isinstance(value, BaseModel) else value)
            for key, value in message_body.items()
        }

        payload = ujson.dumps(message_body).encode()

        return await channel.default_exchange.publish(
            message=aio_pika.Message(body=payload),
            routing_key=self._queue_name,
        )

    def setup_consumer(self, consumer: 'Consumer') -> None:
        """
        Sets the consumer as the primary one for this Carrot instance

        :param consumer: Consumer object
        :return:
        """

        self._consumer = consumer

    async def run(self) -> None:
        """
        Starts the main loop of the Carrot new message listener

        :return:
        """

        if not self._consumer:
            raise RuntimeError('Consumer is not registered. Please, specify using following method: '
                               '.setup_consumer(consumer)')

        if self._is_consumer_alive:
            raise RuntimeError('Consumer loop is already running')

        logger.info('Starting aiocarrot with following configuration:')
        logger.info('')
        logger.info(f'> Queue: {self._queue_name}')
        logger.info(f'> Registered messages:')

        for message_name in self._consumer._messages.keys():
            logger.info(f'  * {message_name}')

        logger.info('')
        logger.info('Starting listener loop...')

        signal.signal(signal.SIGINT, self._exit_signal_handler)

        self._is_consumer_alive = True
        consumer_task = asyncio.create_task(self._consumer_loop())

        try:
            while self._is_consumer_alive:
                if consumer_task.done():
                    logger.critical(
                        f'An unhandled error occurred while the consumer was working: '
                        f'{str(consumer_task.exception())}'
                    )
                    break

                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info('Shutdown signal received')

        if not consumer_task.done():
            consumer_task.cancel()

        await self._shutdown()

    async def _consumer_loop(self) -> None:
        """
        The main event loop used by Carrot to receive new messages and pass them on to the handler

        :return:
        """

        queue = await self._get_queue()

        logger.info('Consumer is successfully connected to queue')

        async with queue.iterator() as queue_iterator:
            async for message in queue_iterator:
                for task in copy.copy(self._tasks):
                    if task.done():
                        self._tasks.remove(task)

                async with message.process():
                    decoded_message: str = message.body.decode()

                    try:
                        message_payload = ujson.loads(decoded_message)

                        assert isinstance(message_payload, dict)
                    except ujson.JSONDecodeError:
                        logger.error(f'Error receiving the message (failed to receive JSON): {decoded_message}')
                        continue

                    message_id = message_payload.get('_cid')
                    message_name = message_payload.get('_cnm')

                    if not message_id:
                        logger.error(
                            'The message format could not be determined (identifier is missing): '
                            f'{message_payload}'
                        )

                        continue

                    if not message_name:
                        logger.error(
                            'The message format could not be determined (message name is missing): '
                            f'{message_payload}'
                        )

                        continue

                    del message_payload['_cid']
                    del message_payload['_cnm']

                    task = asyncio.create_task(self._consumer.on_message(
                        message_id,
                        message_name,
                        **message_payload,
                    ))

                    self._tasks.append(task)

    async def _get_queue(self) -> 'aio_pika.abc.AbstractQueue':
        """
        Retrieves the currently active aiopika queue object

        :return: aiopika queue
        """

        if not self._queue:
            channel = await self._get_channel()
            self._queue = await channel.declare_queue(self._queue_name, durable=True, auto_delete=True)

        return self._queue

    async def _get_channel(self) -> 'aio_pika.abc.AbstractChannel':
        """
        Gets the current active object of the aiopika channel

        :return: aiopika channel
        """

        if not self._channel:
            connection = await self._get_connection()
            self._channel = await connection.channel()

        return self._channel

    async def _get_connection(self) -> 'aio_pika.abc.AbstractConnection':
        """
        Retrieves the object of an active connection with the broker using aiopika

        :return: aiopika broker connection
        """

        if not self._connection:
            self._connection = await aio_pika.connect_robust(url=self._url)

        return self._connection

    async def _shutdown(self) -> None:
        """
        Shutdown carrot application

        :return:
        """

        pending_tasks = [x for x in self._tasks if not x.done()]

        if len(pending_tasks) > 0:
            logger.info(f'Waiting for {len(pending_tasks)} tasks...')
            await asyncio.gather(*pending_tasks)

        try:
            await self._channel.close()
        except:
            pass

        try:
            await self._connection.close()
        except:
            pass

        logger.info('Good bye!')

    def _exit_signal_handler(self, sig, frame) -> int:
        logger.info('Shutdown signal received')

        self._is_consumer_alive = False

        return 0


__all__ = (
    'Carrot',
)
