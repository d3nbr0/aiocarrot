from loguru import logger

from typing import Optional, TYPE_CHECKING

import asyncio, aio_pika, ujson, uuid

if TYPE_CHECKING:
    from aiormq.abc import ConfirmationFrameType

    from .consumer import Consumer


class Carrot:
    """ Carrot framework entrypoint class """

    _url: str
    _queue_name: str
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
        self._queue_name = queue_name

    async def send(self, _cnm: str, **kwargs) -> 'ConfirmationFrameType':
        """
        Send message with specified name

        :param _cnm: Message name
        :param kwargs: Message payload
        :return:
        """

        channel = await self._get_channel()

        message_id = str(uuid.uuid4())
        message_body = {
            '_cid': message_id,
            '_cnm': _cnm,
            **kwargs,
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
        Start carrot listening

        :return:
        """

        if not self._consumer:
            raise RuntimeError('Consumer is not registered. Please, specify using following method: '
                               '.setup_consumer(consumer)')

        logger.info('Starting aiocarrot with following configuration:')
        logger.info('')
        logger.info(f'> Queue: {self._queue_name}')
        logger.info(f'> Registered messages:')

        for message_name in self._consumer._messages.keys():
            logger.info(f'  * {message_name}')

        logger.info('')
        logger.info('Starting listener loop...')

        try:
            await self._consumer_loop()
        except KeyboardInterrupt:
            pass
        except BaseException:
            logger.trace('An unhandled error occurred while the consumer was working')
        finally:
            logger.info('Shutting down...')

    async def _consumer_loop(self) -> None:
        """
        Consumer primary loop

        :return:
        """

        if self._is_consumer_alive:
            raise RuntimeError('Consumer loop is already running')

        if not self._consumer:
            raise RuntimeError('Consumer is not registered. Please, specify using following method: '
                               '.setup_consumer(consumer)')

        queue = await self._get_queue()

        logger.info('Consumer is successfully connected to queue')

        async with queue.iterator() as queue_iterator:
            self._is_consumer_alive = True

            async for message in queue_iterator:
                async with message.process():
                    decoded_message: str = message.body.decode()

                    try:
                        message_payload = ujson.loads(decoded_message)

                        assert isinstance(message_payload, dict)
                    except ujson.JSONDecodeError:
                        logger.trace(f'Error receiving the message (failed to receive JSON): {decoded_message}')
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

                    asyncio.create_task(self._consumer.on_message(
                        message_id,
                        message_name,
                        **message_payload,
                    ))

    async def _get_queue(self) -> 'aio_pika.abc.AbstractQueue':
        """
        Get active broker queue

        :return: aiopika queue
        """

        if not self._queue:
            channel = await self._get_channel()
            self._queue = await channel.declare_queue(self._queue_name, durable=True, auto_delete=True)

        return self._queue

    async def _get_channel(self) -> 'aio_pika.abc.AbstractChannel':
        """
        Get active broker channel

        :return: aiopika channel
        """

        if not self._channel:
            connection = await self._get_connection()
            self._channel = await connection.channel()

        return self._channel

    async def _get_connection(self) -> 'aio_pika.abc.AbstractConnection':
        """
        Get active connection to the broker

        :return: aiopika broker connection
        """

        if not self._connection:
            self._connection = await aio_pika.connect_robust(url=self._url)

        return self._connection


__all__ = (
    'Carrot',
)
