from pydantic import BaseModel

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

        logger.info('Starting aiocarrot with following configuration:')
        logger.info('')
        logger.info(f'> Queue: {self._queue_name}')
        logger.info(f'> Registered messages:')

        for message_name in self._consumer._messages.keys():
            logger.info(f'  * {message_name}')

        logger.info('')
        logger.info('Starting listener loop...')

        raise_loop_error = None

        try:
            await self._consumer_loop()
        except KeyboardInterrupt:
            pass
        except asyncio.CancelledError as e:
            raise_loop_error = e
        except BaseException:
            logger.trace('An unhandled error occurred while the consumer was working')
        finally:
            logger.info('Shutting down...')

            await self._channel.close()
            await self._connection.close()

            logger.info('Good bye!')

        if raise_loop_error is not None:
            raise raise_loop_error

    async def _consumer_loop(self) -> None:
        """
        The main event loop used by Carrot to receive new messages and pass them on to the handler

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


__all__ = (
    'Carrot',
)
