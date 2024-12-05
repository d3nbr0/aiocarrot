from ..exceptions import MessageExistsError

from .types import Message
from .utils import get_dependant

from loguru import logger

from abc import ABC
from typing import TYPE_CHECKING
from copy import deepcopy

if TYPE_CHECKING:
    from typing import Callable


class AbstractConsumer(ABC):
    _messages: dict[str, Message]

    def create_message(self, name: str, handler: 'Callable') -> None:
        """
        Store message handler in the consumer

        :param name: Message name
        :param handler: Handler function
        :return:
        """
        raise NotImplementedError

    def message(self, name: str) -> 'Callable':
        """
        Create a new message handler

        :param name:
        :return:
        """
        raise NotImplementedError

    def on_message(self, _cid: str, _cnm: str, **kwargs) -> None:
        """
        Received message event handler

        :param _cid: Message identifier
        :param _cnm: Message name
        :return:
        """
        raise NotImplementedError


class Consumer(AbstractConsumer):
    _messages: dict[str, Message]

    def __init__(self) -> None:
        self._messages = {}

    def create_message(self, name: str, handler: 'Callable') -> None:
        split_name = name.split()

        if len(split_name) != 1:
            raise AttributeError(f'Route name must be single word (found: {name})')

        message = Message(
            name=name,
            handler=handler,
            dependant=get_dependant(handler=handler),
        )

        self._messages[name] = message

    def message(self, name: str | list[str]) -> 'Callable':
        def decorator(func: 'Callable') -> 'Callable':
            if not isinstance(name, list):
                message_names = [name]
            else:
                message_names = name

            for message_name in message_names:
                self.create_message(name=message_name, handler=func)

            return func

        return decorator

    async def on_message(self, _cid: str, _cnm: str, **kwargs) -> None:
        message = self._messages.get(_cnm)

        if not message:
            logger.error(f'[{_cid}] Received unknown message <{_cnm}>: {kwargs}')
            return

        logger.info(f'[{_cid}] Received message <{_cnm}>: {kwargs}')

        values = {}

        for field in message.dependant.params:
            if field.name not in kwargs:
                if field.required:
                    logger.error(f'[{_cid}] Error processing the message <{_cnm}>: field "{field.name}" is required')
                    return
                else:
                    values[field.name] = deepcopy(field.default)

                continue

            value, errors = field.validate(kwargs[field.name])

            if errors is not None:
                logger.error(
                    f'[{_cid}] Error processing the message <{_cnm}>: field "{field.name}" with value "{kwargs[field.name]}" '
                    f'does not meet the validation conditions - {str(errors)}',
                )

                return

            values[field.name] = value

        try:
            await message.handler(**values)
        except BaseException as e:
            logger.error(f'[{_cid}] Message processing failed <{_cnm}>: {str(e)}')
            return

        logger.info(f'[{_cid}] Message <{_cnm}> processed successfully')

    def include_consumer(self, consumer: 'Consumer') -> None:
        """
        Include messages from the transmitted consumer into the current one

        :param consumer: Consumer object
        :return:
        """

        common_keys = list(set(self._messages.keys()) & set(consumer._messages.keys()))

        if len(common_keys) > 0:
            raise MessageExistsError(f'Error when trying to add a duplicate message: {common_keys[0]}')

        self._messages |= consumer._messages


__all__ = (
    'Consumer',
)
