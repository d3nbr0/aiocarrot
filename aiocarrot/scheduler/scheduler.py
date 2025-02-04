import asyncio

from typing import TYPE_CHECKING
from datetime import datetime, timezone

from loguru import logger

from .types import Task

if TYPE_CHECKING:
    from ..carrot import Carrot
    from ..consumer.types import Message


class Scheduler:
    """ Common Carrot task scheduler """

    _carrot: 'Carrot'
    _tasks: list[Task] = None
    _is_alive: bool = False
    _max_interval: int = 10
    _adjust: float = 0.001

    def __init__(self, carrot: 'Carrot') -> None:
        """ The basic and basic task scheduler for the aiocarrot framework """

        self._carrot = carrot
        self._tasks = []

    def add_task(self, message: 'Message') -> None:
        if not message.schedule:
            raise AttributeError(f'Message <{message.name}> has no schedule attribute')

        try:
            task = Task(message=message, carrot=self._carrot)
        except (ValueError, TypeError):
            return logger.error(f'Issue cron format error for message <{message.name}>')

        self._tasks.append(task)

    async def start(self) -> None:
        if self._is_alive:
            return logger.error('Scheduler already running')

        if len(self._tasks) == 0:
            return logger.warning('Scheduler cannot be started because there are no scheduled tasks')

        logger.info(f'Scheduler registered {len(self._tasks)} messages')
        self._is_alive = True

        while self._is_alive:
            next_task_at = self._get_next_time()
            now = datetime.now(timezone.utc)

            delta = (next_task_at - now).total_seconds()
            if delta < 0:
                delta = self._max_interval

            interval = min(delta, self._max_interval) + self._adjust

            await asyncio.sleep(interval)

            now = datetime.now(timezone.utc)

            for task in self._tasks:
                if not task.is_due(now):
                    continue

                await task.run()

    async def reload(self) -> None:
        logger.info(f'Reloading scheduler after changing consumer...')

        await asyncio.sleep(5)
        await self.start()

    def stop(self) -> None:
        self._is_alive = False

    def clear(self) -> None:
        self._tasks = []

    @property
    def has_tasks(self) -> bool:
        return self._tasks and len(self._tasks) > 0

    def _get_next_time(self) -> datetime:
        return min(self._tasks, key=lambda x: x.next_run).next_run


__all__ = (
    'Scheduler',
)
