from typing import TYPE_CHECKING
from datetime import datetime, timezone

from loguru import logger

from croniter import croniter

if TYPE_CHECKING:
    from ..carrot import Carrot
    from ..consumer.types import Message


class Task:
    """ The basic class of the task for the scheduler """

    _carrot: 'Carrot'
    _message: 'Message'
    _cron: croniter
    _next_run: datetime
    _resync_at_next_tick: bool = False

    def __init__(self, carrot: 'Carrot', message: 'Message') -> None:
        self._carrot = carrot
        self._message = message
        self._cron = croniter(message.schedule, datetime.now(timezone.utc))
        self._sync()

    async def run(self):
        logger.info(f'Scheduled task <{self._message.name}> has been queued')
        await self._carrot.send(self._message.name)
        self._resync_at_next_tick = True

    def is_due(self, now: datetime) -> bool:
        if self._resync_at_next_tick:
            self._sync()

        return now >= self._next_run

    @property
    def next_run(self) -> datetime:
        return self._next_run

    def _sync(self) -> None:
        self._next_run = self._cron.get_next(datetime)
        self._resync_at_next_tick = False


__all__ = (
    'Task',
)
