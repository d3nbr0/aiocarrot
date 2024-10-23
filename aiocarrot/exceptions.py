class CarrotError(Exception):
    pass


class MessageExistsError(CarrotError):
    pass


__all__ = (
    'CarrotError',
    'MessageExistsError',
)
