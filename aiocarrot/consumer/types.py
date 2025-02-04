from typing import Optional, Callable, NamedTuple, TypeVar, Annotated
from dataclasses import dataclass

from pydantic import TypeAdapter, ValidationError
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined as Undefined


T = TypeVar('T')


@dataclass
class Field:
    name: str
    field_info: FieldInfo

    @property
    def required(self) -> bool:
        return self.field_info.is_required()

    @property
    def default(self) -> any:
        if self.field_info.is_required():
            return Undefined
        return self.field_info.get_default(call_default_factory=True)

    @property
    def type(self) -> any:
        return self.field_info.annotation

    def __post_init__(self) -> None:
        self._type_adapter: TypeAdapter[any] = TypeAdapter(
            Annotated[self.field_info.annotation, self.field_info],
        )

    def validate(self, value: any) -> tuple[Optional[T], Optional[str]]:
        try:
            return self._type_adapter.validate_python(value, from_attributes=True), None
        except ValidationError as exc:
            return None, exc.errors()


class Dependant:
    params: list[Field]

    def __init__(self, *, params: list[Field] = None):
        self.params = params or []


class Message(NamedTuple):
    name: Optional[str]
    handler: Callable
    dependant: Dependant
    schedule: Optional[str] = None


__all__ = (
    'Field',
    'Dependant',
    'Message',
)
