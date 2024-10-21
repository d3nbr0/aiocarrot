from pydantic.fields import FieldInfo

from .types import Dependant, Field

from typing import TYPE_CHECKING

import inspect

if TYPE_CHECKING:
    from typing import Callable


def get_param_field(name: str, param: inspect.Parameter) -> Field:
    if not param.annotation:
        raise ValueError(f'The parameter "{name}" does not have a type annotation')

    kwargs = {'annotation': param.annotation}

    if param.default != param.empty:
        kwargs['default'] = param.default

    field_info = FieldInfo(**kwargs)

    return Field(
        name=name,
        field_info=field_info,
    )


def get_dependant(*, handler: 'Callable') -> Dependant:
    signature = inspect.signature(handler)
    signature_params = signature.parameters

    dependant = Dependant()

    for name, param in signature_params.items():
        field = get_param_field(name, param)
        dependant.params.append(field)

    return dependant


def none_param_sort(key: str) -> callable:
    def inner(element: any) -> None:
        return getattr(element, key) or ''

    return inner
