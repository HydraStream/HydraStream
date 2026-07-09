from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar, dataclass_transform, overload

_T = TypeVar("_T")


# 2. Возвращаем декоратор-функцию, чтобы @dataclass_transform снова заработал в IDE!
@dataclass_transform(kw_only_default=True, order_default=False)
@overload
def hydra_dataclass(cls: type[_T], /) -> type[_T]: ...  # noqa: UP047


@dataclass_transform(kw_only_default=True, order_default=False)
@overload
def hydra_dataclass(
    *,
    init: bool = True,
    repr: bool = True,
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    match_args: bool = True,
    kw_only: bool = True,
    slots: bool = True,
    weakref_slot: bool = False,
) -> Callable[[type[_T]], type[_T]]: ...


# Реализация функции (внутри нее мы можем использовать чистый 3.12 синтаксис)
def hydra_dataclass(cls: Any = None, /, **kwargs: Any) -> Any:
    params = {"kw_only": True, "slots": True}
    params.update(kwargs)

    def wrap[ObjType](obj: type[ObjType]) -> type[ObjType]:
        return dataclass(**params)(obj)

    if cls is None:
        return wrap
    return wrap(cls)
