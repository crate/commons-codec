import logging
import typing as t

import jsonpointer
from jsonpointer import JsonPointer, JsonPointerException

logger = logging.getLogger(__name__)

not_found = object()


def swap_node(
    pointer: JsonPointer, value: t.Any, fun: t.Callable = None, on_error: t.Literal["ignore", "raise"] = "ignore"
) -> t.Union[JsonPointer, None]:
    node = pointer.resolve(value, not_found)
    if node is not_found:
        msg = f"Element not found: {pointer}"
        logger.debug(msg)
        if on_error == "raise":
            raise JsonPointerException(msg)
        return value
    if fun is not None:
        node = fun(node)
    inplace = bool(pointer.parts)
    return pointer.set(value, node, inplace=inplace)


def to_pointer(pointer: t.Union[str, JsonPointer]) -> JsonPointer:
    if isinstance(pointer, str):
        try:
            return jsonpointer.JsonPointer(pointer)
        except JsonPointerException as ex:
            raise ValueError(ex) from ex
    elif isinstance(pointer, JsonPointer):
        return pointer
    else:
        raise TypeError(f"Value is not of type str or JsonPointer: {type(pointer).__name__}")
