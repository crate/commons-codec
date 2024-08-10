import typing as t

import jmespath
import jq
import transon

from zyp.model.bucket import MokshaTransformer, TransonTemplate


def compile_expression(type: str, expression: t.Union[str, TransonTemplate]) -> MokshaTransformer:  # noqa: A002
    if type == "jmes":
        return jmespath.compile(expression)
    elif type == "jq":
        return jq.compile(expression)
    elif type == "transon":
        return transon.Transformer(expression)
    else:
        raise TypeError(f"Compilation failed. Type must be either jmes or jq or transon: {type}")
