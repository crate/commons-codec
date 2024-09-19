import importlib
import importlib.resources
import typing as t

import jmespath
import jq
import transon

from zyp.model.bucket import MokshaTransformer, TransonTemplate

# TODO: Is there a better way to configure jq using a custom search path
#       instead of injecting the `include` statement each time again?
jq_functions_path = importlib.resources.files("zyp")
jq_functions_import = f'include "function" {{"search": "{jq_functions_path}"}};'


def compile_expression(type: str, expression: t.Union[str, TransonTemplate]) -> MokshaTransformer:  # noqa: A002
    if type == "jmes":
        return jmespath.compile(expression)
    elif type == "jq":
        return jq.compile(f"{jq_functions_import} {expression}")
    elif type == "transon":
        return transon.Transformer(expression)
    else:
        raise TypeError(f"Compilation failed. Type must be either jmes or jq or transon: {type}")
