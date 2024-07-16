# Copyright (c) 2016-2024, The Kotori Developers and contributors.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import json
import typing as t
from pathlib import Path


def read_jsonfile(name: t.Union[str, Path]) -> t.Dict[str, t.Any]:
    return json.loads(Path(name).read_text())
