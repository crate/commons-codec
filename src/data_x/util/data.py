# Copyright (c) 2016-2024, The Kotori Developers and contributors.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import json
import typing as t


def jd(data: t.Any) -> str:
    return json.dumps(data)


def is_number(s):
    """
    Check string for being a numeric value.
    http://pythoncentral.io/how-to-check-if-a-string-is-a-number-in-python-including-unicode/

    From `kotori.io.protocol.util`.
    """
    try:
        float(s)
        return True
    except (TypeError, ValueError):
        pass

    try:
        import unicodedata

        if isinstance(s, str):
            return all(map(unicodedata.numeric, s))
    except (TypeError, ValueError):
        pass

    return False
