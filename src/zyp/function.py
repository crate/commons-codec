import datetime as dt
import logging
import typing as t

logger = logging.getLogger(__name__)


def to_datetime(value: t.Any, on_error: t.Literal["raise", "ignore"] = "ignore") -> t.Union[dt.datetime, None]:
    if isinstance(value, dt.datetime):
        return value
    import dateutil.parser

    try:
        return dateutil.parser.parse(value)
    except (TypeError, dateutil.parser.ParserError) as ex:
        logger.warning(f"Parsing value into datetime failed: {value}. Reason: {ex}")
        if on_error == "ignore":
            return None
        elif on_error == "raise":
            raise


def to_unixtime(value: t.Any, on_error: t.Literal["raise", "ignore"] = "ignore") -> t.Union[float, None]:
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    if value is not None and not isinstance(value, dt.datetime):
        value = to_datetime(value, on_error=on_error)
    if value is None:
        if on_error == "ignore":
            return None
        elif on_error == "raise":
            raise ValueError(f"Converting value to unixtime failed: {value}")
    return value.timestamp()
