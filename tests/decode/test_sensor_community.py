# Copyright (c) 2020-2024, The Kotori Developers and contributors.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import logging
import typing as t

from commons_codec.decode.sensor_community import SensorCommunity
from commons_codec.util.data import jd

logger = logging.getLogger(__name__)


data_in = {
    "esp8266id": 12041741,
    "sensordatavalues": [
        {"value_type": "SDS_P1", "value": "35.67"},
        {"value_type": "SDS_P2", "value": "17.00"},
        {"value_type": "BME280_temperature", "value": "-2.83"},
        {"value_type": "BME280_humidity", "value": "66.73"},
        {"value_type": "BME280_pressure", "value": "100535.97"},
        {"value_type": "samples", "value": "3016882"},
        {"value_type": "min_micro", "value": "77"},
        {"value_type": "max_micro", "value": "26303"},
        {"value_type": "signal", "value": "-66"},
    ],
    "software_version": "NRZ-2018-123B",
}


data_out = {
    "SDS_P1": 35.67,
    "SDS_P2": 17.00,
    "BME280_temperature": -2.83,
    "BME280_humidity": 66.73,
    "BME280_pressure": 100535.97,
    "samples": 3016882,
    "min_micro": 77,
    "max_micro": 26303,
    "signal": -66,
}


def test_decode_sensor_community():
    """
    Verify decoding a single reading in Sensor.Community JSON format.
    """

    result = SensorCommunity.decode(jd(data_in))
    assert result == data_out
    assert_type(result["SDS_P1"], float)
    assert_type(result["BME280_pressure"], float)
    assert_type(result["signal"], int)
    assert_type(result["samples"], int)
    assert_type(result["min_micro"], int)
    assert_type(result["max_micro"], int)


def assert_type(value: t.Any, type_: t.Type):
    """
    Assertion helper with better error reporting.
    """
    assert isinstance(value, type_), (
        f"Value is of type '{type(value).__name__}', but should be '{type_.__name__}' instead"
    )
