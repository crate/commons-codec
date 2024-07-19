# Copyright (c) 2020-2024, The Kotori Developers and contributors.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import json
from collections import OrderedDict

from commons_codec.util.data import is_number


class SensorCommunity:
    """
    Decode JSON payloads in Sensor.Community format.

    Previously / Also: Airrohr, dusti.api, Luftdaten.info

    Documentation
    =============
    - https://github.com/opendata-stuttgart/meta/wiki/APIs
    - https://kotori.readthedocs.io/en/latest/integration/airrohr.html
    - https://community.hiveeyes.org/t/more-data-acquisition-payload-formats-for-kotori/1421/2

    Example
    =======
    ::

        {
          "esp8266id": 12041741,
          "sensordatavalues": [
            {
              "value_type": "SDS_P1",
              "value": "35.67"
            },
            {
              "value_type": "SDS_P2",
              "value": "17.00"
            },
            {
              "value_type": "BME280_temperature",
              "value": "-2.83"
            },
            {
              "value_type": "BME280_humidity",
              "value": "66.73"
            },
            {
              "value_type": "BME280_pressure",
              "value": "100535.97"
            },
            {
              "value_type": "samples",
              "value": "3016882"
            },
            {
              "value_type": "min_micro",
              "value": "77"
            },
            {
              "value_type": "max_micro",
              "value": "26303"
            },
            {
              "value_type": "signal",
              "value": "-66"
            }
          ],
          "software_version": "NRZ-2018-123B"
        }

    """

    INTEGERS = [
        "signal",
        "samples",
        "min_micro",
        "max_micro",
    ]

    @classmethod
    def decode(cls, payload):
        # Decode from JSON.
        message = json.loads(payload)

        # Create data dictionary by flattening nested message.
        data = OrderedDict()
        for item in message.get("sensordatavalues", []):
            key = item["value_type"]
            value = item["value"]
            if is_number(value):
                if key in cls.INTEGERS:
                    value = int(value)
                else:
                    value = float(value)
            data[key] = value

        return data
