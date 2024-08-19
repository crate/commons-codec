# -*- coding: utf-8 -*-
# (c) 2020-2021 Andreas Motl <andreas@getkotori.org>
import logging

import pytest

from commons_codec.decode.tasmota import TasmotaSensorDecoder, TasmotaStateDecoder
from commons_codec.util.data import jd

logger = logging.getLogger(__name__)


tasmota_sensor_topic = "mqttkit-1/itest/foo/bar/tele/SENSOR"
tasmota_state_topic = "mqttkit-1/itest/foo/bar/tele/STATE"


@pytest.mark.tasmota
def test_tasmota_sonoff_sc():
    """
    Verify decoding a reading from a "Sonoff SC" device in Tasmota JSON format.

    https://kotori.readthedocs.io/en/latest/integration/tasmota.html#submit
    """

    # Submit a single measurement.
    data_in = {
        "Time": "2019-06-02T22:13:07",
        "SonoffSC": {"Temperature": 25, "Humidity": 15, "Light": 20, "Noise": 10, "AirQuality": 90},
        "TempUnit": "C",
    }

    data_out = {
        "Time": "2019-06-02T22:13:07",
        "SonoffSC.AirQuality": 90,
        "SonoffSC.Humidity": 15,
        "SonoffSC.Light": 20,
        "SonoffSC.Noise": 10,
        "SonoffSC.Temperature": 25,
    }

    assert TasmotaSensorDecoder.decode(jd(data_in)) == data_out


@pytest.mark.tasmota
def test_tasmota_ds18b20():
    """
    Verify decoding a reading from a "Wemos DS18B20" device in Tasmota JSON format.

    https://kotori.readthedocs.io/en/latest/integration/tasmota.html#submit
    """

    # Submit a single measurement.
    data_in = {"Time": "2017-02-16T10:13:52", "DS18B20": {"Temperature": 20.6}}

    # Define reference data.
    data_out = {
        "Time": "2017-02-16T10:13:52",
        "DS18B20.Temperature": 20.6,
    }

    assert TasmotaSensorDecoder.decode(jd(data_in)) == data_out


@pytest.mark.tasmota
@pytest.mark.wemos
def test_tasmota_wemos_dht22():
    """
    Verify decoding a reading from a "Wemos DHT22" device in Tasmota JSON format.
    """

    # Submit a single measurement.
    data_in = {"Time": "2017-10-05T22:39:55", "DHT22": {"Temperature": 25.4, "Humidity": 45}, "TempUnit": "C"}

    # Define reference data.
    data_out = {
        "Time": "2017-10-05T22:39:55",
        "DHT22.Temperature": 25.4,
        "DHT22.Humidity": 45,
    }

    assert TasmotaSensorDecoder.decode(jd(data_in)) == data_out


@pytest.mark.tasmota
@pytest.mark.wemos
def test_tasmota_wemos_multi():
    """
    Verify decoding a reading from a "Wemos multi sensor" device in Tasmota JSON format.
    """

    # Submit a single measurement.
    data_in = {
        "Time": "2017-10-05T22:39:45",
        "DS18x20": {
            "DS1": {"Type": "DS18B20", "Address": "28FF4CBFA41604C4", "Temperature": 25.37},
            "DS2": {"Type": "DS18B20", "Address": "28FF1E7FA116035D", "Temperature": 30.44},
            "DS3": {"Type": "DS18B20", "Address": "28FF1597A41604CE", "Temperature": 25.81},
        },
        "DHT22": {"Temperature": 33.2, "Humidity": 30},
        "TempUnit": "C",
    }

    # Define reference data.
    data_out = {
        "Time": "2017-10-05T22:39:45",
        "DHT22.Temperature": 33.2,
        "DHT22.Humidity": 30,
        "DS18x20.DS1.Temperature": 25.37,
        "DS18x20.DS2.Temperature": 30.44,
        "DS18x20.DS3.Temperature": 25.81,
    }

    assert TasmotaSensorDecoder.decode(jd(data_in)) == data_out


@pytest.mark.tasmota
def test_tasmota_state():
    """
    Verify decoding a "state message" in Tasmota JSON format.

    https://kotori.readthedocs.io/en/latest/integration/tasmota.html#submit
    """

    # Submit a single measurement.
    data_in = {
        "Time": "2019-06-02T22:13:07",
        "Uptime": "1T18:10:35",
        "Vcc": 3.182,
        "SleepMode": "Dynamic",
        "Sleep": 50,
        "LoadAvg": 19,
        "Wifi": {
            "AP": 1,
            "SSId": "{redacted}",
            "BSSId": "A0:F3:C1:{redacted}",
            "Channel": 1,
            "RSSI": 100,
            "LinkCount": 1,
            "Downtime": "0T00:00:07",
        },
    }

    # Define reference data.
    data_out = {
        "Time": "2019-06-02T22:13:07",
        "Device.Vcc": 3.182,
        "Device.Sleep": 50,
        "Device.LoadAvg": 19,
        "Device.Wifi.Channel": 1,
        "Device.Wifi.RSSI": 100,
        "Device.Wifi.LinkCount": 1,
    }

    assert TasmotaStateDecoder.decode(jd(data_in)) == data_out
