# -*- coding: utf-8 -*-
# (c) 2022 Andreas Motl <andreas@getkotori.org>
"""
Test the TTS-/TTN-webhook receiver implementation.
TTS/TTN means "The Things Stack" / "The Things Network".

Usage
=====
::

    source .venv/bin/activate
    pytest -m ttn

References
==========
- https://www.thethingsindustries.com/docs/the-things-stack/concepts/data-formats/#uplink-messages
- https://www.thethingsindustries.com/docs/integrations/webhooks/
- https://community.hiveeyes.org/t/more-data-acquisition-payload-formats-for-kotori/1421
- https://community.hiveeyes.org/t/tts-ttn-daten-an-kotori-weiterleiten/1422/34
"""

import logging

from commons_codec.decode.tts_ttn import TheThingsStackDecoder
from commons_codec.util.data import jd
from commons_codec.util.io import read_jsonfile

logger = logging.getLogger(__name__)


def test_decode_tts_ttn_full(tts_ttn_full):
    """
    Verify decoding a full message in TTS/TTN webhook JSON format.

    https://kotori.readthedocs.io/en/latest/integration/tts-ttn.html
    """

    data_in = read_jsonfile(tts_ttn_full)
    data_out = {
        "device_id": "foo-bar-baz",
        "timestamp": "2022-01-19T19:02:34.007345025Z",
        "analog_in_1": 59.04,
        "analog_in_2": 58.69,
        "analog_in_3": 3.49,
        "relative_humidity_2": 78.5,
        "temperature_2": 4.2,
        "temperature_3": 3.4,
        "bw": 125.0,
        "counter": 2289,
        "freq": 868.5,
        "sf": 7,
        "gtw_count": 2,
        "gw_elsewhere-ffp_rssi": -90,
        "gw_elsewhere-ffp_snr": 7,
        "gw_somewhere-ffp_rssi": -107,
        "gw_somewhere-ffp_snr": -6.5,
    }

    assert TheThingsStackDecoder.decode(jd(data_in)) == data_out


def test_decode_tts_ttn_minimal(tts_ttn_minimal):
    """
    Verify decoding a minimal message in TTS/TTN webhook JSON format.

    https://kotori.readthedocs.io/en/latest/integration/tts-ttn.html
    """

    data_in = read_jsonfile(tts_ttn_minimal)
    data_out = {"temperature_1": 53.3, "voltage_4": 3.3}

    outcome = TheThingsStackDecoder.decode(jd(data_in))
    assert outcome == data_out
