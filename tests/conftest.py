from pathlib import Path

import pytest


@pytest.fixture
def tts_ttn_full():
    return Path("tests/assets/tts_ttn_full.json")


@pytest.fixture
def tts_ttn_minimal():
    return Path("tests/assets/tts_ttn_minimal.json")
