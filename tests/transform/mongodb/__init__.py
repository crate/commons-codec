import pytest

pytest.importorskip("bson", reason="Skipping MongoDB/BSON tests because 'bson' package is not installed")
