import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lakehouse.engine import Engine  # noqa: E402


@pytest.fixture()
def engine(tmp_path):
    eng = Engine(str(tmp_path / "data"), str(tmp_path / "meta.sqlite3"), lease_ttl=30)
    eng.create_table("events", ["us", "eu", "apac"])
    return eng


@pytest.fixture()
def tmp(tmp_path):
    return tmp_path
