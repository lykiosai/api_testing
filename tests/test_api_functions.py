import os
import sys
import json
import sqlite3
import importlib.util

import pytest


# Load module from file to avoid package import issues
HERE = os.path.dirname(__file__)
MODULE_PATH = os.path.abspath(os.path.join(HERE, "..", "rapidapi_test", "test_api.py"))
spec = importlib.util.spec_from_file_location("test_api_module", MODULE_PATH)
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


def sample_candle(ts=1600000000, o=1.0, h=2.0, l=0.5, c=1.5, v=100):
    return {"timestamp": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}


def test_find_candles_list_valid():
    lst = [sample_candle(), sample_candle(1600000060)]
    assert api.find_candles(lst) is lst


def test_find_candles_nested():
    payload = {"meta": {"data": [sample_candle()]}}
    found = api.find_candles(payload)
    assert isinstance(found, list)
    assert found[0]["open"] == 1.0


def test_get_field_case_insensitive():
    c = {"Open": 10, "CLOSE": 11}
    assert api.get_field(c, ["open"]) == 10
    assert api.get_field(c, ["close"]) == 11


def test_normalize_epoch_various():
    # seconds
    assert api.normalize_epoch(1600000000) == 1600000000
    # milliseconds -> seconds
    assert api.normalize_epoch(1600000000000) == 1600000000.0
    # microseconds -> seconds
    assert pytest.approx(api.normalize_epoch(1600000000000000)) == 1600000000.0
    # nanoseconds -> seconds
    assert pytest.approx(api.normalize_epoch(1600000000000000000)) == 1600000000.0
    # invalid
    assert api.normalize_epoch("not-a-number") is None


def test_store_candles_db(tmp_path):
    db_file = tmp_path / "test_candles.db"
    rows = [("SYM", 1600000000, "2020-09-13T12:26:40Z", 1, 2, 0.5, 1.5, 100)]
    inserted = api.store_candles_db(rows, db_path=str(db_file))
    assert inserted >= 1
    # verify row exists
    conn = sqlite3.connect(str(db_file))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM candles")
    count = cur.fetchone()[0]
    conn.close()
    assert count == inserted


def test_store_candles_json(tmp_path):
    json_file = tmp_path / "test_candles.json"
    rows = [("SYM", 1600000000, "2020-09-13T12:26:40Z", 1, 2, 0.5, 1.5, 100)]
    inserted = api.store_candles_json(rows, json_path=str(json_file))
    assert inserted == 1
    with open(str(json_file), "r") as f:
        data = json.load(f)
    assert isinstance(data, list) and len(data) == 1


def test_process_and_store_integration(tmp_path, monkeypatch):
    # Prepare sample candles
    candles = [sample_candle(1600000000), sample_candle(1600000060)]

    # Redirect DB and JSON stores to tmp files
    orig_db = api.store_candles_db
    orig_json = api.store_candles_json

    def db_wrapper(rows):
        return orig_db(rows, db_path=str(tmp_path / "proc.db"))

    def json_wrapper(rows):
        return orig_json(rows, json_path=str(tmp_path / "proc.json"))

    monkeypatch.setattr(api, "store_candles_db", db_wrapper)
    monkeypatch.setattr(api, "store_candles_json", json_wrapper)
    monkeypatch.setattr(api, "store_candles_postgres", lambda rows: 0)

    # Run
    api.process_and_store(candles, "TESTSYM")

    # Check outputs
    assert (tmp_path / "proc.db").exists()
    assert (tmp_path / "proc.json").exists()
