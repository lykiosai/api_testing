"""
RapidAPI test script for InsightsEntry OHLC data.
"""
from dotenv import load_dotenv
import os
import requests
import json
import sys
import sqlite3
import time
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def pretty_print(obj):
    print(json.dumps(obj, indent=2, sort_keys=True))


def print_latest_candles_from_payload(payload, n=2):
    """Find candles in payload and print the latest `n` entries (UTC-normalized)."""
    candles = find_candles(payload)
    if not candles:
        print("No candle list found in payload.")
        return
    latest = candles[-n:]
    print(f"\nSample latest {len(latest)} candles (UTC):")
    for i, c in enumerate(latest, 1):
        ts = get_field(c, ["timestamp", "time", "t"]) or None
        tsec = normalize_epoch(ts)
        utc = "-"
        try:
            if tsec is not None:
                utc = datetime.fromtimestamp(tsec, timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            utc = "-"
        o = get_field(c, ["open", "o"]) or "-"
        h = get_field(c, ["high", "h"]) or "-"
        l = get_field(c, ["low", "l"]) or "-"
        cl = get_field(c, ["close", "c"]) or "-"
        v = get_field(c, ["volume", "v"]) or "-"
        print(f"{i:2}. {utc} | open: {o} | high: {h} | low: {l} | close: {cl} | volume: {v}")


def find_candles(obj):

    required = {"timestamp", "time", "t", "open", "high", "low", "close", "volume", "v"}

    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            # check if first item contains at least two of the required keys
            keys = {k.lower() for k in obj[0].keys()}
            if any(k in keys for k in ("open", "high", "low", "close")) and any(k in keys for k in ("timestamp","time","t")):
                return obj

    if isinstance(obj, dict):
        for v in obj.values():
            found = find_candles(v)
            if found:
                return found

    return None


def get_field(candle, names):
    """Return the first matching field value from `names` (case-insensitive)."""
    if not isinstance(candle, dict):
        return None
    lower_map = {k.lower(): v for k, v in candle.items()}
    for n in names:
        if n.lower() in lower_map:
            return lower_map[n.lower()]
    return None


def normalize_epoch(t):
    """Normalize an epoch-like value to seconds (float).

    Handles seconds, milliseconds, microseconds, and nanoseconds heuristically.
    Returns None for invalid inputs.
    """
    if t is None:
        return None
    try:
        val = float(t)
    except Exception:
        return None

    # Heuristic thresholds
    if val > 1e18:
        return val / 1e9
    if val > 1e15:
        return val / 1e6
    if val > 1e12:
        return val / 1e3
    return val


def store_candles_db(rows, db_path=None):
    """Store list of rows into SQLite DB. Rows: (symbol, epoch, utc, open, high, low, close, volume)"""
    if db_path is None:
        db_path = os.path.join(os.path.dirname(__file__), "rapidapi_candles.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            epoch REAL,
            utc TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            UNIQUE(symbol, epoch)
        )
        """
    )
    cur.executemany(
        "INSERT OR IGNORE INTO candles (symbol, epoch, utc, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    inserted = conn.total_changes
    conn.close()
    return inserted


def store_candles_postgres(rows):
    """
    Store rows into Postgres. Rows: (symbol, epoch, utc, open, high, low, close, volume)
    Uses env vars PG_USER/PG_PASSWORD/PG_HOST/PG_PORT/PG_DB.
    Returns number of attempted inserts (approx).
    """
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DB")
    if not all([user, password, db]):
        print("Postgres env vars missing; skipping Postgres store.")
        return 0

    dsn = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(dsn, pool_pre_ping=True)

    create_sql = """
    CREATE TABLE IF NOT EXISTS candles (
        id SERIAL PRIMARY KEY,
        symbol TEXT NOT NULL,
        epoch DOUBLE PRECISION NOT NULL,
        utc TEXT,
        open DOUBLE PRECISION,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        close DOUBLE PRECISION,
        volume DOUBLE PRECISION,
        UNIQUE (symbol, epoch)
    );
    """

    insert_sql = """
    INSERT INTO candles (symbol, epoch, utc, open, high, low, close, volume)
    VALUES (:symbol, :epoch, :utc, :open, :high, :low, :close, :volume)
    ON CONFLICT (symbol, epoch) DO NOTHING
    """

    try:
        with engine.begin() as conn:
            conn.execute(text(create_sql))
            # Deduplicate by (symbol, epoch) to avoid double-counting
            unique = {}
            for r in rows:
                try:
                    epoch_val = float(r[1])
                except Exception:
                    continue
                key = (r[0], epoch_val)
                if key in unique:
                    continue
                unique[key] = {
                    "symbol": r[0],
                    "epoch": epoch_val,
                    "utc": r[2],
                    "open": r[3],
                    "high": r[4],
                    "low": r[5],
                    "close": r[6],
                    "volume": r[7],
                }

            params = list(unique.values())
            if not params:
                return 0

            # Build a single bulk INSERT with RETURNING id so we know exactly which rows were inserted.
            value_placeholders = []
            flat = {}
            for i, p in enumerate(params):
                ph = f"(:symbol{i}, :epoch{i}, :utc{i}, :open{i}, :high{i}, :low{i}, :close{i}, :volume{i})"
                ph = ph.format(i=i)
                value_placeholders.append(ph)
                flat[f"symbol{i}"] = p["symbol"]
                flat[f"epoch{i}"] = p["epoch"]
                flat[f"utc{i}"] = p["utc"]
                flat[f"open{i}"] = p["open"]
                flat[f"high{i}"] = p["high"]
                flat[f"low{i}"] = p["low"]
                flat[f"close{i}"] = p["close"]
                flat[f"volume{i}"] = p["volume"]

            values_sql = ", ".join(value_placeholders)
            bulk_sql = (
                "INSERT INTO candles (symbol, epoch, utc, open, high, low, close, volume) VALUES "
                + values_sql
                + " ON CONFLICT (symbol, epoch) DO NOTHING RETURNING id"
            )

            try:
                result = conn.execute(text(bulk_sql), flat)
                returned = result.fetchall()
                inserted_count = len(returned)
            except Exception as e:
                # Fallback to previous executemany approach if bulk INSERT fails
                try:
                    conn.execute(text(insert_sql), params)
                    inserted_count = len(params)
                except Exception:
                    print("Postgres insert error:", e)
                    return 0

            # Ensure sequence is at least max(id)
            try:
                max_id = conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM candles")).scalar() or 0
                seq = conn.execute(text("SELECT pg_get_serial_sequence('candles','id')")).scalar()
                if seq and max_id:
                    conn.execute(text(f"SELECT setval(:seq, :val, true)"), {"seq": seq, "val": int(max_id)})
            except Exception:
                pass

            return inserted_count
    except SQLAlchemyError as e:
        print("Postgres write error:", e)
    return 0


def store_candles_json(rows, json_path=None):
    """Append new rows to a JSON file, avoiding duplicates by (symbol, epoch)."""
    if json_path is None:
        json_path = os.path.join(os.path.dirname(__file__), "rapidapi_candles.json")
    try:
        if os.path.exists(json_path):
            with open(json_path, "r") as f:
                data = json.load(f)
            if not isinstance(data, list):
                data = []
        else:
            data = []

        existing = {(it.get("symbol"), float(it.get("epoch"))) for it in data if it.get("symbol") is not None and it.get("epoch") is not None}
        new_items = []
        for r in rows:
            key = (r[0], float(r[1]))
            if key in existing:
                continue
            item = {
                "symbol": r[0],
                "epoch": r[1],
                "utc": r[2],
                "open": r[3],
                "high": r[4],
                "low": r[5],
                "close": r[6],
                "volume": r[7],
            }
            data.append(item)
            new_items.append(item)
            existing.add(key)

        with open(json_path, "w") as f:
            json.dump(data, f, indent=2)

        return len(new_items)
    except Exception as e:
        print(f"Failed to write JSON store: {e}")
        return 0


def process_and_store(candles, symbol):
    # Normalize and prepare rows
    rows = []
    times = []
    for c in candles:
        t = get_field(c, ["timestamp", "time", "t"]) or None
        tsec = normalize_epoch(t)
        if tsec is None:
            continue
        utc = datetime.fromtimestamp(tsec, timezone.utc).isoformat().replace("+00:00", "Z")
        o = get_field(c, ["open", "o"]) or None
        h = get_field(c, ["high", "h"]) or None
        l = get_field(c, ["low", "l"]) or None
        cl = get_field(c, ["close", "c"]) or None
        v = get_field(c, ["volume", "v"]) or None
        rows.append((symbol, tsec, utc, o, h, l, cl, v))
        times.append(tsec)

    # Print latest up to 20
    latest = rows[-20:]
    print(f"\nSample latest {len(latest)} candles (UTC):")
    for i, r in enumerate(latest, 1):
        print(f"{i:2}. {r[2]} | open: {r[3]} | high: {r[4]} | low: {r[5]} | close: {r[6]} | volume: {r[7]}")

    if times:
        tmin, tmax = min(times), max(times)
        print(f"\nTime range (epoch): {tmin} - {tmax} | UTC: {datetime.fromtimestamp(tmin, timezone.utc).isoformat().replace('+00:00','Z')} - {datetime.fromtimestamp(tmax, timezone.utc).isoformat().replace('+00:00','Z')}")

    # Store into DB
    inserted = store_candles_db(rows)
    print(f"Inserted (or ignored duplicates) rows count: {inserted}")
    # Also store into Postgres (if configured)
    inserted_pg = store_candles_postgres(rows)
    print(f"Inserted into Postgres (approx): {inserted_pg}")
    # Also store to JSON file for quick testing / verification
    inserted_json = store_candles_json(rows)
    print(f"New JSON rows appended: {inserted_json}")


def main():
    load_dotenv()

    api_key = os.getenv("RAPIDAPI_KEY")
    api_host = os.getenv("RAPIDAPI_HOST")

    if not api_key or not api_host:
        print("Missing RAPIDAPI_KEY or RAPIDAPI_HOST in environment. Check .env file.")
        sys.exit(1)

    # Symbol to request (change as needed). Default set to the format you provided
    # for the NQ mini contract: 'CME_MINI:NQ1!'. Override via SYMBOL.
    symbol = os.getenv("SYMBOL", "CME_MINI:NQ1!")

    # First try the V3 series endpoint commonly used by InsightsEntry (example):
    # /v3/symbols/{symbol}/series
    # Default querystring follows the sample you provided; override with env vars.
    use_v3 = os.getenv("USE_V3_SERIES", "1")

    # The exact path for OHLC data may vary for InsightsEntry.
    # You can set `RAPIDAPI_PATH` in your .env to force a specific endpoint.
    env_path = os.getenv("RAPIDAPI_PATH")

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": api_host,
        "Accept": "application/json",
    }

    poll_interval = int(os.getenv("POLL_INTERVAL", "10"))
    print(f"Polling every {poll_interval} seconds. Press Ctrl-C to stop.")

    # prepare v3 params and symbol variants once
    v3_params = {
        # 5-minute candles by default
        "bar_type": os.getenv("BAR_TYPE", "minute"),
        "bar_interval": os.getenv("BAR_INTERVAL", "5"),
        "extended": os.getenv("EXTENDED", "true"),
        "badj": os.getenv("BADJ", "true"),
        "dadj": os.getenv("DADJ", "false"),
        "dp": os.getenv("DP", "3000"),
        "long_poll": os.getenv("LONG_POLL", "false"),
    }

    symbol_variants = [symbol]
    if ":" not in symbol:
        symbol_variants += [f"CME:{symbol}", f"NASDAQ:{symbol}", f"CME:MNQ", f"CME:NQ"]

    try:
        while True:
            candles_found = None

            if use_v3 == "1":
                for sym in symbol_variants:
                    v3_path = f"/v3/symbols/{sym}/series"
                    print(f"\nTrying V3 series endpoint: https://{api_host}{v3_path}")
                    try:
                        r = requests.get(f"https://{api_host}{v3_path}", headers=headers, params=v3_params, timeout=15)
                        print(f"HTTP {r.status_code} - {r.reason}")
                        try:
                            j = r.json()
                        except ValueError:
                            j = None

                        if r.status_code == 200 and j is not None:
                            print(f"Success — V3 series returned 200 for symbol {sym}")
                            print_latest_candles_from_payload(j, n=2)
                            candles = find_candles(j)
                            if candles:
                                process_and_store(candles, sym)
                                candles_found = True
                                break
                            else:
                                print("No candle list found in V3 response.")
                        else:
                            print(f"V3 series did not return usable data for symbol {sym}; trying next variant.")
                            if j:
                                pretty_print(j)
                    except requests.RequestException as e:
                        print(f"V3 series request failed for symbol {sym}: {e}")

            # If not found via v3, try fallback candidates
            if not candles_found:
                print("\nTrying fallback endpoint candidates...")
                for i, path in enumerate(paths, 1):
                    url = f"https://{api_host}{path}"
                    print(f"\nAttempt {i}/{len(paths)}: GET {url}")
                    try:
                        resp = requests.get(url, headers=headers, params={"symbol": symbol}, timeout=15)
                    except requests.RequestException as e:
                        print(f"Request error for {path}: {e}")
                        continue

                    print(f"HTTP {resp.status_code} - {resp.reason}")
                    try:
                        data = resp.json()
                    except ValueError:
                        data = None

                    if resp.status_code == 200 and data is not None:
                        print("Success — received 200 response")
                        print_latest_candles_from_payload(data, n=2)
                        candles = find_candles(data)
                        if candles:
                            process_and_store(candles, symbol)
                            candles_found = True
                            break
                        else:
                            print("No candle list found in response.")
                    else:
                        if data:
                            print("Response body:")
                            pretty_print(data)
                        else:
                            print("No JSON body returned for this endpoint.")

            if not candles_found:
                print("\nNo candles found in this cycle.")

            print(f"Sleeping {poll_interval} seconds before next poll...")
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        print("\nInterrupted by user — exiting.")
        return

    candidates = [
        env_path,
        "/ohlc",
        "/v1/ohlc",
        "/market/ohlc",
        "/marketdata/ohlc",
        "/v1/candles",
        "/candles",
        "/v1/quotes",
        "/quotes",
        "/v1/quote",
        "/quote",
        "/instruments/ohlc",
    ]

    # Remove None and duplicates while preserving order
    seen = set()
    paths = []
    for p in candidates:
        if not p:
            continue
        if p in seen:
            continue
        seen.add(p)
        paths.append(p)

    params = {"symbol": symbol}

    resp = None
    data = None

    print(f"Attempting to locate OHLC endpoint for symbol: {symbol} on host: {api_host}")

    for i, path in enumerate(paths, 1):
        url = f"https://{api_host}{path}"
        print(f"\nAttempt {i}/{len(paths)}: GET {url}")
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=15)
        except requests.RequestException as e:
            print(f"Request error for {path}: {e}")
            continue

        print(f"HTTP {resp.status_code} - {resp.reason}")

        # Try parsing JSON if any body
        try:
            data = resp.json()
        except ValueError:
            data = None

        if resp.status_code == 200:
            print("Success — received 200 response")
            break
        else:
            # helpful diagnostics for non-200
            if data:
                # avoid printing the API key; data should be safe
                print("Response body:")
                pretty_print(data)
            else:
                print("No JSON body returned for this endpoint.")

    if resp is None:
        print("No requests were successful. Exiting.")
        sys.exit(1)

    if resp.status_code != 200:
        print("\nCould not find a working OHLC endpoint. Consider providing RAPIDAPI_PATH in .env or checking the API docs.")
        if data:
            print("Last JSON response:")
            pretty_print(data)
        sys.exit(1)

    # At this point, data should be the successful JSON payload
    # Print a concise sample instead of full JSON
    print_latest_candles_from_payload(data, n=2)

    # Try to find candles in the response
    candles = find_candles(data)

    if not candles:
        print("\nNo candle list found in response. Cannot extract OHLC data automatically.")
        sys.exit(0)

    print(f"\nFound {len(candles)} candles. Showing up to first 10 (UTC normalized):")
    print(f"\nFound {len(candles)} candles. Showing latest up to 20 (UTC normalized):")
    latest = candles[-20:]
    for i, c in enumerate(latest, 1):
        ts = get_field(c, ["timestamp", "time", "t"]) or None
        o = get_field(c, ["open", "o"]) or "-"
        h = get_field(c, ["high", "h"]) or "-"
        l = get_field(c, ["low", "l"]) or "-"
        cl = get_field(c, ["close", "c"]) or "-"
        v = get_field(c, ["volume", "v"]) or "-"
        utc = "-"
        try:
            tsec = normalize_epoch(ts)
            if tsec is not None:
                utc = datetime.fromtimestamp(tsec, timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            utc = "-"

        print(f"{i:2}. utc: {utc} | timestamp: {ts} | open: {o} | high: {h} | low: {l} | close: {cl} | volume: {v}")


if __name__ == "__main__":
    main()
