import requests
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import time
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.getenv('COINGECKO_DB_PATH', os.path.join(DATA_DIR, 'coingecko_top1000.db'))
DATABASE_URL = os.getenv('DATABASE_URL') or os.getenv('SUPABASE_DATABASE_URL')
API_KEY = "CG-t7FgFVU7PUeZL3nMf7Zd9hRV"
HEADERS = {"accept": "application/json", "x-cg-pro-api-key": API_KEY}
BINANCE_BASE = "https://api.binance.com"

_DB_ENGINE = None
_DB_IS_POSTGRES = None

def _normalize_database_url(url):
    if url.startswith("postgres://"):
        return "postgresql+psycopg://" + url[len("postgres://"):]
    if url.startswith("postgresql://"):
        return "postgresql+psycopg://" + url[len("postgresql://"):]
    return url

def get_db_engine():
    global _DB_ENGINE, _DB_IS_POSTGRES
    if _DB_ENGINE is None:
        if DATABASE_URL:
            db_url = _normalize_database_url(DATABASE_URL)
            _DB_ENGINE = create_engine(db_url, pool_pre_ping=True)
            _DB_IS_POSTGRES = True
        else:
            sqlite_url = f"sqlite:///{DB_PATH}"
            _DB_ENGINE = create_engine(
                sqlite_url,
                connect_args={"check_same_thread": False},
                poolclass=NullPool
            )
            _DB_IS_POSTGRES = False
    return _DB_ENGINE

def is_postgres():
    if _DB_ENGINE is None:
        get_db_engine()
    return _DB_IS_POSTGRES

def fetch_mappings(query, params=None):
    engine = get_db_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return [dict(row) for row in result.mappings().all()]

def fetch_mapping(query, params=None):
    engine = get_db_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        row = result.mappings().first()
        return dict(row) if row else None

def fetch_scalar(query, params=None):
    engine = get_db_engine()
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        return result.scalar()

def execute_write(query, params=None):
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text(query), params or {})

def execute_many(query, params_list):
    engine = get_db_engine()
    with engine.begin() as conn:
        conn.execute(text(query), params_list)

def backfill_ohlcv(symbol, start_ms=0, end_ms=None):
    pair = symbol.upper() + "USDT"
    end_ms = end_ms or int(time.time() * 1000)
    cursor = start_ms
    total = 0

    while cursor < end_ms:
        try:
            r = requests.get(
                BINANCE_BASE + "/api/v3/klines",
                params={
                    "symbol": pair,
                    "interval": "1d",
                    "startTime": cursor,
                    "endTime": end_ms,
                    "limit": 1000
                },
                timeout=10
            )
            chunk = r.json()

            if not isinstance(chunk, list) or len(chunk) == 0:
                break

            candles = [
                (
                    pair,
                    k[0],
                    dt.datetime.fromtimestamp(k[0] / 1000).strftime("%Y-%m-%d"),
                    float(k[1]),
                    float(k[2]),
                    float(k[3]),
                    float(k[4]),
                    float(k[5])
                )
                for k in chunk
            ]

            execute_many("""
                INSERT INTO ohlcv_data
                (symbol, timestamp, date, open, high, low, close, volume)
                VALUES (:symbol, :timestamp, :date, :open, :high, :low, :close, :volume)
                ON CONFLICT (symbol, timestamp) DO NOTHING
            """, [
                {
                    "symbol": c[0],
                    "timestamp": c[1],
                    "date": c[2],
                    "open": c[3],
                    "high": c[4],
                    "low": c[5],
                    "close": c[6],
                    "volume": c[7]
                }
                for c in candles
            ])

            total += len(candles)
            cursor = chunk[-1][0] + 1

            if len(chunk) < 1000:
                break
        except:
            break

    return total

def ensure_ohlcv_data(symbol):
    pair = symbol.upper() + "USDT"
    now_ms = int(time.time() * 1000)

    row = fetch_mapping(
        "SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM ohlcv_data WHERE symbol = :symbol",
        {"symbol": pair}
    )

    min_ts = row["min_ts"] if row else None
    max_ts = row["max_ts"] if row else None
    total = 0

    if min_ts is None:
        total += backfill_ohlcv(symbol, start_ms=0, end_ms=now_ms)
    else:
        if min_ts > 0:
            total += backfill_ohlcv(symbol, start_ms=0, end_ms=min_ts - 1)
        if max_ts and max_ts + 86400000 < now_ms:
            total += backfill_ohlcv(symbol, start_ms=max_ts + 1, end_ms=now_ms)

    return total



# INIT DATABASE (табели: top_coins, meta_info, ohlcv_data)
def init_db():
    if is_postgres():
        statements = [
            """
            CREATE TABLE IF NOT EXISTS top_coins (
                coin_id TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                market_cap_rank INTEGER,
                price DOUBLE PRECISION,
                market_cap DOUBLE PRECISION,
                volume_24h DOUBLE PRECISION,
                liquidity_score DOUBLE PRECISION
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS meta_info (
                id INTEGER PRIMARY KEY,
                last_top1000_update TEXT
            )
            """,
            """
            INSERT INTO meta_info (id, last_top1000_update)
            VALUES (1, NULL)
            ON CONFLICT (id) DO NOTHING
            """,
            """
            CREATE TABLE IF NOT EXISTS ohlcv_data (
                id BIGSERIAL PRIMARY KEY,
                symbol TEXT,
                timestamp BIGINT,
                date TEXT,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                UNIQUE(symbol, timestamp)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_st ON ohlcv_data(symbol, timestamp)"
        ]
    else:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS top_coins (
                coin_id TEXT PRIMARY KEY,
                symbol TEXT,
                name TEXT,
                market_cap_rank INT,
                price REAL,
                market_cap REAL,
                volume_24h REAL,
                liquidity_score REAL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS meta_info (
                id INTEGER PRIMARY KEY,
                last_top1000_update TEXT
            )
            """,
            "INSERT OR IGNORE INTO meta_info (id, last_top1000_update) VALUES (1, NULL)",
            """
            CREATE TABLE IF NOT EXISTS ohlcv_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timestamp INT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                UNIQUE(symbol, timestamp)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_st ON ohlcv_data(symbol, timestamp)"
        ]

    engine = get_db_engine()
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))



# CHECK IF WE NEED TO UPDATE TOP 1000 TODAY
def should_update_top1000():
    row = fetch_scalar("SELECT last_top1000_update FROM meta_info WHERE id = 1")

    today = dt.datetime.now().strftime("%Y-%m-%d")
    return row != today


def mark_top1000_updated():
    today = dt.datetime.now().strftime("%Y-%m-%d")
    execute_write(
        "UPDATE meta_info SET last_top1000_update = :today",
        {"today": today}
    )


# FILTER 1 — Fetch Top 1000 (Coingecko)
def filter_1_fetch_top_coins():
    print("FILTER 1: Fetch Top 1000 (Coingecko)")

    def fetch(p):
        try:
            r = requests.get(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": 250,
                    "page": p
                },
                headers=HEADERS,
                timeout=10
            )
            if r.status_code == 200:
                return r.json()
        except:
            return []
        return []

    raw = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        for f in ex.map(fetch, [1, 2, 3, 4]):
            raw.extend(f)

    valid = []
    for c in raw:
        if (
            c.get("id")
            and c.get("symbol")
            and c.get("name")
            and c.get("market_cap_rank")
            and c.get("current_price")
            and c.get("market_cap")
            and (c.get("total_volume", 0) / c.get("market_cap", 1)) > 0.00001
        ):
            valid.append({
                "coin_id": c["id"],
                "symbol": c["symbol"].upper(),
                "name": c["name"],
                "market_cap_rank": c["market_cap_rank"],
                "price": c["current_price"],
                "market_cap": c["market_cap"],
                "volume_24h": c.get("total_volume"),
                "liquidity_score": c["total_volume"] / c["market_cap"]
            })

            if len(valid) >= 1000:
                break

    print(f"Validni: {len(valid)}/{len(raw)}")
    return valid



# Fetch Binance symbols
def get_binance_symbols():
    try:
        r = requests.get(BINANCE_BASE + "/api/v3/exchangeInfo", timeout=10)
        return {
            s["symbol"]
            for s in r.json()["symbols"]
            if s["status"] == "TRADING" and s["symbol"].endswith("USDT")
        }
    except:
        return set()



# FILTER 2 — Use Cached Top1000 OR Update if Needed
def filter_2_check_last_dates(coins):
    print("FILTER 2: Load or Update Top1000")

    if should_update_top1000():
        print("Updating Top1000 for today")

        execute_write("DELETE FROM top_coins")

        required_keys = {
            "coin_id", "symbol", "name", "market_cap_rank",
            "price", "market_cap", "volume_24h", "liquidity_score"
        }
        insert_rows = []
        for x in coins:
            if required_keys.issubset(x.keys()):
                insert_rows.append({
                    "coin_id": x["coin_id"],
                    "symbol": x["symbol"],
                    "name": x["name"],
                    "market_cap_rank": x["market_cap_rank"],
                    "price": x["price"],
                    "market_cap": x["market_cap"],
                    "volume_24h": x["volume_24h"],
                    "liquidity_score": x["liquidity_score"]
                })

        if insert_rows:
            execute_many("""
                INSERT INTO top_coins
                (coin_id, symbol, name, market_cap_rank, price, market_cap, volume_24h, liquidity_score)
                VALUES (:coin_id, :symbol, :name, :market_cap_rank, :price, :market_cap, :volume_24h, :liquidity_score)
            """, insert_rows)

        mark_top1000_updated()
        print("Top1000 updated")

    else:
        print("Already updated today — loading cached top1000")

        rows = fetch_mappings("SELECT * FROM top_coins")
        coins = rows

    # Get Binance symbols
    binance = get_binance_symbols()

    result = []
    for coin in coins:
        pair = coin["symbol"] + "USDT"

        if pair not in binance:
            continue

        last_ts = fetch_scalar(
            "SELECT MAX(timestamp) FROM ohlcv_data WHERE symbol = :symbol",
            {"symbol": pair}
        )

        coin["binance_pair"] = pair
        coin["last_timestamp"] = last_ts

        result.append(coin)

    print(f"Binance pairs: {len(result)}")
    return result


# GET LAST SAVED TIMESTAMP FOR SYMBOL
def get_last_saved_timestamp(symbol):
    row = fetch_scalar(
        "SELECT MAX(date) FROM ohlcv_data WHERE symbol = :symbol",
        {"symbol": symbol}
    )

    if row:
        next_day = dt.datetime.strptime(row, "%Y-%m-%d") + dt.timedelta(days=1)
        return int(next_day.timestamp() * 1000)

    return int((dt.datetime.now() - dt.timedelta(days=3650)).timestamp() * 1000)


# FILTER 3 — Smart OHLCV Fetch
def filter_3_fill_missing_data(coins):
    print("FILTER 3: Smart OHLCV Fetch")

    def download(coin):
        pair = coin["binance_pair"]

        start = get_last_saved_timestamp(pair)
        end = int(time.time() * 1000)

        if start >= end:
            return (pair, 0)

        candles = []
        cursor = start

        while cursor < end:
            try:
                r = requests.get(
                    BINANCE_BASE + "/api/v3/klines",
                    params={
                        "symbol": pair,
                        "interval": "1d",
                        "startTime": cursor,
                        "endTime": end,
                        "limit": 1000
                    },
                    timeout=10
                )
                chunk = r.json()

                if not isinstance(chunk, list) or len(chunk) == 0:
                    break

                candles.extend([
                    (
                        pair,
                        k[0],
                        dt.datetime.fromtimestamp(k[0] / 1000).strftime("%Y-%m-%d"),
                        float(k[1]),
                        float(k[2]),
                        float(k[3]),
                        float(k[4]),
                        float(k[5])
                    )
                    for k in chunk
                ])

                cursor = chunk[-1][0] + 1

            except:
                break

        if candles:
            execute_many("""
                INSERT INTO ohlcv_data
                (symbol, timestamp, date, open, high, low, close, volume)
                VALUES (:symbol, :timestamp, :date, :open, :high, :low, :close, :volume)
                ON CONFLICT (symbol, timestamp) DO NOTHING
            """, [
                {
                    "symbol": c[0],
                    "timestamp": c[1],
                    "date": c[2],
                    "open": c[3],
                    "high": c[4],
                    "low": c[5],
                    "close": c[6],
                    "volume": c[7]
                }
                for c in candles
            ])

        return (pair, len(candles))

    with ThreadPoolExecutor(max_workers=16) as ex:
        results = list(ex.map(download, coins))

    total = sum(r[1] for r in results)

    print(f"Candles saved: {total}")
    return {"total": len(coins), "candles": total}



# RUN PIPELINE
def run_pipeline():
    print("SMART PIPELINE — Daily Top1000 Caching + Smart OHLCV Update")

    start = time.time()

    init_db()
    coins = filter_1_fetch_top_coins()
    print()
    coins_dates = filter_2_check_last_dates(coins)
    print()
    stats = filter_3_fill_missing_data(coins_dates)
    print()

    print(f"Finished in {time.time() - start:.2f} seconds")
    print(f"Coins processed: {stats['total']}")
    print(f"Candles added: {stats['candles']}")

if __name__ == "__main__":
    run_pipeline()
