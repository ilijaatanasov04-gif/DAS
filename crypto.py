import requests
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import time
import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from urllib.parse import urlparse

DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.getenv('COINGECKO_DB_PATH', os.path.join(DATA_DIR, 'coingecko_top1000.db'))
DATABASE_URL = os.getenv('DATABASE_URL') or os.getenv('SUPABASE_DATABASE_URL')
COINGECKO_BASE = os.getenv("COINGECKO_API_BASE", "https://api.coingecko.com").rstrip("/")
API_KEY = os.getenv("COINGECKO_API_KEY") or ""
API_KEY_TYPE = os.getenv("COINGECKO_API_KEY_TYPE", "").strip().lower()
if not API_KEY_TYPE:
    API_KEY_TYPE = "pro" if "pro-api.coingecko.com" in COINGECKO_BASE else "demo"
HEADERS = {"accept": "application/json"}
if API_KEY:
    header_name = "x-cg-pro-api-key" if API_KEY_TYPE == "pro" else "x-cg-demo-api-key"
    HEADERS[header_name] = API_KEY
BINANCE_BASE = os.getenv("BINANCE_BASE", "https://api.binance.com").rstrip("/")
BINANCE_MAX_COINS = int(os.getenv("BINANCE_MAX_COINS", "100"))
BINANCE_MAX_YEARS = int(os.getenv("BINANCE_MAX_YEARS", "5"))
BINANCE_WORKERS = int(os.getenv("BINANCE_WORKERS", "4"))
COINGECKO_OHLCV_FALLBACK = os.getenv("COINGECKO_OHLCV_FALLBACK", "1").strip().lower() in ("1", "true", "yes", "on")

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

def get_db_target():
    if DATABASE_URL:
        parsed = urlparse(DATABASE_URL)
        host = parsed.hostname or ""
        db = (parsed.path or "").lstrip("/")
        scheme = parsed.scheme or "postgresql"
        return f"{scheme}://{host}/{db}".rstrip("/")
    return f"sqlite:///{DB_PATH}"

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

def _save_candles(candles):
    if not candles:
        return
    execute_many("""
        INSERT INTO ohlcv_data
        (symbol, timestamp, date, open, high, low, close, volume)
        VALUES (:symbol, :timestamp, :date, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, timestamp) DO NOTHING
    """, candles)

def _fetch_binance_candles(pair, start_ms, end_ms):
    candles = []
    cursor = start_ms

    while cursor < end_ms:
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

        if r.status_code != 200:
            print(f"Binance error: status={r.status_code} body={r.text[:200]}")
            return None

        chunk = r.json()
        if not isinstance(chunk, list) or len(chunk) == 0:
            break

        for k in chunk:
            candles.append({
                "symbol": pair,
                "timestamp": int(k[0]),
                "date": dt.datetime.fromtimestamp(k[0] / 1000).strftime("%Y-%m-%d"),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5])
            })

        cursor = int(chunk[-1][0]) + 1
        if len(chunk) < 1000:
            break

    return candles

def _fetch_coingecko_candles(coin_id, pair, start_ms, end_ms):
    if not coin_id:
        return []

    max_days = max(BINANCE_MAX_YEARS, 1) * 365
    days_requested = int((end_ms - start_ms) / 86400000) + 2
    days = max(1, min(days_requested, max_days))
    url = f"{COINGECKO_BASE}/api/v3/coins/{coin_id}/market_chart"

    for attempt in range(4):
        try:
            r = requests.get(
                url,
                params={"vs_currency": "usd", "days": str(days), "interval": "daily"},
                headers=HEADERS,
                timeout=20
            )
            if r.status_code == 429:
                time.sleep(1.0 * (2 ** attempt))
                continue
            if r.status_code != 200:
                print(f"Coingecko market_chart error ({coin_id}): status={r.status_code} body={r.text[:200]}")
                return []

            payload = r.json()
            prices = payload.get("prices") or []
            volumes = payload.get("total_volumes") or []
            if not prices:
                return []

            volume_by_ts = {}
            for row in volumes:
                if isinstance(row, list) and len(row) >= 2:
                    volume_by_ts[int(row[0])] = float(row[1])

            result = []
            for row in prices:
                if not isinstance(row, list) or len(row) < 2:
                    continue
                ts = int(row[0])
                if ts < start_ms or ts > end_ms:
                    continue
                close = float(row[1])
                result.append({
                    "symbol": pair,
                    "timestamp": ts,
                    "date": dt.datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d"),
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": float(volume_by_ts.get(ts, 0.0))
                })
            return result
        except Exception:
            time.sleep(1.0 * (2 ** attempt))

    return []

def _get_coin_id_by_symbol(symbol):
    row = fetch_mapping(
        "SELECT coin_id FROM top_coins WHERE UPPER(symbol) = :symbol LIMIT 1",
        {"symbol": symbol.upper()}
    )
    return row.get("coin_id") if row else None

def backfill_ohlcv(symbol, start_ms=0, end_ms=None):
    pair = symbol.upper() + "USDT"
    end_ms = end_ms or int(time.time() * 1000)
    coin_id = _get_coin_id_by_symbol(symbol)

    try:
        candles = _fetch_binance_candles(pair, start_ms, end_ms)
    except Exception:
        candles = None

    if candles is None and COINGECKO_OHLCV_FALLBACK:
        candles = _fetch_coingecko_candles(coin_id, pair, start_ms, end_ms)

    if candles:
        _save_candles(candles)
        return len(candles)
    return 0

def ensure_ohlcv_data(symbol, max_days=None):
    pair = symbol.upper() + "USDT"
    now_ms = int(time.time() * 1000)
    min_start_ms = None
    if max_days:
        min_start_ms = now_ms - (max_days * 86400000)

    row = fetch_mapping(
        "SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM ohlcv_data WHERE symbol = :symbol",
        {"symbol": pair}
    )

    min_ts = row["min_ts"] if row else None
    max_ts = row["max_ts"] if row else None
    total = 0

    if min_ts is None:
        start_ms = min_start_ms if min_start_ms is not None else 0
        total += backfill_ohlcv(symbol, start_ms=start_ms, end_ms=now_ms)
    else:
        if min_ts > 0:
            start_ms = min_start_ms if min_start_ms is not None else 0
            if min_ts - 1 >= start_ms:
                total += backfill_ohlcv(symbol, start_ms=start_ms, end_ms=min_ts - 1)
        if max_ts and max_ts + 86400000 < now_ms:
            end_ms = now_ms
            if min_start_ms is not None and max_ts + 1 < min_start_ms:
                # Only backfill within the desired window
                return total
            total += backfill_ohlcv(symbol, start_ms=max_ts + 1, end_ms=end_ms)

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
    count = fetch_scalar("SELECT COUNT(*) FROM top_coins")
    if not count:
        return True

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

    page_count = int(os.getenv("COINGECKO_PAGE_COUNT", "4"))
    per_page = int(os.getenv("COINGECKO_PER_PAGE", "250"))
    base_delay = float(os.getenv("COINGECKO_PAGE_DELAY_SEC", "1.2"))
    max_retries = int(os.getenv("COINGECKO_MAX_RETRIES", "5"))

    def fetch(p):
        url = f"{COINGECKO_BASE}/api/v3/coins/markets"
        for attempt in range(max_retries):
            try:
                r = requests.get(
                    url,
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": per_page,
                        "page": p
                    },
                    headers=HEADERS,
                    timeout=15
                )
                if r.status_code == 200:
                    return r.json()
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after else (base_delay * (2 ** attempt))
                    print(f"Coingecko rate limit hit; waiting {wait:.1f}s before retry")
                    time.sleep(wait)
                    continue
                print(f"Coingecko error: status={r.status_code} body={r.text[:200]}")
                return []
            except Exception:
                print("Coingecko error: request failed")
                time.sleep(base_delay * (2 ** attempt))
        return []

    raw = []
    for p in range(1, page_count + 1):
        batch = fetch(p)
        if not batch:
            continue
        raw.extend(batch)
        time.sleep(base_delay)

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
    url = BINANCE_BASE + "/api/v3/exchangeInfo"
    for attempt in range(3):
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                print(f"Binance exchangeInfo error: status={r.status_code} body={r.text[:200]}")
                time.sleep(1.0 * (2 ** attempt))
                continue
            payload = r.json()
            symbols = payload.get("symbols", [])
            if not symbols:
                print("Binance exchangeInfo error: no symbols in response")
                return set()
            return {
                s["symbol"]
                for s in symbols
                if s.get("status") == "TRADING" and s.get("symbol", "").endswith("USDT")
            }
        except Exception:
            time.sleep(1.0 * (2 ** attempt))
            continue
    return set()



# FILTER 2 — Use Cached Top1000 OR Update if Needed
def filter_2_check_last_dates(coins):
    print("FILTER 2: Load or Update Top1000")

    if should_update_top1000():
        print("Updating Top1000 for today")

        if not coins:
            print("No coins fetched; keeping existing cache")
            rows = fetch_mappings("SELECT * FROM top_coins")
            coins = rows
            # continue to source selection below

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

    # Use Binance when available; otherwise use Coingecko fallback for OHLCV.
    binance = get_binance_symbols()
    binance_available = len(binance) > 0

    result = []
    for coin in coins:
        pair = coin["symbol"] + "USDT"

        if binance_available and pair in binance:
            source = "binance"
        else:
            source = "coingecko"
            if not COINGECKO_OHLCV_FALLBACK:
                continue

        last_ts = fetch_scalar(
            "SELECT MAX(timestamp) FROM ohlcv_data WHERE symbol = :symbol",
            {"symbol": pair}
        )

        coin["symbol_pair"] = pair
        coin["binance_pair"] = pair
        coin["data_source"] = source
        coin["last_timestamp"] = last_ts

        result.append(coin)

    if BINANCE_MAX_COINS and len(result) > BINANCE_MAX_COINS:
        result = result[:BINANCE_MAX_COINS]

    source_counts = {"binance": 0, "coingecko": 0}
    for coin in result:
        source_counts[coin["data_source"]] = source_counts.get(coin["data_source"], 0) + 1
    print(f"Pairs prepared: {len(result)} (binance={source_counts['binance']}, coingecko={source_counts['coingecko']})")
    return result


# GET LAST SAVED TIMESTAMP FOR SYMBOL
def get_last_saved_timestamp(symbol):
    row = fetch_scalar(
        "SELECT MAX(timestamp) FROM ohlcv_data WHERE symbol = :symbol",
        {"symbol": symbol}
    )

    if row:
        return int(row) + 86400000

    max_days = max(BINANCE_MAX_YEARS, 1) * 365
    return int((dt.datetime.now() - dt.timedelta(days=max_days)).timestamp() * 1000)


# FILTER 3 — Smart OHLCV Fetch
def filter_3_fill_missing_data(coins):
    print("FILTER 3: Smart OHLCV Fetch")

    def download(coin):
        pair = coin.get("symbol_pair") or coin.get("binance_pair") or (coin["symbol"] + "USDT")
        source = coin.get("data_source", "binance")

        start = get_last_saved_timestamp(pair)
        end = int(time.time() * 1000)

        if start >= end:
            return (pair, 0)

        if source == "binance":
            try:
                candles = _fetch_binance_candles(pair, start, end)
            except Exception:
                candles = None
            if candles is None and COINGECKO_OHLCV_FALLBACK:
                candles = _fetch_coingecko_candles(coin.get("coin_id"), pair, start, end)
        else:
            candles = _fetch_coingecko_candles(coin.get("coin_id"), pair, start, end)

        if candles:
            _save_candles(candles)

        return (pair, len(candles))

    with ThreadPoolExecutor(max_workers=max(BINANCE_WORKERS, 1)) as ex:
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
    top_count = fetch_scalar("SELECT COUNT(*) FROM top_coins")
    print()

    print(f"Finished in {time.time() - start:.2f} seconds")
    print(f"Coins processed: {stats['total']}")
    print(f"Candles added: {stats['candles']}")
    print(f"Top coins in DB: {top_count}")

    return {
        "coins_fetched": len(coins),
        "binance_pairs": len(coins_dates),
        "top_coins": top_count or 0,
        "coins_processed": stats["total"],
        "candles_added": stats["candles"],
        "coingecko_base": COINGECKO_BASE,
        "coingecko_key_set": bool(API_KEY),
        "coingecko_key_type": API_KEY_TYPE if API_KEY else None,
        "db_target": get_db_target()
    }

if __name__ == "__main__":
    run_pipeline()
