import requests
import sqlite3
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import time
import os

DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.getenv('COINGECKO_DB_PATH', os.path.join(DATA_DIR, 'coingecko_top1000.db'))
API_KEY = "CG-t7FgFVU7PUeZL3nMf7Zd9hRV"
HEADERS = {"accept": "application/json", "x-cg-pro-api-key": API_KEY}
BINANCE_BASE = "https://api.binance.com"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

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

            conn = get_db_connection()
            conn.cursor().executemany("""
                INSERT OR IGNORE INTO ohlcv_data
                (symbol, timestamp, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, candles)
            conn.commit()
            conn.close()

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

    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT MIN(timestamp), MAX(timestamp) FROM ohlcv_data WHERE symbol = ?", (pair,))
    row = c.fetchone()
    conn.close()

    min_ts, max_ts = row if row else (None, None)
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
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("""
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
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS meta_info (
            id INTEGER PRIMARY KEY,
            last_top1000_update TEXT
        )
    """)

    c.execute(
        "INSERT OR IGNORE INTO meta_info (id, last_top1000_update) VALUES (1, NULL)"
    )

    c.execute("""
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
    """)

    c.execute("CREATE INDEX IF NOT EXISTS idx_st ON ohlcv_data(symbol, timestamp)")

    conn.commit()
    conn.close()



# CHECK IF WE NEED TO UPDATE TOP 1000 TODAY
def should_update_top1000():
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("SELECT last_top1000_update FROM meta_info WHERE id=1")
    row = c.fetchone()[0]

    conn.close()

    today = dt.datetime.now().strftime("%Y-%m-%d")
    return row != today


def mark_top1000_updated():
    conn = get_db_connection()
    c = conn.cursor()

    today = dt.datetime.now().strftime("%Y-%m-%d")
    c.execute("UPDATE meta_info SET last_top1000_update=?", (today,))

    conn.commit()
    conn.close()


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
            c.get("market_cap_rank")
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

    conn = get_db_connection()
    c = conn.cursor()

    if should_update_top1000():
        print("Updating Top1000 for today")

        c.execute("DELETE FROM top_coins")

        c.executemany("""
            INSERT INTO top_coins
            (coin_id, symbol, name, market_cap_rank, price, market_cap, volume_24h, liquidity_score)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                x["coin_id"], x["symbol"], x["name"], x["market_cap_rank"],
                x["price"], x["market_cap"], x["volume_24h"], x["liquidity_score"]
            )
            for x in coins
        ])

        conn.commit()
        conn.close()

        mark_top1000_updated()

        # reopen after close
        conn = get_db_connection()
        c = conn.cursor()

        print("Top1000 updated")

    else:
        print("Already updated today — loading cached top1000")

        c.execute("SELECT * FROM top_coins")
        rows = c.fetchall()
        coins = []
        for r in rows:
            coins.append({
                "coin_id": r[0],
                "symbol": r[1],
                "name": r[2],
                "market_cap_rank": r[3],
                "price": r[4],
                "market_cap": r[5],
                "volume_24h": r[6],
                "liquidity_score": r[7]
            })

    # Get Binance symbols
    binance = get_binance_symbols()

    result = []
    for coin in coins:
        pair = coin["symbol"] + "USDT"

        if pair not in binance:
            continue

        c.execute("SELECT MAX(timestamp) FROM ohlcv_data WHERE symbol=?", (pair,))
        last_ts = c.fetchone()[0]

        coin["binance_pair"] = pair
        coin["last_timestamp"] = last_ts

        result.append(coin)

    conn.close()
    print(f"Binance pairs: {len(result)}")
    return result


# GET LAST SAVED TIMESTAMP FOR SYMBOL
def get_last_saved_timestamp(symbol):
    conn = get_db_connection()
    c = conn.cursor()

    c.execute("SELECT MAX(date) FROM ohlcv_data WHERE symbol=?", (symbol,))
    row = c.fetchone()[0]

    conn.close()

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
            conn = get_db_connection()
            conn.cursor().executemany("""
                INSERT OR IGNORE INTO ohlcv_data
                (symbol, timestamp, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, candles)
            conn.commit()
            conn.close()

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
