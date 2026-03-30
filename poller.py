"""NSE Option Chain Poller Worker
Fetches NIFTY option chain data every 30 seconds and pushes to Supabase
"""
import time
from datetime import datetime
from supabase import create_client, Client
from nsepython import nse_optionchain_scrapper
import pytz

# Supabase setup
SUPABASE_URL = "https://sljdaubwtmmgwekjcdkk.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNsamRhdWJ3dG1tZ3dla2pjZGtrIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ1NTA2NDgsImV4cCI6MjA5MDEyNjY0OH0.CB_MFtVW_GieONweIUPUyCFS88v3EmFmmjDU4MiyU84"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Config
SYMBOL = "NIFTY"
POLL_INTERVAL_SECONDS = 30

# Timezone
IST = pytz.timezone('Asia/Kolkata')

def is_market_open() -> bool:
    """Check if NSE market is currently open (Mon-Fri 09:15-15:30 IST)"""
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


def parse_chain(data: dict) -> list[dict]:
    """Parse NSE option chain response into Supabase-ready records"""
    records = []
    try:
        expiry = data["records"]["expiryDates"][0]
        now = datetime.utcnow().isoformat()
        for row in data["records"]["data"]:
            strike_price = row.get("strikePrice")
            if not strike_price:
                continue
            for opt_type in ("CE", "PE"):
                if opt_type not in row:
                    continue
                d = row[opt_type]
                record = {
                    "symbol": SYMBOL,
                    "strike": strike_price,
                    "expiry": expiry,
                    "option_type": opt_type,
                    "open_interest": d.get("openInterest"),
                    "iv": d.get("impliedVolatility"),
                    "delta": d.get("delta"),
                    "gamma": d.get("gamma"),
                    "theta": d.get("theta"),
                    "vega": d.get("vega"),
                    "volume": d.get("totalTradedVolume"),
                    "ltp": d.get("lastPrice"),
                    "bid": d.get("bidprice"),
                    "ask": d.get("askPrice"),
                    "snapshot_time": now,
                }
                records.append(record)
        return records
    except (KeyError, IndexError, TypeError) as e:
        print(f"[PARSE ERROR] {e}")
        return []


def push_to_supabase(records: list[dict]) -> bool:
    """Push records to Supabase with upsert logic"""
    if not records:
        print(f"[{datetime.now(IST)}] No records to push")
        return False
    try:
        for record in records:
            supabase.table("historical_options_data_current").delete().match({
                "symbol": record["symbol"],
                "strike": record["strike"],
                "expiry": record["expiry"],
                "option_type": record["option_type"],
            }).execute()
        result = supabase.table("historical_options_data_current").insert(records).execute()
        print(f"[{datetime.now(IST)}] ✓ Pushed {len(records)} rows | Expiry: {records[0]['expiry']}")
        return True
    except Exception as e:
        print(f"[SUPABASE ERROR] {e}")
        return False


def warm_session():
    """Warm up NSE session on startup"""
    try:
        print("[INIT] Warming NSE session...")
        nse_optionchain_scrapper(SYMBOL)
        print("[INIT] ✓ Session ready")
    except Exception as e:
        print(f"[INIT ERROR] {e}")


def run():
    """Main polling loop"""
    print(f"[START] NSE Option Chain Poller")
    print(f"[CONFIG] Symbol: {SYMBOL} | Interval: {POLL_INTERVAL_SECONDS}s")
    print(f"[CONFIG] Market Hours: Mon-Fri 09:15-15:30 IST\n")
    warm_session()
    consecutive_failures = 0
    empty_response_count = 0
    while True:
        try:
            if not is_market_open():
                now = datetime.now(IST)
                print(f"[{now}] Market closed. Sleeping 5 minutes...")
                time.sleep(300)
                continue
            print(f"[{datetime.now(IST)}] Fetching {SYMBOL} option chain...")
            data = nse_optionchain_scrapper(SYMBOL)
            records = parse_chain(data)
            if not records:
                empty_response_count += 1
                print(f"[WARN] Empty response ({empty_response_count}/3)")
                if empty_response_count >= 3:
                    print("[WARN] 3 consecutive empty responses. Refreshing session...")
                    warm_session()
                    empty_response_count = 0
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            empty_response_count = 0
            success = push_to_supabase(records)
            if success:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
        except Exception as e:
            consecutive_failures += 1
            print(f"[ERROR] {e}")
            print(f"[ERROR] Consecutive failures: {consecutive_failures}")
            if consecutive_failures >= 5:
                print("[CRITICAL] 5 consecutive failures. Sleeping 5 minutes...")
                time.sleep(300)
                warm_session()
                consecutive_failures = 0
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
