"""
NSE Option Chain Poller Worker
Fetches NIFTY option chain data every 30 seconds and pushes to Supabase
"""

import time
import os
from datetime import datetime
from dotenv import load_dotenv
from nsepython import nse_optionchain_scrapper
from supabase import create_client, Client
import pytz

load_dotenv()

# Supabase setup
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # Service role key needed for inserts

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY in environment")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Config
SYMBOL = os.getenv("SYMBOL", "NIFTY")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))

# Timezone
IST = pytz.timezone('Asia/Kolkata')


def is_market_open() -> bool:
    """Check if NSE market is currently open (Mon-Fri 09:15-15:30 IST)"""
    now = datetime.now(IST)
    
    # Weekend check
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    
    # Market hours: 09:15 to 15:30 IST
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    
    return market_open <= now <= market_close


def parse_chain(data: dict) -> list[dict]:
    """Parse NSE option chain response into Supabase-ready records"""
    records = []
    
    try:
        # Get nearest expiry
        expiry = data["records"]["expiryDates"][0]
        now = datetime.utcnow().isoformat()
        
        for row in data["records"]["data"]:
            strike_price = row.get("strikePrice")
            
            if not strike_price:
                continue
            
            # Process both CE and PE
            for opt_type in ("CE", "PE"):
                if opt_type not in row:
                    continue
                
                d = row[opt_type]
                
                # Extract all fields
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
        # Upsert based on symbol, strike, expiry, option_type
        # Note: Supabase Python client doesn't support on_conflict directly
        # We'll use insert with error handling or update to upsert
        
        # Delete old snapshots for same contracts first
        for record in records:
            supabase.table("historical_options_data_current").delete().match({
                "symbol": record["symbol"],
                "strike": record["strike"],
                "expiry": record["expiry"],
                "option_type": record["option_type"]
            }).execute()
        
        # Insert new data
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
    
    # Warm session
    warm_session()
    
    consecutive_failures = 0
    empty_response_count = 0
    
    while True:
        try:
            # Check market hours
            if not is_market_open():
                now = datetime.now(IST)
                print(f"[{now}] Market closed. Sleeping 5 minutes...")
                time.sleep(300)  # Sleep 5 min
                continue
            
            # Fetch option chain
            print(f"[{datetime.now(IST)}] Fetching {SYMBOL} option chain...")
            data = nse_optionchain_scrapper(SYMBOL)
            
            # Parse data
            records = parse_chain(data)
            
            # Check for empty response
            if not records:
                empty_response_count += 1
                print(f"[WARN] Empty response ({empty_response_count}/3)")
                
                if empty_response_count >= 3:
                    print("[WARN] 3 consecutive empty responses. Refreshing session...")
                    warm_session()
                    empty_response_count = 0
                
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            
            # Reset empty counter on success
            empty_response_count = 0
            
            # Push to Supabase
            success = push_to_supabase(records)
            
            if success:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
            
        except Exception as e:
            consecutive_failures += 1
            print(f"[ERROR] {e}")
            print(f"[ERROR] Consecutive failures: {consecutive_failures}")
            
            # Exponential backoff on repeated failures
            if consecutive_failures >= 5:
                print("[CRITICAL] 5 consecutive failures. Sleeping 5 minutes...")
                time.sleep(300)
                warm_session()  # Refresh session
                consecutive_failures = 0
        
        # Sleep until next poll
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
