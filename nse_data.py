import os, asyncio, pytz, threading, json, websocket, struct, requests, time, re, pandas as pd
from io import StringIO
from collections import deque
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
SIGNAL_AMOUNT = float(os.getenv("SIGNAL_AMOUNT")) * 5
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IST = pytz.timezone("Asia/Kolkata")

# --- PARAMETERS ---
VOL_5MIN_THRESHOLD_CR = 40.0
COOLDOWN_SECONDS = 300  # 5 Minutes cooldown for same stock alert
CR_UNIT = 10_000_000

# --- STATE ---
alert_cooldowns = {}
volume_history = {} # {sid: deque([(timestamp, cumulative_volume), ...])}
ID_TO_SYMBOL = {}
SIDS_LIST = []

# ============================================================= #
#                INSTRUMENT FETCH & FILTERING                   #
# ============================================================= #
EXCLUDED_SYMBOLS = {
    "M&M","BEL","JISLJALEQS","ABCAPITAL","HDFCLIFE","NSLNISP","ASIANPAINT","HEROMOTOCO",
    "NATIONALUM","NMDC","SAMMAANCAP","NESTLEIND","IDBI","JIOFIN","GAEL","ITC","FMCGIETF",
    "MON100","PSUBNKBEES","SILVERBEES","ITBEES","ITIETF","NIFTYBEES","CONSUMBEES","ALPHA",
    "AUTOBEES","MASPTOP50","SILVERETF","SILVERIETF","LTF","HNGSNGBEES","SOUTHBANK","HINDALCO",
    "IRB","TECHM","SAIL","POWERGRID","CANB","IREDA","IRCON","BEML","AXISBANK","BANKBEES",
    "BANKIETF","BANKNIFTY1","HDFCBANK","ICICIBANK","KOTAKBANK","KTKBANK","PSUBANK","PSUBANKADD",
    "PVTBANKADD","RBLBANK","UTIBANKETF","YESBANK","ETERNAL","SBIN","NTPC","BHEL","RECLTD",
    "WIPRO","INFY","MSUMI","MOTHERSON","ABFRL","DELHIVERY","RELIANCE","TCS","TATACHEM",
    "TATACOMM","TATACONSUM","TATAELXSI","TATAINVEST","TATAMOTORS","TATAPOWER","TATASTEEL",
    "TATATECH","Zerodha Nifty 1D Rate Liquid ETF","ALLCARGO","HDFCSILVER","SILVER1",
    "SILVERADD","SBISILVER","SILVER","SILVERIETF","SILVERETF"
}

def fetch_and_build_list():
    global ID_TO_SYMBOL, SIDS_LIST
    print("⬇️ Fetching live instrument master and leverage data...")
    
    # 1. Get Instrument Master
    inst_url = "https://api.dhan.co/v2/instrument/NSE_EQ"
    # Note: Using standard headers for the master fetch
    headers = {
        'access-token': DHAN_ACCESS_TOKEN, 
        'client-id': DHAN_CLIENT_ID, 
        'Content-Type': 'application/json', 
        'Accept': 'application/json'
    }
    
    # The instrument API returns CSV text
    resp = requests.get(inst_url, headers={'access-token': DHAN_ACCESS_TOKEN})
    inst_df = pd.read_csv(StringIO(resp.text))
    
    inst_df = inst_df[(inst_df["EXCH_ID"] == "NSE") & (inst_df["SEGMENT"] == "E") & (inst_df["INSTRUMENT_TYPE"] == "ES")]
    inst_df = inst_df[["SECURITY_ID", "UNDERLYING_SYMBOL"]].rename(columns={"UNDERLYING_SYMBOL": "Symbol"})
    inst_df["Symbol"] = inst_df["Symbol"].str.upper().str.strip()

    # 2. Get Leverage Sheet
    sheet_url = "https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:csv&gid=1663719548"
    lev_df = pd.read_csv(sheet_url)
    
    cols = list(lev_df.columns)
    symbol_col = cols[cols.index("Sr.") + 1]
    lev_df = lev_df.rename(columns={symbol_col: "Symbol"})
    lev_df["Symbol"] = lev_df["Symbol"].astype(str).str.upper().str.strip()
    lev_df["MIS"] = pd.to_numeric(lev_df["MIS (Intraday)"].astype(str).str.replace("x","",regex=False).str.replace("X","",regex=False), errors="coerce")
    
    # 3. Filter 5x and Exclusions
    mis_df = lev_df[lev_df["MIS"] >= 5][["Symbol", "MIS"]].copy()
    exclude_pattern = re.compile(r"(BEES|ETF|CASE)", re.IGNORECASE)
    mis_df = mis_df[~mis_df["Symbol"].str.contains(exclude_pattern, na=False)]
    mis_df = mis_df[~mis_df["Symbol"].isin(EXCLUDED_SYMBOLS)]
    
    # 4. Merge to get all potential 5x IDs
    final_df = mis_df.merge(inst_df, on="Symbol", how="inner")
    potential_sids = final_df["SECURITY_ID"].tolist()
    
    # PRE-LOOP OPTIMIZATION: Create a fast map to avoid slow .loc inside the loop
    sid_to_symbol_map = dict(zip(final_df['SECURITY_ID'], final_df['Symbol']))

    # 5. Fetch LTP and Filter (5 to 1500)
    print(f"🔍 Checking prices for {len(potential_sids)} candidates...")
    filtered_data = []
    quote_url = "https://api.dhan.co/v2/marketfeed/ltp"
    
    # Process in chunks of 1000 (Dhan's maximum limit)
    for i in range(0, len(potential_sids), 1000):
        chunk = potential_sids[i:i+1000]
        payload = {"NSE_EQ": chunk}
        
        try:
            q_resp = requests.post(quote_url, headers=headers, json=payload, timeout=10)
            
            if q_resp.status_code == 200:
                # The response structure is: data -> NSE_EQ -> { "SID": {"last_price": 0} }
                response_json = q_resp.json()
                market_data = response_json.get('data', {}).get('NSE_EQ', {})
                
                for sid_key, details in market_data.items():
                    # Default to 0 to prevent NoneType comparison errors
                    ltp = details.get('last_price', 0)
                    
                    if 5 <= ltp <= 1500:
                        sid_int = int(sid_key)
                        filtered_data.append({
                            "SECURITY_ID": sid_int, 
                            "Symbol": sid_to_symbol_map.get(sid_int, "Unknown")
                        })
            else:
                print(f"⚠️ API error for chunk starting at {i}: {q_resp.status_code}")
            
            # Rate limit is 1 req/sec. 1.1s is safe.
            time.sleep(1.1) 
            
        except Exception as e:
            print(f"⚠️ Quote Error in batch {i}: {e}")

    # Build Final Lists
    if filtered_data:
        valid_df = pd.DataFrame(filtered_data)
        ID_TO_SYMBOL = pd.Series(valid_df.Symbol.values, index=valid_df.SECURITY_ID).to_dict()
        SIDS_LIST = valid_df["SECURITY_ID"].astype(str).tolist()
        print(f"✅ Setup Complete: Subscribing to {len(SIDS_LIST)} stocks between ₹5-1500.")
    else:
        print("❌ No stocks found meeting the criteria.")
        ID_TO_SYMBOL = {}
        SIDS_LIST = []
        
# ============================================================= #
#                     ANALYTICS & ALERTING                      #
# ============================================================= #
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=5)
    except:
        pass
            
import websocket
def run_ws():
    auth_url = f"wss://api-feed.dhan.co?version=2&token={DHAN_ACCESS_TOKEN}&clientId={DHAN_CLIENT_ID}&authType=2"
    
    def on_open(ws):
        print("🌐 Connected to Dhan Feed.")
        # Subscribe in chunks of 100
        for i in range(0, len(SIDS_LIST), 100):
            chunk = SIDS_LIST[i:i+100]
            sub_data = {
                "RequestCode": 21, # Ticker Mode (LTP + Vol)
                "InstrumentCount": len(chunk),
                "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": s} for s in chunk]
            }
            ws.send(json.dumps(sub_data))

    ws = websocket.WebSocketApp(auth_url, on_message=on_message, on_open=on_open)
    ws.run_forever()

def on_message(ws, message):
    if not isinstance(message, bytes) or len(message) < 62:
        return
    
    # Byte 0 is the Response Code
    response_code = message[0]
    
    try:
        # 1. Header: Security ID (Bytes 4-8) -> index 4:8
        sec_id = struct.unpack('<I', message[4:8])[0]

        # 2. Process only "Full Packet" (Code 8)
        if response_code == 8:
            # Bytes 9-12: LTP (Float) -> index 8:12
            # (Note: In Python message[8:12] covers bytes 9,10,11,12)
            ltp = round(struct.unpack('<f', message[8:12])[0], 2)
            
            # Bytes 23-26: Volume (Int32) -> index 22:26
            # This is the Cumulative Volume for the day
            cum_vol = struct.unpack('<I', message[22:26])[0]

            # SANITY CHECK: Real LTP shouldn't be 0.0000... or 3 Billion
            if 0.05 < ltp < 200000:
                process_volume(sec_id, ltp, cum_vol)
                
    except Exception as e:
        pass

def process_volume(sec_id, ltp, cum_vol):
    now = time.time()
    
    # 1. Initialize per stock
    if sec_id not in volume_history:
        volume_history[sec_id] = deque()
    
    # 2. Store current state
    volume_history[sec_id].append((now, cum_vol))
    
    # 3. Keep exactly 5 minutes of history for THIS stock
    while volume_history[sec_id] and (now - volume_history[sec_id][0][0] > 300):
        volume_history[sec_id].popleft()
    
    # 4. Calculate Delta (Current - Oldest in 5m window)
    if len(volume_history[sec_id]) > 1:
        old_time, old_vol = volume_history[sec_id][0]
        
        # This is the volume traded IN THE LAST 5 MINUTES
        delta_qty = cum_vol - old_vol
        
        # Calculate Value in Crores
        # (Delta Shares * Price) / 10,000,000
        traded_value_cr = (delta_qty * ltp) / 10_000_000
        
        # 5. Alert if threshold met
        if traded_value_cr >= VOL_5MIN_THRESHOLD_CR:
            # Check cooldown so we don't spam for the same stock
            if now - alert_cooldowns.get(sec_id, 0) > COOLDOWN_SECONDS:
                alert_cooldowns[sec_id] = now
                symbol = ID_TO_SYMBOL.get(sec_id, f"ID:{sec_id}")
                
                msg = (f"🔥 *VOLUME SPIKE: {symbol}*\n"
                       f"Value: ₹{traded_value_cr:.2f} Cr (Last 5m)\n"
                       f"Price: ₹{ltp} QTY: { (SIGNAL_AMOUNT/ltp} \n"
                       f"Delta Qty: {delta_qty/1000000 }M shares")
                print(msg)
                threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()


def is_market_open():
    """Checks if the current time is between 09:15 and 15:30 IST."""
    now_ist = datetime.now(IST).time()
    market_start = datetime.strptime("09:15:00", "%H:%M:%S").time()
    market_end = datetime.strptime("15:30:00", "%H:%M:%S").time()
    return market_start <= now_ist <= market_end

def wait_until_market_opens():
    """Pauses execution until 09:15 IST if started early."""
    while True:
        now_ist = datetime.now(IST)
        market_start = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
        
        if now_ist < market_start:
            wait_secs = (market_start - now_ist).total_seconds()
            print(f"🕒 Market hasn't opened yet. Sleeping for {int(wait_secs/60)} minutes...")
            time.sleep(min(wait_secs, 60)) # Check every minute
        else:
            break

if __name__ == "__main__":
    # 1. Handle Pre-market
    wait_until_market_opens()
    
    # 2. Initial Setup
    fetch_and_build_list()
    
    if SIDS_LIST:
        print(f"🚀 Monitoring started. Will auto-close at 15:30 IST.")
        
        # 3. Main Loop
        while True:
            now_ist = datetime.now(IST).time()
            market_end = datetime.strptime("15:30:00", "%H:%M:%S").time()

            if now_ist > market_end:
                print("🏁 Market closed (15:30 IST). Exiting script.")
                break
            
            try:
                run_ws() 
            except Exception as e:
                print(f"⚠️ Connection dropped: {e}. Reconnecting in 5 seconds...")
                time.sleep(5)
