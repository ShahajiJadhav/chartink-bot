import os, asyncio, pytz, threading, json, websocket, struct, requests, time, re, pandas as pd
from io import StringIO
from collections import deque
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IST = pytz.timezone("Asia/Kolkata")

# --- PARAMETERS ---
VOL_5MIN_THRESHOLD_CR = 40.0
COOLDOWN_SECONDS = 300 
CR_UNIT = 10_000_000

# --- STATE ---
alert_cooldowns = {}
volume_history = {} 
ID_TO_SYMBOL = {}
SIDS_LIST = []

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
    print("⬇️ Fetching live instrument master...")
    
    headers = {
        'access-token': DHAN_ACCESS_TOKEN, 
        'client-id': DHAN_CLIENT_ID, 
        'Content-Type': 'application/json'
    }
    
    try:
        resp = requests.get("https://api.dhan.co/v2/instrument/NSE_EQ", headers=headers)
        inst_df = pd.read_csv(StringIO(resp.text))
        inst_df = inst_df[(inst_df["EXCH_ID"] == "NSE") & (inst_df["SEGMENT"] == "E") & (inst_df["INSTRUMENT_TYPE"] == "ES")]
        inst_df = inst_df[["SECURITY_ID", "UNDERLYING_SYMBOL"]].rename(columns={"UNDERLYING_SYMBOL": "Symbol"})
        inst_df["Symbol"] = inst_df["Symbol"].str.upper().str.strip()

        print("⬇️ Fetching leverage data...")
        sheet_url = "https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:csv&gid=1663719548"
        lev_df = pd.read_csv(sheet_url)
        
        symbol_col = lev_df.columns[list(lev_df.columns).index("Sr.") + 1]
        lev_df = lev_df.rename(columns={symbol_col: "Symbol"})
        lev_df["Symbol"] = lev_df["Symbol"].astype(str).str.upper().str.strip()
        lev_df["MIS"] = pd.to_numeric(lev_df["MIS (Intraday)"].astype(str).str.replace("x","",regex=False).str.replace("X","",regex=False), errors="coerce")
        
        # Fixed Regex Warning here
        mis_df = lev_df[lev_df["MIS"] >= 5][["Symbol", "MIS"]].copy()
        mis_df = mis_df[~mis_df["Symbol"].str.contains(r"BEES|ETF|CASE", case=False, na=False)]
        mis_df = mis_df[~mis_df["Symbol"].isin(EXCLUDED_SYMBOLS)]
        
        final_df = mis_df.merge(inst_df, on="Symbol", how="inner")
        potential_sids = final_df["SECURITY_ID"].tolist()
        sid_to_symbol_map = dict(zip(final_df['SECURITY_ID'], final_df['Symbol']))

        print(f"🔍 Checking prices for {len(potential_sids)} candidates. This may take a minute...")
        filtered_data = []
        quote_url = "https://api.dhan.co/v2/marketfeed/ltp"
        
        for i in range(0, len(potential_sids), 1000):
            chunk = potential_sids[i:i+1000]
            q_resp = requests.post(quote_url, headers=headers, json={"NSE_EQ": chunk}, timeout=15)
            if q_resp.status_code == 200:
                market_data = q_resp.json().get('data', {}).get('NSE_EQ', {})
                for sid_key, details in market_data.items():
                    ltp = details.get('last_price', 0)
                    if 5 <= ltp <= 1500:
                        sid_int = int(sid_key)
                        filtered_data.append({"SECURITY_ID": sid_int, "Symbol": sid_to_symbol_map.get(sid_int, "Unknown")})
            
            print(f"   Progress: {min(i+1000, len(potential_sids))}/{len(potential_sids)} checked.")
            time.sleep(1.2) 

        if filtered_data:
            valid_df = pd.DataFrame(filtered_data)
            ID_TO_SYMBOL = pd.Series(valid_df.Symbol.values, index=valid_df.SECURITY_ID).to_dict()
            SIDS_LIST = valid_df["SECURITY_ID"].astype(str).tolist()
            print(f"✅ Subscribing to {len(SIDS_LIST)} stocks.")
        else:
            print("❌ No stocks found.")

    except Exception as e:
        print(f"❌ Critical Error in Setup: {e}")

# ... (Rest of your process_volume, on_message, and run_ws functions remain the same) ...

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=5)
    except:
        pass

def process_volume(sec_id, ltp, cum_vol):
    now = time.time()
    if sec_id not in volume_history:
        volume_history[sec_id] = deque()
    
    volume_history[sec_id].append((now, cum_vol))
    while volume_history[sec_id] and (now - volume_history[sec_id][0][0] > 300):
        volume_history[sec_id].popleft()
    
    if len(volume_history[sec_id]) > 1:
        start_vol = volume_history[sec_id][0][1]
        delta_qty = cum_vol - start_vol
        traded_value_cr = (delta_qty * ltp) / CR_UNIT
        
        if traded_value_cr >= VOL_5MIN_THRESHOLD_CR:
            if now - alert_cooldowns.get(sec_id, 0) > COOLDOWN_SECONDS:
                alert_cooldowns[sec_id] = now
                symbol = ID_TO_SYMBOL.get(sec_id, f"ID:{sec_id}")
                msg = (f"🔥 *VOLUME SPIKE*\n*Stock:* {symbol}\n*5-Min Vol:* ₹{traded_value_cr:.2f} Cr\n*Price:* {ltp}")
                threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()

def on_message(ws, message):
    if isinstance(message, bytes) and message[0] == 8:
        try:
            sec_id = struct.unpack('<I', message[4:8])[0]
            ltp = round(struct.unpack('<f', message[8:12])[0], 2)
            cum_vol = struct.unpack('<I', message[24:28])[0]
            process_volume(sec_id, ltp, cum_vol)
        except:
            pass

def run_ws():
    auth_url = f"wss://api-feed.dhan.co?version=2&token={DHAN_ACCESS_TOKEN}&clientId={DHAN_CLIENT_ID}&authType=2"
    def on_open(ws):
        print("🌐 WebSocket Connected.")
        for i in range(0, len(SIDS_LIST), 100):
            chunk = SIDS_LIST[i:i+100]
            sub_data = {
                "RequestCode": 21,
                "InstrumentCount": len(chunk),
                "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": s} for s in chunk]
            }
            ws.send(json.dumps(sub_data))

    ws = websocket.WebSocketApp(auth_url, on_message=on_message, on_open=on_open)
    ws.run_forever()

if __name__ == "__main__":
    fetch_and_build_list()
    if SIDS_LIST:
        run_ws()
