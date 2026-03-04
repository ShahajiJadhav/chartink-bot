import os, asyncio, pandas as pd, pytz, threading, json, websocket, struct, requests, time, re
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
SIGNAL_AMOUNT = float(os.getenv("SIGNAL_AMOUNT", 0)) * 5
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IST = pytz.timezone("Asia/Kolkata")

# --- PARAMETERS ---
THRESHOLD_CR = 40000000 
COOLDOWN_SECONDS = 600     
ID_TO_SYMBOL = {}
SIDS_LIST = []
alert_cooldowns = {}

# ================= EXCLUSIONS ================= #
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
    
    headers = {
        'access-token': DHAN_ACCESS_TOKEN, 
        'client-id': DHAN_CLIENT_ID, 
        'Content-Type': 'application/json'
    }
    
    try:
        # 1. Instrument Master
        inst_url = "https://api.dhan.co/v2/instrument/NSE_EQ"
        resp = requests.get(inst_url, headers={'access-token': DHAN_ACCESS_TOKEN})
        inst_df = pd.read_csv(StringIO(resp.text))
        inst_df = inst_df[(inst_df["EXCH_ID"] == "NSE") & (inst_df["SEGMENT"] == "E") & (inst_df["INSTRUMENT_TYPE"] == "ES")]
        inst_df = inst_df[["SECURITY_ID", "UNDERLYING_SYMBOL"]].rename(columns={"UNDERLYING_SYMBOL": "Symbol"})
        inst_df["Symbol"] = inst_df["Symbol"].str.upper().str.strip()

        # 2. Leverage Sheet
        sheet_url = "https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:csv&gid=1663719548"
        lev_df = pd.read_csv(sheet_url)
        symbol_col = lev_df.columns[list(lev_df.columns).index("Sr.") + 1]
        lev_df = lev_df.rename(columns={symbol_col: "Symbol"})
        lev_df["Symbol"] = lev_df["Symbol"].astype(str).str.upper().str.strip()
        lev_df["MIS"] = pd.to_numeric(lev_df["MIS (Intraday)"].astype(str).str.replace("x","",regex=False).str.replace("X","",regex=False), errors="coerce")
        
        # 3. Filter
        mis_df = lev_df[lev_df["MIS"] >= 5][["Symbol", "MIS"]].copy()
        exclude_pattern = re.compile(r"(BEES|ETF|CASE)", re.IGNORECASE)
        mis_df = mis_df[~mis_df["Symbol"].str.contains(exclude_pattern, na=False)]
        mis_df = mis_df[~mis_df["Symbol"].isin(EXCLUDED_SYMBOLS)]
        
        final_df = mis_df.merge(inst_df, on="Symbol", how="inner")
        sid_to_symbol_map = dict(zip(final_df['SECURITY_ID'], final_df['Symbol']))
        potential_sids = final_df["SECURITY_ID"].tolist()

        # 4. LTP Filter (5 to 1500)
        filtered_data = []
        quote_url = "https://api.dhan.co/v2/marketfeed/ltp"
        for i in range(0, len(potential_sids), 1000):
            chunk = potential_sids[i:i+1000]
            q_resp = requests.post(quote_url, headers=headers, json={"NSE_EQ": chunk}, timeout=10)
            if q_resp.status_code == 200:
                market_data = q_resp.json().get('data', {}).get('NSE_EQ', {})
                for sid_key, details in market_data.items():
                    ltp = details.get('last_price', 0)
                    if 5 <= ltp <= 1500:
                        sid_int = int(sid_key)
                        filtered_data.append({"SECURITY_ID": sid_int, "Symbol": sid_to_symbol_map.get(sid_int, "Unknown")})
            time.sleep(1.1)

        if filtered_data:
            valid_df = pd.DataFrame(filtered_data)
            ID_TO_SYMBOL = pd.Series(valid_df.Symbol.values, index=valid_df.SECURITY_ID).to_dict()
            SIDS_LIST = valid_df["SECURITY_ID"].astype(str).tolist()
            print(f"✅ Setup Complete: {len(SIDS_LIST)} stocks ready.")
    except Exception as e:
        print(f"❌ Error during setup: {e}")

# ================= ALERT LOGIC ================= #

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = { "chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML" }
    try:
        requests.post(url, data=payload, timeout=5).raise_for_status()
    except Exception as e:
        print(f"Error sending message: {e}")

def parse_and_alert(message):
    try:
        if len(message) < 10: return
        feed_code = message[0]
        sec_id = struct.unpack('<I', message[4:8])[0]

        # Process Market Depth Feed
        if feed_code == 8 and len(message) >= 162:
            ltp = round(struct.unpack('<f', message[8:12])[0], 2)
            
            bids_amt, asks_amt = [], []
            for i in range(5):
                off = 62 + (i * 20)
                b_q, a_q = struct.unpack('<I', message[off : off+4])[0], struct.unpack('<I', message[off+4 : off+8])[0]
                b_p, a_p = struct.unpack('<f', message[off+12 : off+16])[0], struct.unpack('<f', message[off+16 : off+20])[0]
                bids_amt.append(b_p * b_q)
                asks_amt.append(a_p * a_q)

            max_b, max_a = int(max(bids_amt)), int(max(asks_amt))
            
            # Threshold Check
            if max_b >= THRESHOLD_CR or max_a >= THRESHOLD_CR:
                now = time.time()
                if (now - alert_cooldowns.get(sec_id, 0)) > COOLDOWN_SECONDS:
                    alert_cooldowns[sec_id] = now
                    side = "BUY" if max_b >= THRESHOLD_CR else "SELL"
                    sym = ID_TO_SYMBOL.get(sec_id, f"ID:{sec_id}")
                    qty = int(SIGNAL_AMOUNT / ltp) if ltp > 0 else 0
                    
                    msg = f"<b>OrderBook ALERT</b>Side: {side}\nStock: {sym} LTP: ₹{ltp} Qty: {qty}"
                    threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()
                    print(f"🔔 Alert sent for {sym}")
    except:
        pass

def on_message(ws, message):
    if isinstance(message, bytes):
        parse_and_alert(message)

def on_open(ws):
    print(f"🌐 WebSocket Connected. Subscribing to stocks...")
    for i in range(0, len(SIDS_LIST), 100):
        chunk = SIDS_LIST[i:i+100]
        msg = {"RequestCode": 21, "InstrumentCount": len(chunk), "InstrumentList": [{"ExchangeSegment": "NSE_EQ", "SecurityId": s} for s in chunk]}
        ws.send(json.dumps(msg))
        time.sleep(0.1)

def run_ws():
    url = f"wss://api-feed.dhan.co?version=2&token={DHAN_ACCESS_TOKEN}&clientId={DHAN_CLIENT_ID}&authType=2"
    websocket.WebSocketApp(url, on_message=on_message, on_open=on_open).run_forever()

async def monitor_market_close():
    while True:
        now_ist = datetime.now(IST)
        if now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 30):
            print(f"🕒 Market closed ({now_ist.strftime('%H:%M')}). Shutting down...")
            break
        await asyncio.sleep(30)

async def main():
    fetch_and_build_list()
    if not SIDS_LIST:
        return

    threading.Thread(target=run_ws, daemon=True).start()
    await monitor_market_close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Shutdown.")
