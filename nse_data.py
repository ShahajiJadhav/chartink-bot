import os, asyncio, pytz, threading, json, websocket, struct, requests, time, re, pandas as pd
from io import StringIO
from collections import deque
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
# --- CONFIG ---
SIGNAL_AMOUNT = float(os.getenv("SIGNAL_AMOUNT"))
DHAN_CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
IST = pytz.timezone("Asia/Kolkata")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# --- PARAMETERS ---
VOL_5MIN_THRESHOLD_CR = 40.0
COOLDOWN_SECONDS = 500 
CR_UNIT = 10_000_000

# --- STATE ---
alert_cooldowns = {}
volume_history = {} 
ID_TO_SYMBOL = {}
SIDS_LIST = []
packets_received = 0


# ============================================================= #
#                INSTRUMENT FETCH & FILTERING                   #
# ============================================================= #
EXCLUDED_SYMBOLS = {
    "M&M", "BEL", "JISLJALEQS", "ABCAPITAL", "HDFCLIFE", "NSLNISP", "ASIANPAINT", "HEROMOTOCO","IDEA","SBIN","CANB","CENTRALBK","BANKBARODA","IDBI"
    "NATIONALUM", "NMDC", "SAMMAANCAP", "NESTLEIND", "IDBI", "JIOFIN", "GAEL", "ITC", "FMCGIETF","HDFCGOLD",
    "MON100", "PSUBNKBEES", "SILVERBEES", "ITBEES", "ITIETF", "NIFTYBEES", "CONSUMBEES", "ALPHA",
    "AUTOBEES", "MASPTOP50", "SILVERETF", "SILVERIETF", "LTF", "HNGSNGBEES", "SOUTHBANK", "HINDALCO","ICICIPRULI","TATSILV"
    "IRB", "TECHM", "SAIL", "POWERGRID", "CANB", "IREDA", "IRCON", "BEML", "AXISBANK", "BANKBEES",
    "BANKIETF", "BANKNIFTY1", "HDFCBANK", "ICICIBANK", "KOTAKBANK", "KTKBANK", "PSUBANK", "PSUBANKADD",
    "PVTBANKADD", "RBLBANK", "UTIBANKETF", "YESBANK", "ETERNAL", "SBIN", "NTPC", "BHEL", "RECLTD",
    "WIPRO", "INFY", "MSUMI", "MOTHERSON", "ABFRL", "DELHIVERY", "RELIANCE", "TCS", "TATACHEM",
    "TATACOMM", "TATACONSUM", "TATAELXSI", "TATAINVEST", "TATAMOTORS", "TATAPOWER", "TATASTEEL","SILVER360","ABSLPSE","SILVER","AXISILVER","SILVERBND","EQUAL50ADD","ESILVER","IT","BFSI","SILVERAG","MOSILVER","SILVERBETA",
    "TATATECH", "Zerodha Nifty 1D Rate Liquid ETF", "ALLCARGO", "HDFCSILVER", "SILVER1", "SILVERADD", "SBISILVER","GOLD360","GOLD360","BSLNIFTY","AXISGOLD","GOLDBND","BBNPPGOLD","ICICIB22","CHOICEGOLD","GOLDADD","TOP10ADD","EGOLD","ELM250","GROWWGOLD","GROWWDEFNC","GROWWRAIL","HDFCNIFBAN","HDFCMID150","HDFCSML250","HDFCSENSEX","IVZINGOLD","GOLD1","MIDCAP","LICMFGOLD","MIDSMALL","MAFANG","LIQUID","MAKEINDIA","METAL","NEXT50","SMALLCAP","MOGOLD","MOM100","MONQ50","MONIFTY500","MOMENTUM50","MOCAPITAL","MODEFENCE","MOREALTY","MOSMALL250","MOVALUE","QGOLDHALF","SBIBPB","SENSEXBETA","BANKBETA","NEXT50BETA","UNIONGOLD","GOLDBETA","TATAGOLD"
}

# EXCLUDED_SYMBOLS =  {os.getenv("EXCLUDED_SYMBOLS")}

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

        print("📊 Fetching leverage sheet...")
        sheet_url = "https://docs.google.com/spreadsheets/d/1zqhM3geRNW_ZzEx62y0W5U2ZlaXxG-NDn0V8sJk5TQ4/gviz/tq?tqx=out:csv&gid=1663719548"
        lev_df = pd.read_csv(sheet_url)
        
        # Identify symbol column dynamically
        symbol_col = lev_df.columns[lev_df.columns.get_loc("Sr.") + 1]
        lev_df = lev_df.rename(columns={symbol_col: "Symbol"})
        lev_df["Symbol"] = lev_df["Symbol"].astype(str).str.upper().str.strip()
        lev_df["MIS"] = pd.to_numeric(lev_df["MIS (Intraday)"].astype(str).str.replace("x","",case=False), errors="coerce")

        mis_df = lev_df[lev_df["MIS"] >= 5][["Symbol"]].copy()
        mis_df = mis_df[~mis_df["Symbol"].str.contains("BEES|ETF|CASE", case=False, na=False)]
        mis_df = mis_df[~mis_df["Symbol"].isin(EXCLUDED_SYMBOLS)]

        final_df = mis_df.merge(inst_df, on="Symbol", how="inner")
        potential_sids = final_df["SECURITY_ID"].tolist()
        sid_to_symbol_map = dict(zip(final_df['SECURITY_ID'], final_df['Symbol']))

        print(f"🔍 Checking LTP for {len(potential_sids)} candidates...")
        filtered_data = []
        for i in range(0, len(potential_sids), 1000):
            chunk = potential_sids[i:i+1000]
            q_resp = requests.post("https://api.dhan.co/v2/marketfeed/ltp", headers=headers, json={"NSE_EQ": chunk}, timeout=10)
            if q_resp.status_code == 200:
                market_data = q_resp.json().get('data', {}).get('NSE_EQ', {})
                for sid_key, details in market_data.items():
                    ltp = details.get('last_price', 0)
                    if 5 <= ltp <= 1500:
                        sid_int = int(sid_key)
                        filtered_data.append({"SECURITY_ID": sid_int, "Symbol": sid_to_symbol_map.get(sid_int, "Unknown")})
            time.sleep(1.2)

        if filtered_data:
            valid_df = pd.DataFrame(filtered_data)
            ID_TO_SYMBOL = pd.Series(valid_df.Symbol.values, index=valid_df.SECURITY_ID).to_dict()
            SIDS_LIST = valid_df["SECURITY_ID"].astype(str).tolist()
            print(f"✅ Setup Complete: Subscribing to {len(SIDS_LIST)} stocks.")
        else:
            print("❌ No stocks matched criteria.")

    except Exception as e:
        print(f"❌ Error during setup: {e}")

# ============================================================= #
#                       FEED & ANALYTICS                        #
# ============================================================= #

def send_telegram(msg):
    if not TELEGRAM_TOKEN:
        print("❌ ERROR: Telegram Token is NULL. Check your .env/Secrets.")
        return
    # Ensure the token doesn't already start with 'bot' (common mistake)
    token = TELEGRAM_TOKEN.replace("bot", "")
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }
    try:
        resp = requests.post(url, data=payload, timeout=8)
        if resp.status_code == 200:
            print("📤 Telegram message sent successfully.")
        else:
            print(f"❌ Telegram API Error: {resp.status_code} - {resp.text}")
            print(f"Target URL (Masked): https://api.telegram.org/bot{token[:5]}.../sendMessage")
    except Exception as e:
        print(f"❌ Telegram Connection Error: {e}")
        
'''
def process_volume(sec_id, ltp, cum_vol):
    now = time.time()
    if sec_id not in volume_history:
        volume_history[sec_id] = deque()

    volume_history[sec_id].append((now, cum_vol))
    
    # Keep only 5 mins of data
    while volume_history[sec_id] and (now - volume_history[sec_id][0][0] > 300):
        volume_history[sec_id].popleft()

    if len(volume_history[sec_id]) > 1:
        start_vol = volume_history[sec_id][0][1]
        delta_qty = cum_vol - start_vol
        traded_value_cr = (delta_qty * ltp) / CR_UNIT

        if traded_value_cr >= VOL_5MIN_THRESHOLD_CR:
            last_alert = alert_cooldowns.get(sec_id, 0)
            
            # CHECK COOLDOWN FIRST
            if now - last_alert > COOLDOWN_SECONDS:
                # UPDATE IMMEDIATELY to block near-simultaneous packets
                alert_cooldowns[sec_id] = now 
                
                symbol = ID_TO_SYMBOL.get(sec_id, f"ID:{sec_id}")
                qty = int((SIGNAL_AMOUNT * 5) // ltp)
                msg = f"🔥VOLUME SPIKE\nStock: {symbol} QTY: {qty} \n*5-Min Vol:* ₹{traded_value_cr:.2f} Cr\n*Price:* {ltp}"
                
                print(f"Alert: {symbol} - {traded_value_cr:.2f} Cr")
                threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()
'''

def process_volume(sec_id, ltp, cum_vol):
    now = time.time()
    
    # 1. Initialize history if new security
    if sec_id not in volume_history:
        volume_history[sec_id] = deque()

    # 2. Append current data point
    volume_history[sec_id].append((now, cum_vol))
    
    # 3. Sliding Window: Keep only the last 5 minutes (300 seconds) of data
    while volume_history[sec_id] and (now - volume_history[sec_id][0][0] > 300):
        volume_history[sec_id].popleft()

    # 4. Check for Spike
    if len(volume_history[sec_id]) > 1:
        start_vol = volume_history[sec_id][0][1]
        delta_qty = cum_vol - start_vol
        traded_value_cr = (delta_qty * ltp) / CR_UNIT

        # 5. Evaluate Threshold
        if traded_value_cr >= VOL_5MIN_THRESHOLD_CR:
            last_alert_time = alert_cooldowns.get(sec_id, 0)
            
            # 6. Check Cooldown (500 seconds)
            if now - last_alert_time > COOLDOWN_SECONDS:
                
                # --- CRITICAL FIX ---
                # We update the dictionary IMMEDIATELY.
                # This "closes the gate" for any other packets arriving 
                # while the Telegram message is still being prepared.
                alert_cooldowns[sec_id] = now 
                # ---------------------

                # 7. Prepare message details
                symbol = ID_TO_SYMBOL.get(sec_id, f"ID:{sec_id}")
                # Simple QTY calculation based on your SIGNAL_AMOUNT
                qty = int((SIGNAL_AMOUNT * 5) // ltp)
                
                msg = (
                    f"VOL SPIKE- {symbol}, Qty: {qty}\n"
                    f"Vol: ₹{traded_value_cr:.2f} Cr"
                )
                print(f"🚀 Alert Triggered: {symbol} | Vol: {traded_value_cr:.2f} Cr")
                threading.Thread(target=send_telegram, args=(msg,), daemon=True).start()


def on_message(ws, message):
    global packets_received
    if isinstance(message, bytes) and message[0] == 8:
        packets_received += 1
        try:
            # Correct Dhan offsets for Full Packet (Code 8)
            sec_id = struct.unpack('<I', message[4:8])[0]
            ltp = round(struct.unpack('<f', message[8:12])[0], 2)
            # Volume usually starts at byte 22 or 23 for Code 8
            cum_vol = struct.unpack('<I', message[22:26])[0] 
            process_volume(sec_id, ltp, cum_vol)
        except:
            pass

def heartbeat():
    """Prints status every minute to keep GitHub Action logs alive."""
    while True:
        time.sleep(60)
        print(f"💓 Heartbeat: {datetime.now(IST).strftime('%H:%M:%S')} | Packets Recv: {packets_received} | Monitoring: {len(SIDS_LIST)}")

def run_ws():
    auth_url = f"wss://api-feed.dhan.co?version=2&token={DHAN_ACCESS_TOKEN}&clientId={DHAN_CLIENT_ID}&authType=2"
    
    def on_open(ws):
        print("🌐 WebSocket Connected. Subscribing to instruments...")
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
    print(f"🎬 Script Started at {datetime.now(IST)}")
    threading.Thread(target=heartbeat, daemon=True).start()
    
    # Loop until we actually find stocks or market closes
    while not SIDS_LIST:
        fetch_and_build_list()
        if not SIDS_LIST:
            print("Refetching in 30 seconds...")
            time.sleep(30)

    # Now enter the WebSocket loop
    while True:
        if datetime.now(IST).time() > datetime.strptime("15:30:00", "%H:%M:%S").time():
            print("🏁 Market Closed. Exiting script.")
            break
        try:
            run_ws()
        except Exception as e:
            print(f"⚠️ Socket error: {e}. Reconnecting...")
            time.sleep(5)
