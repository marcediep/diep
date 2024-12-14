import os
import aiohttp
import asyncio
import json
import datetime
import time
import yaml
import pandas as pd
from pytz import timezone
import re

with open('config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)

# Constants
KST = timezone('Asia/Seoul')    
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
TOKEN_FILE = "access_token.json"
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
URL_BASE = _cfg['URL_BASE']
TOKEN_PATH = "/oauth2/tokenP"
semaphore = asyncio.Semaphore(20)
lock = asyncio.Lock()
file_name = os.path.splitext(os.path.basename(__file__))[0]

# 매매 종목 리스트
def load_symbol_list():
    symbol_list = []
    with open('symbol_list.txt', 'r', encoding='UTF-8') as f:
        for line in f:
            parts = line.strip().split('/')
            if parts:  # 비어 있지 않은 라인만 추가
                symbol_list.append(parts[0])  # 종목 코드만 추가
    return symbol_list

# 매매 종목 Data
def load_symbol_data():
    low_symbol_prices = {}
    high_symbol_prices = {}
    trade_types = {}
    with open('symbol_list.txt', 'r', encoding='UTF-8') as f:
        for line in f:
            parts = line.strip().split('/')
            if len(parts) >= 4:
                symbol = parts[0]
                price1 = float(parts[1])
                price2 = float(parts[2])
                low_symbol_prices[symbol] = price1
                high_symbol_prices[symbol] = price2
                types = parts[3]
                trade_types[symbol] = types
    return low_symbol_prices, high_symbol_prices, trade_types

# 매매 종목 Data 호출
async def get_symbol_datas(sym):
    low_symbol_prices, high_symbol_prices, trade_types = await asyncio.to_thread(load_symbol_data)
    low_price = low_symbol_prices.get(sym)
    high_price = high_symbol_prices.get(sym)
    trade_type = trade_types.get(sym)
    return low_price, high_price, trade_type

# ANSI 코드 및 Discord 메시지 포맷 제거 함수
def clean_message(msg):
    ansi_escape = re.compile(r'\x1b[^m]*m')
    cleaned_msg = ansi_escape.sub('', msg)  # ANSI 코드 제거
    cleaned_msg = re.sub(r'```.*?\n', '', cleaned_msg)  # 코드 블록 시작
    cleaned_msg = re.sub(r'```', '', cleaned_msg)  # 코드 블록 끝
    cleaned_msg = re.sub(r'\*\*', '', cleaned_msg)  # ** 제거
    return cleaned_msg

# Discord 메시지 전송
async def send_message(session, msg, include_time=True):
    now = datetime.datetime.now(KST)
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}" if include_time else str(msg)}
    clean_msg = clean_message(message['content'])
    async with session.post(DISCORD_WEBHOOK_URL, json=message) as res:
        await res.text()
    print(message)
    with open('trade_log.txt', 'a', encoding='UTF-8') as f:
        f.write(f"{clean_msg}\n")

# 토큰 발급
async def get_access_token(session):
    global ACCESS_TOKEN
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, "r") as f:
                token_data = json.load(f)
                access_token = token_data.get("access_token")
                expiry_timestamp = token_data.get("expiry_timestamp")

                if expiry_timestamp and expiry_timestamp > time.time():
                    ACCESS_TOKEN = access_token
                    return access_token, token_data
                else:
                    await send_message(session, "**```cs\n[Info] Token expired or missing. Refreshing...```**")
        else:
            await send_message(session, "**```cs\n[Info] Token file does not exist. Requesting new token...```**")

        headers = {"Content-Type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET
        }
        url = f"{URL_BASE}{TOKEN_PATH}"
        async with session.post(url, headers=headers, json=body) as response:
            if response.status == 200:
                response_data = await response.json()
                ACCESS_TOKEN = response_data.get("access_token")
                expiry_seconds = response_data.get("expires_in", 72000)
                expiry_timestamp = time.time() + expiry_seconds
                token_data = {
                    "access_token": ACCESS_TOKEN,
                    "expiry_timestamp": expiry_timestamp
                }
                with open(TOKEN_FILE, "w") as f:
                    json.dump(token_data, f, indent=4)
                return ACCESS_TOKEN, token_data
            else:
                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Failed to retrieve access token: {response.status}, {await response.text()}```**")
                return None, None
    except Exception as e:
        await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception in get_access_token: {e}```**")
        return None, None

# 공통 헤더
def get_headers(tr_id, custtype):
    return {
        "Content-Type": "application/json",
        "authorization": f"Bearer {ACCESS_TOKEN}",
        "appKey": APP_KEY,
        "appSecret": APP_SECRET,
        "tr_id": tr_id,
        "custtype": custtype
    }

# 암호화
async def hashkey(session, datas):
    try:
        PATH = "/uapi/hashkey"
        URL = f"{URL_BASE}{PATH}"
        headers = get_headers("hashkey", "")
        async with session.post(URL, headers=headers, data=json.dumps(datas)) as res:
            res_json = await res.json()
            hashkey = res_json["HASH"]
            return hashkey
    except Exception as e:
        await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception in hashkey: {e}```**")
        return None

# Retry single request
async def retry_request(session, method, url, retries=10, delay=2, **kwargs):
    for attempt in range(retries):
        try:
            async with session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, ConnectionResetError) as e:
            if attempt < retries - 1:
                await asyncio.sleep(delay)
            else:
                raise e

# Retry session
async def ensure_active_session(session, retries=10, delay=1):
    for _ in range(retries):
        if not session.closed:
            return session
        await asyncio.sleep(delay)
        session = aiohttp.ClientSession()
    return session

# Fetch Data
async def fetch_data(session, path, headers, params, response_key, sub_key=None):
    async with semaphore:
        try:
            #await asyncio.sleep(0.05)
            URL = f"{URL_BASE}/{path}"
            async with session.get(URL, headers=headers, params=params) as res:
                res_json = await res.json()
                if res.status == 200 and response_key in res_json:
                    data = res_json.get(response_key)
                    if isinstance(data, list) and len(data) > 0:
                        if sub_key:
                            return int(data[0].get(sub_key, 0))
                        else:
                            return int(data[0])
                    elif isinstance(data, dict):
                        return int(data.get(sub_key, 0)) if sub_key else int(data)
                    else:
                        await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Data format error in fetch_data: {data}```**")
                        return None
                else:
                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Fetching data failed: {res.status}, {await res.text()}```**")
                    return None
        except Exception as e:
            await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception in fetch_data: {e}```**")
            return None

# Batch
async def process_symbol_batches(session, symbols, batch_size=10):
    batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
    all_results = []

    for batch in batches:
        tasks = [fetch_price_info(session, sym) for sym in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, result in zip(batch, batch_results):
            if isinstance(result, Exception):
                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Failed to fetch data for {sym}: {result}```**")
            else:
                all_results.append((sym, result))
        await asyncio.sleep(0.05)  # Batch 간 대기 시간
    return all_results

# 주문 (매수)
async def buy(session, code, qty, use_credit=False, use_limit=False, price=None):
    if use_credit and use_limit and price is not None:
        PATH = "/uapi/domestic-stock/v1/trading/order-credit"
        tr_id = "TTTC0852U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": "00",  # 지정가
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": str(price),  # 지정가 가격
            "CRDT_TYPE": "21",
            "RVSN_ORD_YN": "N",
            "LOAN_DT": datetime.datetime.now(KST).strftime('%Y%m%d')
        }
    elif use_credit:
        PATH = "/uapi/domestic-stock/v1/trading/order-credit"
        tr_id = "TTTC0852U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": "0",
            "CRDT_TYPE": "21",
            "RVSN_ORD_YN": "N",
            "LOAN_DT": datetime.datetime.now(KST).strftime('%Y%m%d')
        }
    elif use_limit and price is not None:
        PATH = "/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "TTTC0802U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": "00",  # 지정가
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": str(price),  # 지정가 가격
        }
    else:
        PATH = "/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "TTTC0802U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": "0",
        }

    headers = get_headers(tr_id, "P")
    headers["hashkey"] = await hashkey(session, data)
    async with session.post(f"{URL_BASE}{PATH}", headers=headers, json=data) as res:
        res_json = await res.json()
        if res_json['rt_cd'] == '0':
            await send_message(session, f"**```ansi\n\u001b[0;36m[매수 주문 성공] {str(res_json)}```**")
            return True
        else:
            await send_message(session, f"**```ansi\n\u001b[0;35m[매수 주문 실패] {str(res_json)}```**")
            return False


# 신용매수 가능 여부 조회
async def psbl_credit(session, code):
    async with semaphore:
        try:
            PATH = "/uapi/domestic-stock/v1/trading/inquire-credit-psamount"
            URL = f"{URL_BASE}{PATH}"
            headers = get_headers("TTTC8909R", "P")
            params = {
                "CANO": CANO,
                "ACNT_PRDT_CD": ACNT_PRDT_CD,
                "PDNO": code,
                "ORD_UNPR": "0",
                "ORD_DVSN": "00",
                "CRDT_TYPE": "21",
                "CMA_EVLU_AMT_ICLD_YN": "N",
                "OVRS_ICLD_YN": "N"
            }
            async with session.get(URL, headers=headers, params=params) as res:
                if res.status == 200:
                    res_json = await res.json()
                    max_buy_amt = int(res_json['output']['max_buy_amt'])
                    credit_available = max_buy_amt >= 30000000
                    return credit_available
                else:
                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Failed to check credit purchase availability for {code}: {res.status}, {await res.text()}```**")
                    return False
        except Exception:
            return False

# 종목명 조회
async def get_name(session, code=""):
    async with semaphore:
        try:
            PATH = "/uapi/domestic-stock/v1/quotations/search-stock-info"
            URL = f"{URL_BASE}/{PATH}"
            headers = get_headers("CTPF1002R", "P")
            params = {
                "PDNO": code,
                "PRDT_TYPE_CD": "300",
            }
            async with session.get(URL, headers=headers, params=params) as res:
                if res.status == 200:
                    res_json = await res.json()
                    return res_json['output']['prdt_abrv_name']
                else:
                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Failed to get name for {code}: {res.status}, {await res.text()}```**")
                    return None
        except Exception as e:
            await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception in get_name: {e}```**")
            return None

# 주가 조회
async def get_price_info(session, code, price_type):
    async with semaphore:
        try:
            paths = {
                'c': ("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100", 'output', 'stck_prpr'),
                'h': ("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100", 'output', 'stck_hgpr'),
                'sd': ("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100", 'output', 'stck_sdpr')
            }
            if price_type not in paths:
                raise ValueError(f"Invalid price_type: {price_type}. Valid options are {list(paths.keys())}")
            path, tr_id, response_key, sub_key = paths[price_type]
            headers = get_headers(tr_id, "")
            params = {
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd": code,
            }
            if price_type == 'o':
                params.update({
                    "fid_org_adj_prc": "0",
                    "fid_period_div_code": "D"
                })
            return await fetch_data(session, path, headers, params, response_key, sub_key)
        except Exception as e:
            await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception in get_price_info: {e}```**")
            return None

# 주가 gather
async def fetch_price_info(session, sym):
    try:
        async with semaphore:
            tasks = [
                get_price_info(session, sym, 'c'),
                get_symbol_datas(sym)
            ]
            current_price, (low_price, high_price, trade_type) = await asyncio.gather(*tasks, return_exceptions=True)
            return current_price, low_price, high_price, trade_type
    except Exception as e:
        await send_message(session, f"**```ansi\n\u001b[0;31m[Error in fetch_price_info for {sym}: {str(e)}```**")
        return None, None, None, None

# Cut price
cut_price_dict = {} 
async def update_cut_price(session, avg_price, high_price, low_price, prdt_name, sym, second_bought_list, trade_type, cut_price_dict):
    try:
        last_cut_price = cut_price_dict.get(sym)
        if trade_type == 'MR':
            if high_price is None or low_price is None:  
                raise ValueError(f"Invalid price data for {prdt_name}({sym}) high_price: {high_price}, low_price: {low_price}")
            cut_price = avg_price - ((high_price - low_price) / 3 if sym in second_bought_list else (high_price - low_price) / 2 )
        elif trade_type == 'MR2':
            if high_price is None or low_price is None:  
                raise ValueError(f"Invalid price data for {prdt_name}({sym}) high_price: {high_price}, low_price: {low_price}")
            cut_price = avg_price - (high_price - low_price) / 3
        else:
            cut_price = avg_price * 0.92
        if last_cut_price != cut_price:
            await send_message(session, f"**```cs\n[{prdt_name}({sym}/{trade_type})] 평균가({avg_price}), 최고가({high_price}), 최저가({low_price}), 손절가({cut_price})```**")
            cut_price_dict[sym] = cut_price
    except Exception as e:
        await send_message(session, f"**```ansi\n\u001b[0;31m[Error in updating cut price for {sym}] {str(e)}```**")

# 주문 (매도)
async def sell(session, code, qty, loan_dt=None):
    if loan_dt:
        PATH = "/uapi/domestic-stock/v1/trading/order-credit"
        tr_id = "TTTC0851U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": "01", 
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": "0",
            "CRDT_TYPE": "25",
            "RVSN_ORD_YN": "N",
            "LOAN_DT": loan_dt
        }
    else:
        PATH = "/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "TTTC0801U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": "01",
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": "0",
        }

    headers = get_headers(tr_id, "P")
    headers["hashkey"] = await hashkey(session, data)
    async with session.post(f"{URL_BASE}{PATH}", headers=headers, json=data) as res:
        res_json = await res.json()
        if res_json['rt_cd'] == '0':
            await send_message(session, f"**```ansi\n\u001b[0;36m[매도 주문 성공] {str(res_json)}```**")
            return True
        else:
            await send_message(session, f"**```ansi\n\u001b[0;35m[매도 주문 실패] {str(res_json)}```**")
            return False

# 신용 분배 매도
async def advanced_sell(session, code, total_sell_qty):
    loan_data = loan_dt_qty_dict.get(code, {})   
    if loan_data:
        total_holding_qty = sum(loan_data.values())
        for loan_dt, qty in loan_data.items():
            sell_qty = round(total_sell_qty * (qty / total_holding_qty))
            if sell_qty > 0:
                result = await sell(session, code, sell_qty, loan_dt=loan_dt)
                if not result:
                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error] 신용매도 주문 실패 {code}({sell_qty}주) 대출일자({loan_dt})```**")
            await asyncio.sleep(0.01) 
    else: 
        if total_sell_qty > 0:
            result = await sell(session, code, total_sell_qty)
            if not result:
                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] 매도 주문 실패 {code}({total_sell_qty}주)```**")

# 주식 잔고 조회
async def get_stock_balance(session):
    timeout = aiohttp.ClientTimeout(total=10)
    retry_count = 10
    async with semaphore:
        global loan_dt_qty_dict
        loan_dt_qty_dict = {}
        stock_dict = {}

        for inqr_dvsn in ["01", "02"]:  # "01": 신용, "02": 현금
            for attempt in range(retry_count):
                try:
                    #await asyncio.sleep(0.05)
                    PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
                    URL = f"{URL_BASE}{PATH}"
                    headers = get_headers("TTTC8434R", "P")
                    params = {
                        "CANO": CANO,
                        "ACNT_PRDT_CD": ACNT_PRDT_CD,
                        "AFHR_FLPR_YN": "N",
                        "OFL_YN": "",
                        "INQR_DVSN": inqr_dvsn,
                        "UNPR_DVSN": "01",
                        "FUND_STTL_ICLD_YN": "N",
                        "FNCG_AMT_AUTO_RDPT_YN": "N",
                        "PRCS_DVSN": "01",
                        "CTX_AREA_FK100": "",
                        "CTX_AREA_NK100": ""
                    }
                    async with session.get(URL, headers=headers, params=params, timeout=timeout) as res:
                        res_json = await res.json()
                        stock_list = res_json.get('output1', [])

                        for stock in stock_list:
                            if int(stock['hldg_qty']) > 0:
                                pdno = stock['pdno']  # 종목 코드
                                hldg_qty = int(stock['hldg_qty'])  # 보유 수량
                                ord_psbl_qty = int(stock['ord_psbl_qty'])  # 주문 가능 수량
                                pchs_avg_pric = float(stock['pchs_avg_pric'])  # 평균 매수가
                                prdt_name = stock['prdt_name']  # 상품명
                                loan_dt = stock.get('loan_dt') if inqr_dvsn == "01" else None  # 신용 매수일

                                stock_dict[pdno] = {
                                    'hldg_qty': hldg_qty,
                                    'ord_psbl_qty': ord_psbl_qty,
                                    'pchs_avg_pric': pchs_avg_pric,
                                    'pdno': pdno,
                                    'prdt_name': prdt_name,
                                    'loan_dt': loan_dt
                                }

                                if loan_dt:
                                    loan_dt_qty_dict.setdefault(pdno, {})[loan_dt] = ord_psbl_qty
                    break
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    await send_message(session, f"**```ansi\n\u001b[0;31m[Attempt {attempt + 1}] Error in get_stock_balance ({inqr_dvsn}): {e}```**")
                    await asyncio.sleep(1)
                    continue
            else:
                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Failed to get stock balance ({inqr_dvsn}) after retries```**")
        return stock_dict

# 메인 함수
async def main():
    try:
        global ACCESS_TOKEN
        conn = aiohttp.TCPConnector(limit=10, limit_per_host=3)
        async with aiohttp.ClientSession(connector=conn) as session:
            ACCESS_TOKEN, token_data = await get_access_token(session)
            if not ACCESS_TOKEN:
                await send_message(session, "**```ansi\n\u001b[0;31m[Error] Failed to get ACCESS_TOKEN. Exiting...```**")
                return
            
            BUY_AMOUNT = 15000000
            first_bought_list = []
            second_bought_list = []
            soldout_dict = {}
            symbol_list = load_symbol_list()
            last_sell_prices = {}
            cut_price_dict = {}
            name_dict = {}

            stock_dict = await get_stock_balance(session)
            for sym in stock_dict.keys():
                first_bought_list.append(sym)
                last_sell_prices[sym] = None

            await send_message(session, f"**```ansi\n\u001b[0;37m==  Trading Bot Start ({file_name})  ==```**", include_time=True)

            while True:
                session = await ensure_active_session(session)
                t_now = datetime.datetime.now(KST)
                if time.time() > (token_data.get("expiry_timestamp") - 600):
                    try:
                        ACCESS_TOKEN, token_data = await get_access_token(session)
                        if not ACCESS_TOKEN:
                            await send_message(session, "**```ansi\n\u001b[0;31m[Error] Failed to refresh ACCESS_TOKEN. Exiting...```**")
                            return
                    except Exception as e:
                        await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception while refreshing ACCESS_TOKEN: {e}```**")
                        return

                t_start = t_now.replace(hour=9, minute=0, second=0, microsecond=0)
                t_cut = t_now.replace(hour=15, minute=20, second=10, microsecond=0)
                t_exit = t_now.replace(hour=15, minute=20, second=30, microsecond=0)
                symbol_list = load_symbol_list()
                #semaphore = asyncio.Semaphore(min(3, max(1, len(symbol_list) // 4)))

                if t_start < t_now < t_exit:  # 매수
                    for sym in symbol_list:
                        if (sym in second_bought_list) or (sym in soldout_dict and soldout_dict[sym]):
                            continue
                        if not sym or not isinstance(sym, str):
                            continue
                        
                        try:
                            batch_results = await process_symbol_batches(session, [sym], batch_size=10)
                            if not batch_results:
                                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] No batch results for {sym}```**")
                                continue
                            batch_results_dict = {sym: result for sym, result in batch_results}
                            if sym not in batch_results_dict:
                                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] No data fount in batch results for {sym}```**")
                                continue
                            result = batch_results_dict[sym]
                            current_price, low_price, high_price, trade_type = result
                            
                            avg_price = None
                            if sym in stock_dict:
                                avg_price = stock_dict[sym]['pchs_avg_pric']
                            if trade_type == "MR":
                                MR_price1 = high_price - ((high_price - low_price) / 3)
                                MR_price2 = MR_price1 - ((high_price - low_price) / 3)
                                if avg_price is not None:
                                    MR_price2 = avg_price - ((high_price - low_price) / 3)
                                else:
                                    MR_price2 = None
                            
                            name = name_dict.get(sym)
                            if not name:
                                name = await get_name(session, sym)
                                if name:
                                    name_dict[sym] = name
                                else:
                                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Failed to fetch name for symbol {sym}```**")
                                    continue
                        except Exception as e:
                            await send_message(session, f"**```ansi\n\u001b[0;31m[Error in getting trade info for ({sym})] {str(e)}```**")
                            continue
                        
                        if trade_type == 'MR' and t_start < t_now < t_exit:
                            if sym not in first_bought_list:
                                if current_price <= MR_price1:
                                    try:
                                        buy_qty = int(BUY_AMOUNT // (current_price * 1.000036396))
                                        #buy_qty = int(500000 // (current_price * 1.000036396))
                                        if buy_qty > 0:
                                            async with lock:
                                                credit_available = await psbl_credit(session, sym)
                                                if credit_available:
                                                    await send_message(session, f"**```cs\n목표가 도달({format(MR_price1, ',')})원 ▶ {name}({sym}/{trade_type}) {format(current_price, ',')}원 {format(buy_qty, ',')}주 신용매수 주문```**")
                                                    result = await buy(session, sym, buy_qty, use_credit=True, use_limit=True, price=current_price)
                                                else:
                                                    await send_message(session, f"**```cs\n목표가 도달({format(MR_price1, ',')})원 ▶ {name}({sym}/{trade_type}) {format(current_price, ',')}원 {format(buy_qty, ',')}주 매수 주문```**")
                                                    result = await buy(session, sym, buy_qty, use_limit=True, price=current_price)
                                                await asyncio.sleep(0.05)
                                                if result:
                                                    first_bought_list.append(sym)
                                                    soldout_dict[sym] = False
                                                    stock_dict = await get_stock_balance(session)
                                    except Exception as e:
                                        await send_message(session, f"**```ansi\n\u001b[0;31m[Error in Buy MT T1 {name}({sym})] {e}```**")                                                                              
                            if MR_price2 is not None and sym not in second_bought_list:
                                if current_price <= MR_price2:
                                    try:
                                        buy_qty = int(BUY_AMOUNT // (current_price * 1.000036396))
                                        #buy_qty = int(500000 // (current_price * 1.000036396))
                                        if buy_qty > 0:
                                            async with lock:
                                                credit_available = await psbl_credit(session, sym)
                                                if credit_available:
                                                    await send_message(session, f"**```cs\n목표가 도달({format(MR_price2, ',')})원 ▶ {name}({sym}/{trade_type}) {format(current_price, ',')}원 {format(buy_qty, ',')}주 신용매수 주문```**")
                                                    result = await buy(session, sym, buy_qty, use_credit=True, use_limit=True, price=current_price)
                                                else:
                                                    await send_message(session, f"**```cs\n목표가 도달({format(MR_price2, ',')})원 ▶ {name}({sym}/{trade_type}) {format(current_price, ',')}원 {format(buy_qty, ',')}주 매수 주문```**")
                                                    result = await buy(session, sym, buy_qty, use_limit=True, price=current_price)
                                                await asyncio.sleep(0.05)
                                                if result:
                                                    second_bought_list.append(sym)
                                                    await send_message(session, f"**```cs\nMT Second Bought List({second_bought_list})```**")
                                                    soldout_dict[sym] = False
                                                    stock_dict = await get_stock_balance(session)
                                                    avg_price = stock_dict[sym]['pchs_avg_pric']
                                    except Exception as e:
                                        await send_message(session, f"**```ansi\n\u001b[0;31m[Error in Buy MT T2 {name}({sym})] {e}```**")                                                                      

                        await asyncio.sleep(0.05)
                        
                if t_start < t_now < t_exit:  # 매도
                    stock_dict = await get_stock_balance(session)
                    for sym, data in stock_dict.items():         

                        try:
                            if sym not in first_bought_list or sym not in symbol_list:
                                continue
                            if sym in soldout_dict and soldout_dict[sym]:
                                continue
                            batch_results = await process_symbol_batches(session, [sym], batch_size=10)
                            if not batch_results:
                                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] No batch results for {sym}```**")
                                continue
                            batch_results_dict = {sym: result for sym, result in batch_results}
                            if sym not in batch_results_dict:
                                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] No data fount in batch results for {sym}```**")
                                continue
                            result = batch_results_dict[sym]
                            current_price, low_price, high_price, trade_type = result
                            avg_price = data['pchs_avg_pric']
                            qty = data['ord_psbl_qty']
                            prdt_name = data['prdt_name']
                            if avg_price is None or qty is None or qty <=0 or prdt_name is None:
                                continue

                            if sym in last_sell_prices:
                                last_sell_price = last_sell_prices[sym]
                            else:
                                last_sell_price = None
                            prof = (current_price - avg_price) / avg_price * 100
                            loss = (avg_price - current_price) / avg_price * -100
                            await update_cut_price(session, avg_price, high_price, low_price, prdt_name, sym, second_bought_list, trade_type, cut_price_dict)
                            cut_price = cut_price_dict.get(sym)
                            if cut_price is None:
                                continue
                            sell_qty = 0
                            sell_reason = ""

                            if trade_type not in ['LB'] and current_price <= cut_price:
                                sell_qty = qty
                                sell_reason = f"Loss Cut 손절({loss:.2f}%)"
                                last_sell_prices[sym] = None
                            elif trade_type == 'MR3' and t_now >= t_cut:
                                sell_qty = qty
                                sell_reason = f"Time Cut {'익절' if current_price >= avg_price * 1.001872792 else '손절'}({prof if current_price >= avg_price * 1.001872792 else loss:.2f}%)"
                                last_sell_prices[sym] = None
                            elif last_sell_price is None:
                                if current_price >= avg_price * 1.001872792 and ((trade_type == 'MR' and sym in second_bought_list) or trade_type in ['MR2' or 'MR3']):
                                    sell_qty = max(1, int(round(qty / 2)))
                                    sell_reason = f"절반({prof:.2f}%)"
                                    last_sell_prices[sym] = avg_price * 1.031872792
                                elif current_price >= avg_price * 1.031872792:
                                    sell_qty = max(1, int(round(qty / 5)))
                                    sell_reason = f"1차 수익 실현({prof:.2f}%)"
                                    last_sell_prices[sym] = avg_price * 1.041872792
                            elif last_sell_price is not None and current_price >= last_sell_price:
                                if avg_price * 1.031872792 <= last_sell_price < avg_price * 1.041872792:
                                    sell_qty = max(1, int(round(qty / 4)))
                                    sell_reason = f"절반 후 1차 수익 실현({prof:.2f}%)"
                                    last_sell_prices[sym] = avg_price * 1.041872792
                                elif avg_price * 1.041872792 <= last_sell_price < avg_price * 1.051872792:
                                    sell_qty = max(1, int(round(qty / 4)))
                                    sell_reason = f"2차 수익 실현({prof:.2f}%)"
                                    last_sell_prices[sym] = avg_price * 1.051872792
                                elif avg_price * 1.051872792 <= last_sell_price < avg_price * 1.061872792:
                                    sell_qty = max(1, int(round(qty / 3)))
                                    sell_reason = f"3차 수익 실현({prof:.2f}%)"
                                    last_sell_prices[sym] = avg_price * 1.061872792
                                elif avg_price * 1.061872792 <= last_sell_price < avg_price * 1.071872792:
                                    sell_qty = max(1, int(round(qty / 2)))
                                    sell_reason = f"4차 수익 실현({prof:.2f}%)"
                                    last_sell_prices[sym] = avg_price * 1.071872792
                                elif avg_price * 1.071872792 <= last_sell_price:
                                    sell_qty = qty
                                    sell_reason = f"최종 수익 실현({prof:.2f}%)"
                                    last_sell_prices[sym] = None
                            elif last_sell_price is not None:
                                if last_sell_price >= avg_price * 1.041872792 and current_price <= last_sell_price * 0.97:
                                    sell_qty = qty
                                    sell_reason = f"Up & Down 익절({prof:.2f}%)"
                                    last_sell_prices[sym] = None
                                elif last_sell_price >= avg_price * 1.031872792 and current_price <= avg_price * 1.001872792:
                                    sell_qty = qty
                                    sell_reason = f"본절({prof:.2f}%)"
                                    last_sell_prices[sym] = None
                                elif last_sell_price >= avg_price * 1.031872792 and current_price >= avg_price * 1.001872792 and t_now >= t_cut:
                                    sell_qty = qty
                                    sell_reason = f"장 마감 익절({prof:.2f}%)"
                                    last_sell_prices[sym] = None
                            else:
                                sell_qty = 0

                            if sell_qty > 0:
                                try:
                                    await send_message(session, f"**```cs\n{sell_reason} ▶ {prdt_name}({sym}/{trade_type}) {format(current_price, ',')}원 {format(sell_qty, ',')}주 매도 주문```**")
                                    sell_result = await advanced_sell(session, sym, sell_qty)
                                    if sell_result:
                                        if any(reason in sell_reason for reason in ["Loss Cut", "Time Cut", "최종", "본절", "익절"]):
                                            soldout_dict[sym] = True
                                            stock_dict = await get_stock_balance(session)
                                            qty = stock_dict[sym]['ord_psbl_qty'] if sym in stock_dict else 0
                                except Exception as e:
                                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error in Sell {sell_reason} {prdt_name}({sym})] {e}```**")

                        except Exception as e:
                            await send_message(session, f"**```ansi\n\u001b[0;31m[Error in Sell Loop] {e}```**")                                              

                await asyncio.sleep(1)

                if t_exit < t_now:  # 프로그램 종료
                    first_bought_list.clear()
                    second_bought_list.clear()
                    await send_message(session, f"**```ansi\n\u001b[0;37m==  Trading Bot End ({file_name})  ==```**", include_time=True)
                    break

    except Exception as e:
        async with aiohttp.ClientSession() as session:
            await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Main loop exception: {e}```**")

if __name__ == "__main__":

    asyncio.run(main())
