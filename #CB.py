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
file_name = os.path.splitext(os.path.basename(__file__))[0]

# 매매 종목 리스트
def load_symbol_list():
    symbol_list = []
    with open('symbol_list_cb.txt', 'r', encoding='UTF-8') as f:
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
    with open('symbol_list_cb.txt', 'r', encoding='UTF-8') as f:
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

# 종목명 조회
async def get_name(session, code=""):
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

# Fetch Data
async def fetch_data(session, path, headers, params, response_key, sub_key=None):
    try:
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

# 주가 조회
async def get_price_info(session, code, price_type):
    try:
        paths = {
            'ec': ("/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn", "FHKST01010200", 'output2', 'antc_cnpr'),
            'o': ("/uapi/domestic-stock/v1/quotations/inquire-daily-price", "FHKST01010400", 'output', 'stck_oprc'),
            'c': ("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100", 'output', 'stck_prpr'),
            'm': ("/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100", 'output', 'stck_mxpr'),
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
        current_price_task = get_price_info(session, sym, 'ec')
        open_price_task = get_price_info(session, sym, 'o')
        max_price_task = get_price_info(session, sym, 'm')
        sd_price_task = get_price_info(session, sym, 'sd')
        d120_price_task = get_d120_price(session, sym)
        d2_price_task = get_d2_price(session, sym)
        symbol_data_task = get_symbol_datas(sym)
        current_price, open_price, max_price, sd_price, d120_price, d2_price, (low_price, high_price, trade_type) = await asyncio.gather(
            current_price_task,
            open_price_task,
            max_price_task,
            sd_price_task, 
            d120_price_task,
            d2_price_task,
            symbol_data_task,
            return_exceptions=True
        )
        return current_price, open_price, max_price, sd_price, d120_price, d2_price, low_price, high_price, trade_type
    except Exception as e:
        await send_message(session, f"**```ansi\n\u001b[0;31m[Error in fetch_price_info for {sym}: {str(e)}```**")
        return None, None, None, None, None, None, None, None, None

# 120일 최고가 조회
async def get_d120_price(session, code):
    try:
        combined_df = pd.DataFrame()
        today = datetime.date.today()
        for i in range(1, 3):
            PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
            URL = f"{URL_BASE}{PATH}"
            if i > 1:
                today = today_prev1
                today_prev1 = today_prev1 - datetime.timedelta(days=100)
            else:
                today_prev1 = today - datetime.timedelta(days=100)
            headers = get_headers("FHKST03010100", "P")
            params = {
                "fid_cond_mrkt_div_code": "J",
                "fid_input_iscd": code,
                "fid_input_date_1": today_prev1.strftime("%Y%m%d"),
                "fid_input_date_2": today.strftime("%Y%m%d"),
                "fid_period_div_code": "D",
                "fid_org_adj_prc": 0
            }
            async with session.get(URL, headers=headers, params=params) as res:
                evaluation = (await res.json()).get('output2')
                if evaluation:
                    df = pd.DataFrame(evaluation)
                    df['stck_bsop_date'] = pd.to_datetime(df['stck_bsop_date'], format='%Y%m%d')
                    combined_df = pd.concat([combined_df, df])
        combined_df = combined_df.sort_values(by='stck_bsop_date', ascending=False)
        if len(combined_df) > 0:
            most_recent_date = combined_df['stck_bsop_date'].iloc[0]
            filtered_df = combined_df[combined_df['stck_bsop_date'] < most_recent_date]
            filtered_df.loc[:, 'stck_hgpr'] = pd.to_numeric(filtered_df['stck_hgpr'], errors='coerce')
            max_hgpr = filtered_df['stck_hgpr'].head(120).max()
            return int(max_hgpr) if not pd.isna(max_hgpr) else None
        else:
            return None
    except Exception as e:
        await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception in get_d120_price: {e}```**")
        return None

# 직전 19일간 종가 합계
async def get_d19_price(session, code):
    try:
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=50)
        PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        URL = f"{URL_BASE}{PATH}"
        headers = get_headers("FHKST03010100", "P")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
            "fid_input_date_1": start_date.strftime("%Y%m%d"),
            "fid_input_date_2": today.strftime("%Y%m%d"),
            "fid_period_div_code": "D",
            "fid_org_adj_prc": 0
        }
        async with session.get(URL, headers=headers, params=params) as res:
            res.raise_for_status()
            evaluation = (await res.json()).get('output2')
            if evaluation:
                df = pd.DataFrame(evaluation)
                df['stck_bsop_date'] = pd.to_datetime(df['stck_bsop_date'], format='%Y%m%d')
                df = df.sort_values(by='stck_bsop_date', ascending=False)  # 최근 날짜가 가장 위로 오도록 정렬
                if len(df) > 0:
                    most_recent_date = df['stck_bsop_date'].iloc[0]
                    filtered_df = df[df['stck_bsop_date'] < most_recent_date]  # 가장 최근 날짜 데이터 제외
                    filtered_df.loc[:, 'stck_clpr'] = pd.to_numeric(filtered_df['stck_clpr'], errors='coerce')
                    return filtered_df['stck_clpr'].head(19).sum()
                else:
                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error] No data returned for {code}```**")
                    return None
    except Exception as e:
        await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception in get_d19_price: {e}```**")
        return None

# 직전 2일간 종가 합계
async def get_d2_price(session, code):
    try:
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=20)
        PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        URL = f"{URL_BASE}{PATH}"
        headers = get_headers("FHKST03010100", "P")
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
            "fid_input_date_1": start_date.strftime("%Y%m%d"),
            "fid_input_date_2": today.strftime("%Y%m%d"),
            "fid_period_div_code": "D",
            "fid_org_adj_prc": 0
        }
        async with session.get(URL, headers=headers, params=params) as res:
            res.raise_for_status()
            evaluation = (await res.json()).get('output2')
            if evaluation:
                df = pd.DataFrame(evaluation)
                df['stck_bsop_date'] = pd.to_datetime(df['stck_bsop_date'], format='%Y%m%d')
                df = df.sort_values(by='stck_bsop_date', ascending=False)  # 최근 날짜가 가장 위로 오도록 정렬
                if len(df) > 0:
                    most_recent_date = df['stck_bsop_date'].iloc[0]
                    filtered_df = df[df['stck_bsop_date'] < most_recent_date]  # 가장 최근 날짜 데이터 제외
                    filtered_df.loc[:, 'stck_clpr'] = pd.to_numeric(filtered_df['stck_clpr'], errors='coerce')
                    return filtered_df['stck_clpr'].head(2).sum()
                else:
                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error] No data returned for {code}```**")
                    return None
    except Exception as e:
        await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception in get_d2_price: {e}```**")
        return None

# 주식 잔고 조회
async def get_stock_balance(session):
    timeout = aiohttp.ClientTimeout(total=10)
    retry_count = 10
    async with semaphore:
        global loan_dt_qty_dict  # loan_dt_qty_dict 업데이트

        loan_dt_qty_dict = {}  # 초기화 후 갱신
        stock_dict = {}

        for inqr_dvsn in ["01", "02"]:  # "01": 신용, "02": 현금
            for attempt in range(retry_count):
                try:
                    await asyncio.sleep(0.05)
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
                                    if pdno not in loan_dt_qty_dict:
                                        loan_dt_qty_dict[pdno] = {}
                                    if loan_dt in loan_dt_qty_dict[pdno]:
                                        loan_dt_qty_dict[pdno][loan_dt] += ord_psbl_qty
                                    else:
                                        loan_dt_qty_dict[pdno][loan_dt] = ord_psbl_qty

                                await asyncio.sleep(0.05)
                    break
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    await send_message(session, f"**```ansi\n\u001b[0;31m[Attempt {attempt + 1}] Error in get_stock_balance ({inqr_dvsn}): {e}```**")
                    await asyncio.sleep(1)
                    continue
            else:
                await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Failed to get stock balance ({inqr_dvsn}) after retries```**")
        return stock_dict

# 신용매수 가능 여부 조회
async def psbl_credit(session, code):
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
    except Exception as e:
        return False


# 주문 (매수)
async def buy(session, code, qty, use_credit=False):
    if use_credit:
        PATH = "/uapi/domestic-stock/v1/trading/order-credit"
        tr_id = "TTTC0852U"
        data = {
            "CANO": CANO,
            "ACNT_PRDT_CD": ACNT_PRDT_CD,
            "PDNO": code,
            "ORD_DVSN": "01", 
            "ORD_QTY": str(int(qty)),
            "ORD_UNPR": "0",
            "CRDT_TYPE": "21",
            "RVSN_ORD_YN": "N",
            "LOAN_DT": datetime.datetime.now(KST).strftime('%Y%m%d')
        }
    else:
        PATH = "/uapi/domestic-stock/v1/trading/order-cash"
        tr_id = "TTTC0802U"
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
            await send_message(session, f"**```ansi\n\u001b[0;36m[매수 성공] {str(res_json)}```**")
            return True
        else:
            await send_message(session, f"**```ansi\n\u001b[0;35m[매수 실패] {str(res_json)}```**")
            return False

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

# 메인 함수
async def main():
    try:
        global ACCESS_TOKEN
        async with aiohttp.ClientSession() as session:
            ACCESS_TOKEN, token_data = await get_access_token(session)
            if not ACCESS_TOKEN:
                await send_message(session, "**```ansi\n\u001b[0;31m[Error] Failed to get ACCESS_TOKEN. Exiting...```**")
                return
            BUY_AMOUNT = 15000000
            bought_list = []
            soldout_dict = {}
            name_dict = {}
            stock_dict = await get_stock_balance(session)

            #for sym in stock_dict.keys():
                #bought_list.append(sym)

            await send_message(session, f"**```ansi\n\u001b[0;37m==  Trading Bot Start ({file_name})  ==```**", include_time=True)

            while True:
                t_now = datetime.datetime.now(KST)
                if time.time() > (token_data.get("expiry_timestamp") - 300):
                    try:
                        ACCESS_TOKEN, token_data = await get_access_token(session)
                        if not ACCESS_TOKEN:
                            await send_message(session, "**```ansi\n\u001b[0;31m[Error] Failed to refresh ACCESS_TOKEN. Exiting...```**")
                            return
                    except Exception as e:
                        await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Exception while refreshing ACCESS_TOKEN: {e}```**")
                        return

                t_buy = t_now.replace(hour=15, minute=29, second=50, microsecond=0)
                t_sell = t_now.replace(hour=15, minute=29, second=45, microsecond=0)
                t_exit = t_now.replace(hour=15, minute=32, second=0, microsecond=0)
                symbol_list = load_symbol_list()

                if t_buy < t_now < t_exit:  # 매수
                    for sym in symbol_list:
                        if sym in bought_list:
                            continue
                        if not sym or not isinstance(sym, str):
                            continue
                        try:
                            result = await fetch_price_info(session, sym)
                            if None in result:
                                await send_message(session, f"**```ansi\n\u001b[0;31m[Error in getting fetch price info for ({sym})] {str(e)}```**")
                                continue
                            current_price, open_price, max_price, sd_price, d120_price, d2_price, _, _, trade_type = result
                            d3_price = (d2_price + current_price) / 3
                            name = name_dict.get(sym)
                            if not name:
                                name = await get_name(session, sym)
                                if name:
                                    name_dict[sym] = name
                                else:
                                    await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Failed to fetch name for symbol {sym}```**")
                                    continue
                        except Exception as e:
                            await send_message(session, f"**```ansi\n\u001b[0;31m[Error in getting price info for {name}({sym})] {e}```**")
                            continue

                        if trade_type == 'HB' and (sd_price * 1.05 <= current_price) and (d120_price < current_price) and (open_price < current_price) and (current_price < max_price):
                            try:
                                buy_qty = int(BUY_AMOUNT // (current_price * 1.000036396))
                                result = False  # result 변수를 초기화
                                if buy_qty > 0:
                                    credit_available = await psbl_credit(session, sym)
                                    if credit_available:  # 신용 매수 가능 시
                                        await send_message(session, f"**```cs\n목표가 달성({format(d120_price, ',')} < {format(current_price, ',')}) ▶ {name}({sym}) {format(buy_qty, ',')}주 신용매수 주문```**")
                                        result = await buy(session, sym, buy_qty, use_credit=True)
                                    else:  # 현금으로 매수 시도
                                        await send_message(session, f"**```cs\n목표가 달성({format(d120_price, ',')} < {format(current_price, ',')}) ▶ {name}({sym}) {format(buy_qty, ',')}주 매수 주문```**")
                                        result = await buy(session, sym, buy_qty)
                                    if result:
                                        bought_list.append(sym)
                                        stock_dict = await get_stock_balance(session)
                            except Exception as e:
                                await send_message(session, f"**```ansi\n\u001b[0;31m[Error in Buy UT {name}({sym})] {e}```**")

                        elif trade_type == 'LB' and (current_price < d3_price):
                            try:
                                buy_qty = int(5000000 // (current_price * 1.000036396))
                                result = False  # result 변수를 초기화
                                if buy_qty > 0:
                                    credit_available = await psbl_credit(session, sym)
                                    if credit_available:  # 신용 매수 가능 시
                                        await send_message(session, f"**```cs\n목표가 달성({format(current_price, ',')} < {format(d3_price, ',')}) ▶ {name}({sym}) {format(buy_qty, ',')}주 신용매수 주문```**")
                                        result = await buy(session, sym, buy_qty, use_credit=True)
                                    else:  # 현금으로 매수 시도
                                        await send_message(session, f"**```cs\n목표가 달성({format(current_price, ',')} < {format(d3_price, ',')}) ▶ {name}({sym}) {format(buy_qty, ',')}주 매수 주문```**")
                                        result = await buy(session, sym, buy_qty)
                                    if result:
                                        bought_list.append(sym)
                                        stock_dict = await get_stock_balance(session)
                            except Exception as e:
                                await send_message(session, f"**```ansi\n\u001b[0;31m[Error in Buy LB {name}({sym})] {e}```**")

                        elif trade_type == 'LB2' and (current_price < d3_price):
                            try:
                                buy_qty = int(10000000 // (current_price * 1.000036396))
                                result = False  # result 변수를 초기화
                                if buy_qty > 0:
                                    credit_available = await psbl_credit(session, sym)
                                    if credit_available:  # 신용 매수 가능 시
                                        await send_message(session, f"**```cs\n목표가 달성({format(current_price, ',')} < {format(d3_price, ',')}) ▶ {name}({sym}) {format(buy_qty, ',')}주 신용매수 주문```**")
                                        result = await buy(session, sym, buy_qty, use_credit=True)
                                    else:  # 현금으로 매수 시도
                                        await send_message(session, f"**```cs\n목표가 달성({format(current_price, ',')} < {format(d3_price, ',')}) ▶ {name}({sym}) {format(buy_qty, ',')}주 매수 주문```**")
                                        result = await buy(session, sym, buy_qty)
                                    if result:
                                        bought_list.append(sym)
                                        stock_dict = await get_stock_balance(session)
                            except Exception as e:
                                await send_message(session, f"**```ansi\n\u001b[0;31m[Error in Buy LT2 {name}({sym})] {e}```**")

                if t_sell < t_now < t_exit:  # 매도
                    stock_dict = await get_stock_balance(session)
                    for sym, data in stock_dict.items():         
                        try:
                            if sym in soldout_dict and soldout_dict[sym]:
                                continue
                            avg_price = data['pchs_avg_pric']
                            qty = data['ord_psbl_qty']
                            prdt_name = data['prdt_name']
                            current_price = await get_price_info(session, sym, 'ec')
                            d19_price = await get_d19_price(session, sym)
                            d20_price = (d19_price + current_price) / 20
                            _, _, trade_type = await get_symbol_datas(sym)
                            if avg_price is None or qty is None or qty <=0 or prdt_name is None or current_price is None or d19_price is None or d20_price is None:
                                continue
                            loss = (avg_price - current_price) / avg_price * -100
                            sell_qty = 0

                            if trade_type == 'UT' and ((avg_price * 0.92 >= current_price) or (d20_price >= current_price)):
                                sell_qty = qty
                                sell_result = await advanced_sell(session, sym, sell_qty)
                                if sell_result:
                                    await send_message(session, f"**```cs\n손절가 도래({loss:.2f}%) ▶ {prdt_name}({sym}) 매도 주문```**")
                                    bought_list.remove(sym)
                                    soldout_dict[sym] = True
                                    stock_dict = await get_stock_balance(session)
                                    qty = stock_dict[sym]['ord_psbl_qty'] if sym in stock_dict else 0
                                else:
                                    await send_message(session, f"**```ansi\n\u001b[0;31m[Warning] Failed to Sell for {prdt_name}({sym})```**")
                        except Exception as e:
                            await send_message(session, f"**```ansi\n\u001b[0;31m[Error in Sell] {e}```**")
                
                await asyncio.sleep(0.05)

                if t_exit < t_now:  # 프로그램 종료
                    await send_message(session, f"**```ansi\n\u001b[0;37m==  Trading Bot End ({file_name})  ==```**", include_time=True)
                    break

    except Exception as e:
        async with aiohttp.ClientSession() as session:
            await send_message(session, f"**```ansi\n\u001b[0;31m[Error] Main loop exception: {e}```**")

if __name__ == "__main__":
    asyncio.run(main())
