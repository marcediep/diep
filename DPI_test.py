import requests
import json
import datetime
import time
import yaml
import pandas as pd

with open('config.yaml', encoding='UTF-8') as f:
    _cfg = yaml.load(f, Loader=yaml.FullLoader)
APP_KEY = _cfg['APP_KEY']
APP_SECRET = _cfg['APP_SECRET']
ACCESS_TOKEN = ""
CANO = _cfg['CANO']
ACNT_PRDT_CD = _cfg['ACNT_PRDT_CD']
DISCORD_WEBHOOK_URL = _cfg['DISCORD_WEBHOOK_URL']
URL_BASE = _cfg['URL_BASE']

def send_message(msg):
    """디스코드 메세지 전송"""
    now = datetime.datetime.now()
    message = {"content": f"[{now.strftime('%Y-%m-%d %H:%M:%S')}] {str(msg)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message)
    print(message)

def send_message2(msg2):
    """디스코드 메세지 전송2"""
    message2 = {"content": f"{str(msg2)}"}
    requests.post(DISCORD_WEBHOOK_URL, data=message2)
    print(message2)

def get_access_token():
    """토큰 발급"""
    headers = {"content-type":"application/json"}
    body = {"grant_type":"client_credentials",
    "appkey":APP_KEY, 
    "appsecret":APP_SECRET}
    PATH = "oauth2/tokenP"
    URL = f"{URL_BASE}/{PATH}"
    res = requests.post(URL, headers=headers, data=json.dumps(body))
    ACCESS_TOKEN = res.json()["access_token"]
    return ACCESS_TOKEN
    
def hashkey(datas):
    """암호화"""
    PATH = "uapi/hashkey"
    URL = f"{URL_BASE}/{PATH}"
    headers = {
    'content-Type' : 'application/json',
    'appKey' : APP_KEY,
    'appSecret' : APP_SECRET,
    }
    res = requests.post(URL, headers=headers, data=json.dumps(datas))
    hashkey = res.json()["HASH"]
    return hashkey

def get_current_price(code="005930"):
    """현재가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output']['stck_prpr'])

def get_max_price(code="005930"):
    """상한가 조회"""
    PATH = "uapi/domestic-stock/v1/quotations/inquire-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"FHKST01010100"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output']['stck_mxpr'])

def get_expclosing_price(code="005930"):
    """예상체결가 조회"""
    PATH = "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
            "authorization": f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
            "appKey":APP_KEY,
            "appSecret":APP_SECRET,
            "tr_id":"FHKST01010200"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    }
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output2']['antc_cnpr'])

def get_start_price(code="005930"):
    """시가 조회"""
    PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization": f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"FHKST01010400"}
    params = {
    "fid_cond_mrkt_div_code":"J",
    "fid_input_iscd":code,
    "fid_org_adj_prc":"0",
    "fid_period_div_code":"D"
    }
    res = requests.get(URL, headers=headers, params=params)
    return int(res.json()['output'][0]['stck_oprc'])

def daily_price(code="005930"):
    """120일 종가 최고값"""
    max_clpr_list=[]
    for i in range (1,3):
        today2 = datetime.date.today()
        today1 = today2 - datetime.timedelta(days=1)
        PATH = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        URL = f"{URL_BASE}/{PATH}"
        if i > 1:
            today1 = today_prev1 -  datetime.timedelta(days=1)
        today_prev1 = datetime.date.today() - datetime.timedelta(days=89 * i)
        headers = {"Content-Type":"application/json", 
                "authorization": f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
                "appKey":APP_KEY,
                "appSecret":APP_SECRET,
                "tr_id":"FHKST03010100",
                "custtype":"P"}
        params = {
        "fid_cond_mrkt_div_code":"J",
        "fid_input_iscd":code,
        "fid_input_date_1":today_prev1.strftime("%Y%m%d"),
        "fid_input_date_2":today1.strftime("%Y%m%d"),
        "fid_period_div_code":"D",
        "fid_org_adj_prc":0}
        res = requests.get(URL, headers=headers, params=params)
        time.sleep(0.5)
        evaluation = res.json().get('output2')
        df = pd.DataFrame(evaluation)
        max_clpr = df['stck_clpr'].max()
        max_clpr_list.append(max_clpr)
    return int(max(max_clpr_list))

def get_stock_balance():
    """주식 잔고조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8434R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    res = requests.get(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    evaluation = res.json()['output2']
    stock_dict = {}
    #send_message(f"**```ansi\n\u001b[0;37m==  주식 보유잔고  ==```**")
    for stock in stock_list:
        if int(stock['hldg_qty']) > 0:
            stock_dict[stock['pdno']] = stock['hldg_qty']
            if float(stock['evlu_pfls_rt']) >= 0:
                send_message2(f"**```ansi\n\u001b[0;37m{stock['prdt_name']}({stock['pdno']}):  {stock['hldg_qty']}주 (수익률: \u001b[0;31m{stock['evlu_pfls_rt']}%\u001b[0;37m)```**")
            if float(stock['evlu_pfls_rt']) < 0:
                send_message2(f"**```ansi\n\u001b[0;37m{stock['prdt_name']}({stock['pdno']}):  {stock['hldg_qty']}주 (수익률: \u001b[0;34m{stock['evlu_pfls_rt']}%\u001b[0;37m)```**")
            time.sleep(0.1)
    #send_message2(f"**```cs\n주식 평가 금액: {format(int(evaluation[0]['scts_evlu_amt']),",")}원```**")
    #time.sleep(0.1)
    #send_message2(f"**```cs\n평가 손익 합계: {format(int(evaluation[0]['evlu_pfls_smtl_amt']),",")}원```**")
    #time.sleep(0.1)
    #send_message2(f"**```cs\n총 평가 금액: {format(int(evaluation[0]['tot_evlu_amt']),",")}원```**")
    #time.sleep(0.1)
    #send_message2(f"**```ansi\n\u001b[0;37m=================```**")
    return stock_dict

def get_avg_price():
    """매입 평균가 조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-balance"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8434R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    res = requests.get(URL, headers=headers, params=params)
    stock_list = res.json()['output1']
    evaluation = res.json()['output2']
    stock_dict = {}
    for stock in stock_list:
        if int(stock['hldg_qty']) > 0:
            stock_dict[stock['pdno']] = stock['hldg_qty']
            stock_dict[stock['pdno']] = stock['pchs_avg_pric']
            print(stock['pdno'], stock['pchs_avg_pric'])

def get_balance():
    """현금 잔고조회"""
    PATH = "uapi/domestic-stock/v1/trading/inquire-psbl-order"
    URL = f"{URL_BASE}/{PATH}"
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC8908R",
        "custtype":"P",
    }
    params = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": "005930",
        "ORD_UNPR": "65500",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "Y",
        "OVRS_ICLD_YN": "Y"
    }
    res = requests.get(URL, headers=headers, params=params)
    cash = res.json()['output']['ord_psbl_cash']
    #send_message(f"**```cs\n주문 가능 현금 잔고: {format(int(cash),",")}원```**")
    return int(cash)

def buy(code="005930", qty="1"):
    """주식 시장가 매수"""  
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    data = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": str(int(qty)),
        "ORD_UNPR": "0",
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC0802U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json()['rt_cd'] == '0':
        send_message(f"**```ansi\n\u001b[0;35m[매수 성공] {str(res.json())}```**")
        return True
    else:
        send_message(f"**```ansi\n\u001b[0;36m[매수 실패] {str(res.json())}```**")
        return False

def sell(code="005930", qty="1"):
    """주식 시장가 매도"""
    PATH = "uapi/domestic-stock/v1/trading/order-cash"
    URL = f"{URL_BASE}/{PATH}"
    data = {
        "CANO": CANO,
        "ACNT_PRDT_CD": ACNT_PRDT_CD,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": qty,
        "ORD_UNPR": "0",
    }
    headers = {"Content-Type":"application/json", 
        "authorization":f"Bearer {"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0b2tlbiIsImF1ZCI6IjZkYzRmOTI5LTBkMGYtNDdkOS05YjljLWZjNDhmNTgyMmI3ZiIsInByZHRfY2QiOiIiLCJpc3MiOiJ1bm9ndyIsImV4cCI6MTcxMzY3NDI3NiwiaWF0IjoxNzEzNTg3ODc2LCJqdGkiOiJQU203M1JYdTk5R2dwdExuN2RPQk5WS1RCRExBRUx6THZYMEQifQ.06KURukHU6DTbaYtNxM6lvzSi0Q6gfIU7fQ4Imw-ZVts0IsU9qUWq6q0LaU-q0N19CvXGpC-_0JBxZ4MA1CU_Q"}",
        "appKey":APP_KEY,
        "appSecret":APP_SECRET,
        "tr_id":"TTTC0801U",
        "custtype":"P",
        "hashkey" : hashkey(data)
    }
    res = requests.post(URL, headers=headers, data=json.dumps(data))
    if res.json()['rt_cd'] == '0':
        send_message(f"**```ansi\n\u001b[0;35m[매도 성공]{str(res.json())}```**")
        return True
    else:
        send_message(f"**```ansi\n\u001b[0;36m[매도 실패]{str(res.json())}```**")
        return False

# 자동매매 시작
try:
    #ACCESS_TOKEN = get_access_token()

    symbol_list = ["359090"] # 매수 희망 종목 리스트
    bought_list = [] # 매수 완료된 종목 리스트
    total_cash = get_balance() # 보유 현금 조회
    stock_dict = get_stock_balance() # 보유 주식 조회
    for sym in stock_dict.keys():
        bought_list.append(sym)
    target_buy_count = 3 # 매수할 종목 수
    buy_amount = 30000000  # 종목별 주문 금액 계산
    soldout = False

    send_message("**```ansi\n\u001b[0;37m==  Trading Bot Start  ==```**")
    while True :       
        t_now = datetime.datetime.now()
        t_start = t_now.replace(hour=9, minute=29, second=50, microsecond=0)
        t_exit = t_now.replace(hour=23, minute=38, second=0,microsecond=0)
        today = datetime.datetime.today().weekday()
        if t_start < t_now < t_exit :  # 매수
            for sym in symbol_list:
                if len(bought_list) < target_buy_count:
                    if sym in bought_list:
                        continue
                    target_price = daily_price(sym)
                    current_price = get_expclosing_price(sym)                  
                    start_price = get_start_price(sym)
                    max_price = get_max_price(sym)
                    if (target_price <= current_price) and (start_price < current_price) and (current_price < max_price):
                        buy_qty = 0  # 매수할 수량 초기화
                        buy_qty = int(buy_amount // (current_price * 1.000036396))
                        if buy_qty > 0:
                            send_message(f"**```cs\n목표가 달성({format(target_price,",")} < {format(current_price,",")}) ▶ ({sym}) {format(buy_qty,",")}개 매수 주문```**")
                            result = buy(sym, buy_qty)
                            if result:
                                soldout = False
                                bought_list.append(sym)
                                get_stock_balance()
                    time.sleep(0.5)
            time.sleep(1200)
        if t_start < t_now < t_exit :  # 매도
            if soldout == False:
                stock_dict = get_stock_balance()            
                for sym, qty in stock_dict.items():
                    current_price = get_expclosing_price(sym)                  
                    avg_price = get_avg_price(sym)
                    if avg_price * 0.91 >= current_price:
                        sell(sym, qty)
                    soldout = True
                    bought_list = []
                    time.sleep(1)
        if t_exit < t_now:  # 15:38 ~ :프로그램 종료
            send_message("**```ansi\n\u001b[0;37m==  Trading Bot End  ==```**")
            break
except Exception as e:
    send_message(f"**```ansi\n\u001b[0;37m[Error] {e}```**")
    time.sleep(1)