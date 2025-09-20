import httpx
import asyncio
import time
from datetime import datetime, timedelta


async def fetch_mexc(symbol: str):
    """
    Вывод ставок финансирования MEXC
    """
    symbol_usdt = symbol.replace("USDT", "_USDT")
    url = f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol_usdt}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json().get("data")
        if not data:
            return None
        return {
            "exchange": "MEXC",
            "symbol": symbol_usdt,
            "funding_rate": float(data.get("fundingRate", 0)),
            "next_funding_time": int(data.get("nextSettleTime", 0))
        }


async def fetch_binance(symbol: str):
    """
    Вывод ставок финансирования Binance
    """
    url = f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json()
        return {
            "exchange": "BINANCE",
            "symbol": symbol,
            "funding_rate": float(data.get("lastFundingRate", 0)),
            "next_funding_time": int(data.get("nextFundingTime", 0))
        }


async def fetch_bingx(symbol: str):
    """
    Вывод ставок финансирования BINGX
    """
    symbol_bingx = symbol.replace("USDT", "-USDT")
    url = f"https://open-api.bingx.com/openApi/swap/v2/quote/premiumIndex?symbol={symbol_bingx}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json().get("data", {})
        return {
            "exchange": "BINGX",
            "symbol": symbol_bingx,
            "funding_rate": float(data.get("lastFundingRate", 0)),
            "next_funding_time": int(data.get("nextFundingTime", 0))
        }


async def fetch_bitget(symbol: str):
    """
    Вывод ставок финансирования BITGET
    """
    url = f"https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={symbol}&productType=usdt-futures"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data_list = r.json().get("data", [])
        if not data_list:
            return None
        data = data_list[0]
        return {
            "exchange": "BITGET",
            "symbol": symbol,
            "funding_rate": float(data.get("fundingRate", 0)),
            "next_funding_time": int(data.get("nextUpdate", 0))
        }


async def fetch_bitmart(symbol: str):
    """
    Вывод ставок финансирования BITMART
    """
    url = f"https://api-cloud-v2.bitmart.com/contract/public/details?symbol={symbol}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data_list = r.json().get("data", {}).get("symbols", [])
        if not data_list:
            return None
        data = data_list[0]
        return {
            "exchange": "BITMART",
            "symbol": symbol,
            "funding_rate": float(data.get("funding_rate", 0)),
            "next_funding_time": int(data.get("funding_time", 0))
        }


async def fetch_bybit(symbol: str):
    """
    Вывод ставок финансирования BYBIT
    """
    url = f"https://api.bybit.com/v5/market/tickers?category=inverse&symbol={symbol}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        lst = r.json().get("result", {}).get("list", [])
        if not lst:
            return None
        data = lst[0]
        return {
            "exchange": "BYBIT",
            "symbol": symbol,
            "funding_rate": float(data.get("fundingRate", 0)),
            "next_funding_time": int(data.get("nextFundingTime", 0))
        }


async def fetch_coinex(symbol: str):
    """
    Вывод ставок финансирования COINEX
    """
    url = f"https://api.coinex.com/v2/futures/funding-rate?market={symbol}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        lst = r.json().get("data", [])
        if not lst:
            return None
        data = lst[0]
        return {
            "exchange": "COINEX",
            "symbol": symbol,
            "funding_rate": float(data.get("latest_funding_rate", 0)),
            "next_funding_time": int(data.get("latest_funding_time", 0))
        }


async def fetch_gate(symbol: str):
    """
    Вывод ставок финансирования GATE
    """
    symbol_gate = symbol.replace("USDT", "_USDT")
    url = f"https://api.gateio.ws/api/v4/futures/usdt/contracts/{symbol_gate}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json()
        return {
            "exchange": "GATE",
            "symbol": symbol_gate,
            "funding_rate": float(data.get("funding_rate", 0)),
            "next_funding_time": int(data.get("funding_next_apply", 0))
        }


async def fetch_htx(symbol: str):
    """
    Вывод ставок финансирования HTX (Huobi)
    """
    symbol_htx = symbol.replace("USDT", "-USDT")
    url = f"https://api.hbdm.com/linear-swap-api/v1/swap_funding_rate?contract_code={symbol_htx}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json().get("data", {})
        if not data:
            return None
        return {
            "exchange": "HTX",
            "symbol": symbol_htx,
            "funding_rate": float(data.get("funding_rate", 0)),
            "next_funding_time": int(data.get("funding_time", 0))
        }


async def fetch_kucoin(symbol: str):
    """
    Вывод ставок финансирования KUCOIN
    """
    symbol_kc = f"{symbol}M"
    url = f"https://api-futures.kucoin.com/api/v1/contracts/{symbol_kc}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json().get("data", {})
        if not data:
            return None
        return {
            "exchange": "KUCOIN",
            "symbol": symbol_kc,
            "funding_rate": float(data.get("fundingFeeRate", 0)),
            "next_funding_time": int(data.get("nextFundingRateTime", 0))
        }


async def fetch_okx(symbol: str):
    """
    Вывод ставок финансирования OKX
    """
    symbol_okx = symbol.replace("USDT", "-USDT-SWAP")
    url = f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol_okx}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data_list = r.json().get("data", [])
        if not data_list:
            return None
        data = data_list[0]
        return {
            "exchange": "OKX",
            "symbol": symbol_okx,
            "funding_rate": float(data.get("fundingRate", 0)),
            "next_funding_time": int(data.get("fundingTime", 0))
        }


async def fetch_weex(symbol: str):
    """
    Вывод ставок финансирования WEEX (как у MEXC)
    """
    symbol_usdt = symbol.replace("USDT", "_USDT")
    url = f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol_usdt}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json().get("data")
        if not data:
            return None
        return {
            "exchange": "WEEX",
            "symbol": symbol_usdt,
            "funding_rate": float(data.get("fundingRate", 0)),
            "next_funding_time": int(data.get("nextSettleTime", 0))
        }


async def fetch_blofin(symbol: str):
    """
    Вывод ставок финансирования BLOFIN
    """
    symbol_blofin = symbol.replace("USDT", "-USDT")
    url = f"https://openapi.blofin.com/api/v1/market/funding-rate?instId={symbol_blofin}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        lst = r.json().get("data", [])
        if not lst:
            return None
        data = lst[0]
        return {
            "exchange": "BLOFIN",
            "symbol": symbol_blofin,
            "funding_rate": float(data.get("fundingRate", 0)),
            "next_funding_time": int(data.get("fundingTime", 0))
        }


async def fetch_ourbit(symbol: str):
    """
    Вывод ставок финансирования OURBIT
    """
    symbol_ourbit = symbol.replace("USDT", "_USDT")
    url = f"https://futures.ourbit.com/api/v1/contract/funding_rate/{symbol_ourbit}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json().get("data", {})
        if not data:
            return None
        return {
            "exchange": "OURBIT",
            "symbol": symbol_ourbit,
            "funding_rate": float(data.get("fundingRate", 0)),
            "next_funding_time": int(data.get("timestamp", 0))
        }


async def fetch_xt(symbol: str):
    """
    Вывод ставок финансирования XT
    """
    symbol_xt = symbol.lower() + "_usdt"
    url = f"https://fapi.xt.com/future/market/v1/public/q/funding-rate?symbol={symbol_xt}"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        if r.status_code != 200:
            return None
        data = r.json().get("result", {})
        if not data:
            return None
        return {
            "exchange": "XT",
            "symbol": symbol_xt,
            "funding_rate": float(data.get("fundingRate", 0)),
            "next_funding_time": int(data.get("nextCollectionTime", 0))
        }


async def get_all_funding(symbol: str):
    """
    Вывод ставок финансирования со всех бирж
    """
    exchanges_funcs = [
        fetch_mexc,
        fetch_binance,
        fetch_bingx,
        fetch_bitget,
        fetch_bitmart,
        fetch_bybit,
        fetch_coinex,
        fetch_gate,
        fetch_htx,
        fetch_kucoin,
        fetch_okx,
        fetch_weex,
        fetch_blofin,
        fetch_ourbit,
        fetch_xt,
    ]

    tasks = [func(symbol) for func in exchanges_funcs]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    funding_data = []
    for res in results:
        if isinstance(res, dict) and res:
            funding_data.append(res)

    return funding_data


async def fetch_binance_history(symbol: str, days: int = 7, limit: int = 500):
    """
    Получает историю ставок финансирования Binance за последние `days` дней.
    """
    end = int(datetime.utcnow().timestamp() * 1000)
    start = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    url = "https://fapi.binance.com/fapi/v1/fundingRate"
    params = {
        "symbol": symbol,
        "startTime": start,
        "endTime": end,
        "limit": limit
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()
    history = []
    for item in resp_json:
        history.append({
            "exchange": "BINANCE",
            "symbol": symbol,
            "funding_rate": float(item.get("fundingRate", 0)),
            "funding_time": int(item.get("fundingTime", 0))
        })
    return history


async def fetch_bybit_history(symbol: str, days: int = 100):
    end = int(time.time()) * 1000
    start = int((datetime.utcnow() - timedelta(days=days)).timestamp()) * 1000
    url = f"https://api.bybit.com/v5/market/funding/history?symbol={symbol}&startTime={start}&endTime={end}&category=linear"
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url)
        resp_json = r.json()
    
    history = []
    
    for item in resp_json.get("result", {}).get("list", []):
        history.append({
            "exchange": "BYBIT",
            "symbol": symbol,
            "funding_rate": float(item["fundingRate"]),
            "funding_time": int(item["fundingRateTimestamp"])
        })
    
    return history


async def fetch_kucoin_history(symbol: str, days: int):
    """
    История funding rate KuCoin (публичная история), за интервал времени [start, end] (unix seconds).
    """
    end = int(time.time()) * 1000
    start = int((datetime.utcnow() - timedelta(days=days)).timestamp()) * 1000
    url = "https://api-futures.kucoin.com/api/v1/contract/funding-rates"
    if symbol == "BTCUSDT":
        symbol = "XBTUSDTM"
    elif symbol:
        symbol = f"{symbol}M"

    params = {
        "symbol": f"{symbol}",
        "from": start,
        "to": end,
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()

    history = []
    data_list = resp_json.get("data", [])
    for item in data_list:
        history.append({
            "exchange": "KUCOIN",
            "symbol": f"{symbol}",
            "funding_rate": float(item.get("fundingRate", 0)),
            "funding_time": int(item.get("timepoint", 0))
        })
    return history


async def fetch_okx_history(instId: str, days: int = 7):
    """
    История funding rate OKX за последние `days` дней.
    instId формат: e.g. "BTC-USDT-SWAP"
    """
    end = int(datetime.utcnow().timestamp() * 1000)
    start = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    url = "https://www.okx.com/api/v5/public/funding-rate-history"
    if 'USDT' in instId:
        instId = instId.replace('USDT', '-USDT-SWAP')

    params = {
        "instId": instId,
        "limit": days * 4,
        "before": None,
        "after": None,
        "startTime": start,
        "endTime": end
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()

    history = []
    last_time = 0
    eight_hours = 8 * 60 * 60 * 1000

    for item in sorted(resp_json.get("data", []), key=lambda x: int(x["fundingTime"])):
        t = int(item.get("fundingTime", 0))
        if t - last_time >= eight_hours or not history:
            history.append({
                "exchange": "OKX",
                "symbol": instId,
                "funding_rate": float(item.get("fundingRate", 0)),
                "funding_time": t
            })
            last_time = t

    return history


async def fetch_mexc_history(symbol: str, days: int = 100):
    url = f"https://contract.mexc.com/api/v1/contract/funding_rate/history"
    cutoff = int((time.time() - days * 24 * 60 * 60) * 1000)

    if 'USDT' in symbol:
        symbol = symbol.replace('USDT', '_USDT')

    params = {
        "symbol": symbol,
        "page_num": 1,
        "page_size": 50,
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()

    all_data = []
    filtered = [item for item in resp_json['data']['resultList'] if int(item["settleTime"]) >= cutoff]
    all_data.extend(filtered)
    history = []
    
    for item in all_data:
        history.append({
            "exchange": "MEXC",
            "symbol": symbol,
            "funding_rate": float(item["fundingRate"]),
            "funding_time": int(item["settleTime"])
        })

    return history


async def fetch_bitget_history(symbol: str, days: int = 100):
    url = "https://api.bitget.com/api/v2/mix/market/history-fund-rate"
    cutoff = int((time.time() - days * 24 * 60 * 60) * 1000)

    if 'USDT' in symbol:
        product_type = 'USDT-FUTURES'
    elif 'COIN' in symbol:
        product_type = 'COIN-FUTURES'
    elif 'USDC' in symbol:
        product_type = 'USDC-FUTURES'

    params = {
        "symbol": symbol,
        "productType": product_type,
        "pageSize": 100,
        "pageNo": 1,
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()

    all_data = []
    filtered = [item for item in resp_json['data'] if int(item["fundingTime"]) >= cutoff]
    all_data.extend(filtered)
    history = []

    for item in all_data:
        history.append({
            "exchange": "BITGET",
            "symbol": symbol,
            "funding_rate": float(item["fundingRate"]),
            "funding_time": int(item["fundingTime"])
        })

    return history


async def fetch_bitmart_history(symbol: str, days: int = 100):
    url = f"https://api-cloud-v2.bitmart.com/contract/public/funding-rate-history"
    cutoff = int((time.time() - days * 24 * 60 * 60) * 1000)

    params = {
        "symbol": symbol,
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()

    all_data = []
    filtered = [item for item in resp_json['data']['list'] if int(item["funding_time"]) >= cutoff]
    all_data.extend(filtered)
    history = []
    
    for item in all_data:
        history.append({
            "exchange": "BITMART",
            "symbol": symbol,
            "funding_rate": float(item["funding_rate"]),
            "funding_time": int(item["funding_time"])
        })

    return history


async def fetch_weex_history(symbol: str, days: int = 100):
    url = f"https://contract.mexc.com/api/v1/contract/funding_rate/history"
    cutoff = int((time.time() - days * 24 * 60 * 60) * 1000)

    if 'USDT' in symbol:
        symbol = symbol.replace('USDT', '_USDT')

    params = {
        "symbol": symbol,
        "page_num": 1,
        "page_size": 50,
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()

    all_data = []
    filtered = [item for item in resp_json['data']['resultList'] if int(item["settleTime"]) >= cutoff]
    all_data.extend(filtered)
    history = []
    
    for item in all_data:
        history.append({
            "exchange": "WEEX",
            "symbol": symbol,
            "funding_rate": float(item["fundingRate"]),
            "funding_time": int(item["settleTime"])
        })

    return history


async def fetch_blofin_history(symbol: str, days: int = 7, limit: int = 100):
    """
    История funding rate Blofin за последние `days` дней.
    instId формат: e.g. "BTC-USDT-SWAP"
    """
    end = int(datetime.utcnow().timestamp() * 1000)
    start = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    url = "https://openapi.blofin.com/api/v1/market/funding-rate-history"

    if 'USDT' in symbol:
        symbol = symbol.replace('USDT', '-USDT')

    params = {
        "instId": symbol,
        "limit": limit,
        "before": start,
        "after": end,
        "startTime": None,
        "endTime": None
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()

    history = []
    for item in resp_json.get("data", []):
        history.append({
            "exchange": "BLOFIN",
            "symbol": symbol,
            "funding_rate": float(item.get("fundingRate", 0)),
            "funding_time": int(item.get("fundingTime", 0))
        })
    return history


async def fetch_coinex_history(symbol: str, days: int = 7):
    """
    История funding rate Coinex за последние `days` дней.
    """
    end = int(datetime.utcnow().timestamp() * 1000)
    start = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    url = "https://api.coinex.com/v2/futures/funding-rate-history"

    params = {
        "market": symbol,
        "limit": 30,
        "start_time": start,
        "end_time": end
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()
    
    history = []
    for item in resp_json.get("data", []):
        history.append({
            "exchange": "COINEX",
            "symbol": symbol,
            "funding_rate": float(item.get("actual_funding_rate", 0)),
            "funding_time": int(item.get("funding_time", 0))
        })

    return history


async def fetch_bingx_history(symbol: str, days: int = 7):
    """
    История funding rate Bingx за последние `days` дней.
    """
    end = int(datetime.utcnow().timestamp() * 1000)
    start = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/fundingRate"

    if 'USDT' in symbol:
        symbol = symbol.replace('USDT', '-USDT')

    params = {
        "symbol": symbol,
        "startTime": start,
        "endTime": end,
        'timestamp': int(time.time()) * 1000,
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()
    
    history = []
    for item in resp_json.get("data", []):
        history.append({
            "exchange": "BINGX",
            "symbol": symbol,
            "funding_rate": float(item.get("fundingRate", 0)),
            "funding_time": int(item.get("fundingTime", 0))
        })

    return history


async def fetch_htx_history(symbol: str, days: int = 100):
    url = f"https://api.hbdm.com/linear-swap-api/v1/swap_historical_funding_rate"
    cutoff = int((time.time() - days * 24 * 60 * 60) * 1000)

    if 'USDT' in symbol:
        symbol = symbol.replace('USDT', '-USDT')

    params = {
        "contract_code": symbol,
        "page_index": 1,
        "page_size": 50,
    }
    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, params=params)
        resp_json = r.json()

    all_data = []
    filtered = [item for item in resp_json['data']['data'] if int(item["funding_time"]) >= cutoff]
    all_data.extend(filtered)

    history = []

    for item in all_data:
        history.append({
            "exchange": "HTX",
            "symbol": symbol,
            "funding_rate": float(item["funding_rate"]),
            "funding_time": int(item["funding_time"])
        })

    return history


async def fetch_Gate_history(symbol: str, days: int = 7, limit: int = 100):
    """
    История funding rate Gate за последние `days` дней.
    instId формат: e.g. ""
    """
    end = int(datetime.utcnow().timestamp() * 1000)
    start = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    if 'USDT' in symbol:
        symbol = symbol.replace('USDT', '_USDT')

    url = f"https://api.gateio.ws/api/v4/futures/{symbol}/funding_rate?contract={symbol}&from={start}&to={end}"

    headers = {
        "Timestamp": str(int(time.time())),
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    async with httpx.AsyncClient(verify=False) as client:
        r = await client.get(url, headers=headers)
        resp_json = r.json()

    history = []
    for item in resp_json.get("data", []):
        history.append({
            "exchange": "GATE",
            "symbol": symbol,
            "funding_rate": float(item.get("r", 0)),
            "funding_time": int(item.get("t", 0))
        })
    return history


async def get_history_all(symbol: str, days: int = 7):

    tasks = [
        # fetch_Gate_history(symbol, days), {'label': 'MISSING_REQUIRED_HEADER', 'message': 'Missing required header: KEY'}
        fetch_binance_history(symbol, days),
        fetch_bybit_history(symbol, days),
        fetch_okx_history(symbol, days),
        fetch_kucoin_history(symbol, days),
        fetch_htx_history(symbol, days),
        fetch_blofin_history(symbol, days),
        fetch_mexc_history(symbol, days),
        fetch_weex_history(symbol, days),
        fetch_coinex_history(symbol, days),
        fetch_bingx_history(symbol, days),
        fetch_bitmart_history(symbol, days),
        fetch_bitget_history(symbol, days)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    history = []
    for res in results:
        if isinstance(res, list):
            history.extend(res)
    return history
