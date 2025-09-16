import httpx
import asyncio


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
