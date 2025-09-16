import itertools
from datetime import datetime


def calc_spread(rate1, rate2):
    """
    Подсчет спреда
    """
    if (rate1 >= 0 and rate2 < 0) or (rate1 < 0 and rate2 >= 0):
        return abs(rate1) + abs(rate2)
    return abs(rate1 - rate2)


def calc_max_spread(data):
    """
    Подсчет максимального спреда
    """
    max_spread = 0
    max_pair = ("", "")
    for a, b in itertools.combinations(data, 2):
        spread = calc_spread(a["funding_rate"], b["funding_rate"])
        if spread > max_spread:
            max_spread = spread
            max_pair = (a["exchange"], b["exchange"])
    return max_spread, max_pair


def normalize_funding_data(data):
    """
    Приводит raw данные из разных бирж к единому виду:
    - funding_rate в процентах, 6 знаков после запятой
    - next_funding_time в формате HH:MM:SS
    """
    normalized = []
    for item in data:
        rate = float(item.get("funding_rate", 0)) * 100
        rate = round(rate, 6)

        ts = item.get("next_funding_time", 0)
        if ts > 1e12:

            ts /= 1000

        dt = datetime.utcfromtimestamp(ts).strftime("%H:%M:%S")

        normalized.append({
            "exchange": item.get("exchange"),
            "symbol": item.get("symbol"),
            "funding_rate": rate,
            "next_funding_time": dt
        })
    return normalized
