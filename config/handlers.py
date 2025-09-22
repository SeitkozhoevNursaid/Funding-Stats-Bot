import io
import tempfile
from datetime import datetime

import numpy as np
import matplotlib.pyplot as plt
from aiogram.types import FSInputFile
from aiogram import types
from aiogram import Dispatcher, types
from aiogram.filters import Command

from funding_fetcher import get_all_funding, get_history_all
from utils import calc_max_spread, normalize_funding_data


async def start_cmd(message: types.Message):
    """
    Хэндлер команды /start
    """
    await message.answer(
        "👋 Привет! Я — Funding-Stats-Bot.\n\n"
        "Я помогаю получать ставки финансирования, историю и строить графики.\n\n"
        "Доступные команды (подробно):\n\n"
        "1) /funding <symbol>\n"
        "   — Текущие ставки финансирования для символа по всем поддерживаемым биржам\n"
        "   — Выводит: биржа, funding_rate (в % с 6 знаками), время исполнения в формате HH:MM:SS и максимальный спред между биржами.\n"
        "   Пример: /funding BTCUSDT\n\n"
        "2) /funding_spread_chart <exchange> <days> <symbol>\n"
        "   — Построить график кумулятивной суммы (cumulative) funding rate по одному символу на выбранной бирже за последние <days> дней.\n"
        "   — Также таблица/список с исходными точками времени и соответствующими funding_rate (без суммирования).\n"
        "   Формат: /funding_spread_chart BYBIT 7 BTCUSDT\n"
        "   Примечание: <exchange> — код биржи (например, BINANCE, BYBIT).\n\n"
        "3) /top_tokens_chart <exchange> <days> <symbol1,symbol2,...>\n"
        "   — Для заданного списка токенов строит кумулятивные графики funding rate (каждый токен) и сортирует их по итоговой кумулятивной разнице.\n"
        "   — Возвращает PNG с графиком и текстовый ТОП (символы + итоговые значения).\n"
        "   Пример: /top_tokens_chart BYBIT 3 BTCUSDT,ETHUSDT,SOLUSDT\n\n"
        "ВАЖНЫЕ МЕЛОЧИ / подсказки:\n"
        "- Указывай символы в привычном виде, например: BTCUSDT, ETHUSDT, SOLUSDT.\n"
        "- Для некоторых бирж (BYBIT, BINANCE и др.) бот автоматически преобразует формат символа (например, в BTC-USDT или BTC-USDT-SWAP) — обычно достаточно передать 'BTCUSDT'.\n"
        "- Если данных по символу нет — бот вернёт сообщение \"Не найдено\" или пустой список (возможно, символ не торгуется на выбранной бирже или формат неверный).\n"
        "- Пользователь передаёт список токенов через запятую без пробелов после запятой: BTCUSDT,ETHUSDT,SOLUSDT\n"
        "- Все времена в ответах переводятся в формат HH:MM:SS (UTC).\n\n"
    )


async def funding_cmd(message: types.Message):
    """
    Хэндлер команды /funding
    """
    args = message.text.split()
    if len(args) < 2 or len(args) > 2:
        return await message.answer("⚠️ Укажи символ, например: /funding BTCUSDT")

    await message.answer('Идет обработка запроса, это займет некоторое время...')

    symbol = args[1].upper()
    data = await get_all_funding(symbol)
    data = normalize_funding_data(data)

    if not data:
        return await message.answer("❌ Не удалось получить данные")

    text = f"📊 Ставки финансирования по {symbol}:\n\n"
    for row in data:
        text += f"{row['exchange']}: {(row['funding_rate'] * 100):.2f}% (время {row['next_funding_time']})\n"

    spread, pair = calc_max_spread(data)
    text += f"\n🔥 Максимальный спред: {(spread * 100):.3f}% ({pair[0]} ↔ {pair[1]})"

    await message.answer(text)


async def funding_spread_chart_cmd(message: types.Message):
    """
    /funding_spread_chart <exchange> <days> <symbol>
    Пример: /funding_spread_chart OKX 7 BTCUSDT
    """
    args = message.text.split()
    if len(args) < 4 or len(args) > 4:
        return await message.answer("⚠️ Пример: /funding_spread_chart OKX 7 BTCUSDT")
    
    await message.answer('Идет обработка запроса, это займет некоторое время...')

    exchange = args[1].upper()
    days = int(args[2])
    symbol = args[3].upper()

    history = await get_history_all(symbol, days)
    if not history:
        return await message.answer("❌ История не найдена")

    rates = [h for h in history if h["exchange"] == exchange]
    rates.sort(key=lambda x: x["funding_time"])

    times = [datetime.utcfromtimestamp(h["funding_time"]/1000) for h in rates]
    funding_rates = [(h["funding_rate"] * 100) * 100 for h in rates]  # в процентах
    cum_rates = np.cumsum(funding_rates)

    total = round(np.sum(funding_rates), 2)
    mean = round(np.mean(funding_rates), 2)
    max_rate = round(np.max(funding_rates), 2)
    min_rate = round(np.min(funding_rates), 2)

    plt.figure(figsize=(10, 5))
    plt.plot(times, cum_rates, label=f"Cumulative Funding Rate {exchange}")
    plt.title(
        f"Cumulative Funding Rate {symbol} ({exchange})\n"
        f"Total: {total}% | Mean(average): {mean}% | Max: {max_rate}% | Min: {min_rate}%"
    )
    plt.xlabel("Time")
    plt.ylabel("Cumulative %")
    plt.legend()
    plt.grid(True)

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        tmp.write(buf.getbuffer())
        tmp_path = tmp.name

    await message.answer_photo(FSInputFile(tmp_path))


async def top_tokens_chart_cmd(message: types.Message):
    """
    /top_tokens_chart  <exchange> <days> <tokens>
    Пример:
    /top_tokens_chart  OKX 3 BTCUSDT,ETHUSDT,SOLUSDT
    """
    args = message.text.split()
    if len(args) < 4:
        return await message.answer(
            "⚠️ Пример: /top_tokens_chart  OKX 3 BTCUSDT,ETHUSDT,SOLUSDT"
        )

    await message.answer('Идет обработка запроса, это займет некоторое время...')

    exchange = args[1].upper()
    days = int(args[2])
    raw_tokens = args[3].replace(', ', ',')
    tokens = [t.strip().upper() for t in raw_tokens.split(",") if t.strip()]

    if not tokens:
        return await message.answer("❌ Укажите хотя бы один токен")

    results = []  # (symbol, times[], cumsum[])

    # Обходим все токены
    for symbol in tokens:
        history = await get_history_all(symbol, days)
        if not history:
            continue

        # фильтруем по бирже
        rates = [h for h in history if h["exchange"].upper() == exchange.upper()]
        rates.sort(key=lambda x: x["funding_time"])
        if not rates:
            continue

        times = [datetime.utcfromtimestamp(h["funding_time"]/1000) for h in rates]
        funding_rates = [(h["funding_rate"] * 100) * 100 for h in rates]
        cum = np.cumsum(funding_rates)

        total = round(cum[-1], 2)
        mean = round(np.mean(funding_rates), 2)
        max_rate = round(np.max(funding_rates), 2)
        min_rate = round(np.min(funding_rates), 2)

        results.append((symbol, total, mean, max_rate, min_rate, times, cum))

    if not results:
        return await message.answer("❌ Нет данных по заданным токенам")

    results.sort(key=lambda x: abs(x[1]), reverse=True)

    plt.figure(figsize=(12, 6))
    for symbol, total, mean, max_rate, min_rate, times, cum in results:
        label = (
            f"{symbol} | Total:{total}% | Mean:{mean}% "
            f"| Max:{max_rate}% | Min:{min_rate}%"
        )
        plt.plot(times, cum, label=label)

    plt.title(f"TOP cumulative funding ({exchange}) - {days}d")
    plt.xlabel("Time")
    plt.ylabel("Cumulative %")
    plt.legend()
    plt.grid(True)

    buf = io.BytesIO()
    plt.savefig(buf, format="png")
    buf.seek(0)
    plt.close()

    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        tmp.write(buf.getbuffer())
        tmp_path = tmp.name

    await message.answer_photo(FSInputFile(tmp_path))


def register_handlers(dp: Dispatcher):
    """
    Регистрация хэндлеров к командам
    """
    dp.message.register(start_cmd, Command(commands=["start"]))
    dp.message.register(funding_cmd, Command(commands=["funding"]))
    dp.message.register(top_tokens_chart_cmd, Command(commands=["top_tokens_chart"]))
    dp.message.register(funding_spread_chart_cmd, Command(commands=["funding_spread_chart"]))
