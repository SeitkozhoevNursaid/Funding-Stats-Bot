from aiogram import Dispatcher, types
from aiogram.filters import Command

from config.funding_fetcher import get_all_funding
from utils import calc_max_spread, normalize_funding_data


async def start_cmd(message: types.Message):
    """
    Хэндлер команды /start
    """
    await message.answer(
        "Привет Я Funding-Stats-Bot!\n\n"
        "Команды:\n"
        "/funding <symbol> – ставки по активу\n"
        "Пример: /funding BTCUSDT"
    )


async def funding_cmd(message: types.Message):
    """
    Хэндлер команды /funding
    """
    args = message.text.split()
    if len(args) < 2:
        return await message.answer("⚠️ Укажи символ, например: /funding BTCUSDT")

    symbol = args[1].upper()
    data = await get_all_funding(symbol)
    data = normalize_funding_data(data)
    print(f"our_dataaaa    {data}")

    if not data:
        return await message.answer("❌ Не удалось получить данные")

    text = f"📊 Ставки финансирования по {symbol}:\n\n"
    for row in data:
        print(f'roww++++          {row}')
        text += f"{row['exchange']}: {row['funding_rate']}% (время {row['next_funding_time']})\n"

    spread, pair = calc_max_spread(data)
    text += f"\n🔥 Максимальный спред: {spread:.5f}% ({pair[0]} ↔ {pair[1]})"

    await message.answer(text)


def register_handlers(dp: Dispatcher):
    """
    Регистрация хэндлеров к командам
    """
    dp.message.register(start_cmd, Command(commands=["start"]))
    dp.message.register(funding_cmd, Command(commands=["funding"]))
