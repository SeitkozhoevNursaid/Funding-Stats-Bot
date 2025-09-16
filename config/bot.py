import asyncio
from aiogram import Bot, Dispatcher
from constants import BOT_TOKEN
from handlers import register_handlers
from ssl_patch import patch_ssl_correctly


async def main():
    """
    Запуск бота!
    """
    patch_ssl_correctly()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    register_handlers(dp)

    print("Бот запущен!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
