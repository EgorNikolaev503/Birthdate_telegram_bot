import pandas as pd
import asyncio
import logging
from aiogram import Bot, Dispatcher
from datetime import datetime, timedelta

TOKEN = "TOKEN"  # токен бота
CHAT_ID = "-1002107639055"  # ID чата, можно взять в @username_to_id_bot
CSV_PATH = "test.csv"  # файл csv c днями рождения

# csv можно выгрузить из яндекс форм или подобного:
"""
birthdate;username;full_name
22.03.2006;@nikegr503;Егорчик
26.05.2007;@Username0074135;Димасик
27.01.2007;@kak_tolkol_tak_srazzu;Андрей
"""

SEND_HOUR = 12  # час в который будет приходить сообщение (по локальному времени)
SEND_MINUTE = 49  # минута в которую будет приходить сообщение (по локальному времени)

bot = Bot(token=TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)


def get_today_birthdays():
    today = datetime.now().strftime("%d.%m")
    df = pd.read_csv(CSV_PATH, delimiter=';')
    print(df)
    df["birthdate"] = df["birthdate"].astype(str)
    birthdays = df[df["birthdate"].str.startswith(today)][["username", "full_name"]].values.tolist()
    return birthdays


async def send_birthday_message():
    print(1)
    birthdays = get_today_birthdays()
    if birthdays:
        message = "🎉 Сегодня день рождения празднуют: \n"
        message += "\n".join([f"{user[0]} ({user[1]})" for user in birthdays])
        await bot.send_message(CHAT_ID, message)


async def scheduled_task():
    while True:
        now = datetime.now()
        target_time = now.replace(hour=SEND_HOUR, minute=SEND_MINUTE, second=0, microsecond=0)

        if now > target_time:
            target_time += timedelta(days=1)
        sleep_seconds = (target_time - now).total_seconds()
        await asyncio.sleep(sleep_seconds)
        await send_birthday_message()


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(scheduled_task())
    loop.run_until_complete(dp.start_polling(bot, allowed_updates=["message", "chat_member", "callback_query"]))
