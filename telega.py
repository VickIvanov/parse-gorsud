import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError

class Config:
    load_dotenv()
    API_ID = int(os.getenv("TELEGRAM_API_ID"))
    API_HASH = os.getenv("TELEGRAM_API_HASH")
    PHONE = os.getenv("TELEGRAM_PHONE")
    CHANNELS = ["dzen_kvartaly_a101", "lyublinskij_park","vick_test_channel"]
    DATA_DIR = "data_telega"
    SESSION_FILE = "safe_session"
    CHUNK_SIZE = 100  # Сообщений за один запрос
    DELAY = 1  # Задержка между запросами (секунды)


os.makedirs(Config.DATA_DIR, exist_ok=True)

def clean_api_hash():
    # Функция больше не требуется, так как конфиденциальные данные теперь в .env
    pass


async def save_messages(channel, messages, is_history=False):
    """Сохраняет сообщения в соответствующий файл"""
    filename = f"{Config.DATA_DIR}/{channel}_history.txt" if is_history else f"{Config.DATA_DIR}/{channel}_new.txt"

    with open(filename, 'a', encoding='utf-8') as f:
        for msg in messages:
            entry = f"{msg.date} | {msg.text or ''}"
            if msg.media:
                if isinstance(msg.media, MessageMediaPhoto):
                    entry += " <фото>"
                elif isinstance(msg.media, MessageMediaDocument):
                    entry += " <документ>"
            f.write(f"{entry}\n")

    print(f"💾 Сохранено {len(messages)} сообщений в {filename}")


async def fetch_history(client, channel):
    """Постепенно загружает историю сообщений"""
    print(f"⏳ Загрузка истории для {channel}...")
    all_messages = []
    offset_id = 0

    try:
        while True:
            messages = await client.get_messages(
                channel,
                limit=Config.CHUNK_SIZE,
                offset_id=offset_id
            )

            if not messages:
                break

            await save_messages(channel, messages, is_history=True)
            all_messages.extend(messages)
            offset_id = messages[-1].id

            await asyncio.sleep(Config.DELAY)

    except FloodWaitError as e:
        print(f"⚠️ Ожидаем {e.seconds} секунд (лимит Telegram)")
        await asyncio.sleep(e.seconds)
        return await fetch_history(client, channel)

    return all_messages


async def monitor_new_messages(client):
    """Мониторит новые сообщения"""

    @client.on(events.NewMessage(chats=Config.CHANNELS))
    async def handler(event):
        await save_messages(event.chat.username, [event.message])

    print("👂 Ожидание новых сообщений...")
    await client.run_until_disconnected()


async def merge_files(channel):
    """Объединяет временные файлы в основной"""
    history_file = f"{Config.DATA_DIR}/{channel}_history.txt"
    new_file = f"{Config.DATA_DIR}/{channel}_new.txt"

    if os.path.exists(new_file):
        with open(new_file, 'r', encoding='utf-8') as f:
            new_messages = f.readlines()

        with open(history_file, 'a', encoding='utf-8') as f:
            f.writelines(new_messages)

        os.remove(new_file)
        print(f"🔄 Объединены файлы для {channel}")


async def main():
    client = TelegramClient(
        Config.SESSION_FILE,
        Config.API_ID,
        Config.API_HASH,
        device_model="PC 10.0",
        system_version="Linux",
        app_version="1.0.0",
        system_lang_code="ru"
    )

    try:
        await client.start(phone=Config.PHONE)
        clean_api_hash()

        # Загружаем историю для каждого канала
        for channel in Config.CHANNELS:
            if not os.path.exists(f"{Config.DATA_DIR}/{channel}_history.txt"):
                await fetch_history(client, channel)
            else:
                print(f"✓ История {channel} уже загружена")

        # Запускаем мониторинг новых сообщений
        monitor_task = asyncio.create_task(monitor_new_messages(client))

        # Периодически объединяем файлы
        while True:
            await asyncio.sleep(60 * 5)  # Каждые 5 минут
            for channel in Config.CHANNELS:
                await merge_files(channel)

    except Exception as e:
        print(f"⚠️ Ошибка: {str(e)}")
    finally:
        await client.disconnect()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🚪 Корректное завершение работы")
        # Перед выходом объединяем файлы
        for channel in Config.CHANNELS:
            asyncio.run(merge_files(channel))