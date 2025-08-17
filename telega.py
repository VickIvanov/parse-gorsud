import os
import asyncio
import json
from datetime import datetime
from dotenv import load_dotenv
import logging
import requests
from telethon import TelegramClient, events
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError

class Config:
    load_dotenv(encoding="windows-1251")  # указываем кодировку файла .env
    API_ID = int(os.getenv("TELEGRAM_API_ID"))
    API_HASH = os.getenv("TELEGRAM_API_HASH")
    PHONE = os.getenv("TELEGRAM_PHONE")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    CHANNELS = ["dzen_kvartaly_a101", "lyublinskij_park","vick_test_channel"]
    DATA_DIR = "data_telega"
    SESSION_FILE = "safe_session"
    CHUNK_SIZE = 100  # Сообщений за один запрос
    DELAY = 1  # Задержка между запросами (секунды)

# Отключаем прокси из переменных окружения
for var in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    os.environ.pop(var, None)

LOG_DIR = "logs/telega"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=f"{LOG_DIR}/telega.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    encoding="utf-8"
)

os.makedirs(Config.DATA_DIR, exist_ok=True)

def clean_api_hash():
    # Функция больше не требуется, так как конфиденциальные данные теперь в .env
    pass


async def save_messages(channel, messages, is_history=False):
    """Сохраняет сообщения в соответствующий файл (новые сообщения вверху)"""
    filename = f"{Config.DATA_DIR}/{channel}_history.txt" if is_history else f"{Config.DATA_DIR}/{channel}_new.txt"

    # Сначала считываем существующий файл, если он есть
    existing_messages = []
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            existing_messages = f.readlines()

    # Подготавливаем новые сообщения
    new_entries = []
    for msg in messages:
        entry = f"{msg.date} | {msg.text or ''}"
        if msg.media:
            if isinstance(msg.media, MessageMediaPhoto):
                entry += " <фото>"
            elif isinstance(msg.media, MessageMediaDocument):
                entry += " <документ>"
        new_entries.append(f"{entry}\n")

    # Сортируем новые сообщения, чтобы новые были сверху
    new_entries.sort(reverse=True, key=lambda x: x.split(' | ')[0])

    # Записываем с новым порядком: сначала новые, потом старые
    with open(filename, 'w', encoding='utf-8') as f:
        f.writelines(new_entries + existing_messages)

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
    """Объединяет временные файлы в основной, сохраняя новые сообщения вверху"""
    history_file = f"{Config.DATA_DIR}/{channel}_history.txt"
    new_file = f"{Config.DATA_DIR}/{channel}_new.txt"

    if os.path.exists(new_file):
        # Считываем новые сообщения
        with open(new_file, 'r', encoding='utf-8') as f:
            new_messages = f.readlines()

        # Считываем историю, если файл существует
        history_messages = []
        if os.path.exists(history_file):
            with open(history_file, 'r', encoding='utf-8') as f:
                history_messages = f.readlines()

        # Объединяем и сортируем все сообщения (новые вверху)
        all_messages = new_messages + history_messages
        all_messages.sort(reverse=True, key=lambda x: x.split(' | ')[0] if ' | ' in x else '')

        # Записываем результат обратно в файл истории
        with open(history_file, 'w', encoding='utf-8') as f:
            f.writelines(all_messages)

        # Удаляем временный файл
        os.remove(new_file)
        print(f"🔄 Объединены файлы для {channel}")

async def get_all_chats(client):
    """Получает и логирует список всех доступных чатов"""
    print("📋 Получение списка всех доступных чатов...")
    dialogs = await client.get_dialogs()

    log_file = f"{Config.DATA_DIR}/all_chats_log.txt"
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"=== Список чатов на {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")
        for dialog in dialogs:
            chat_type = "Личный чат" if dialog.is_user else "Группа" if dialog.is_group else "Канал"
            chat_id = dialog.entity.id
            chat_name = dialog.name
            username = getattr(dialog.entity, 'username', 'Нет username')
            username = username if username else 'Нет username'

            f.write(f"Тип: {chat_type}\n")
            f.write(f"ID: {chat_id}\n")
            f.write(f"Имя: {chat_name}\n")
            f.write(f"Username: {username}\n")
            f.write("-" * 50 + "\n")

    print(f"✅ Список чатов сохранен в {log_file} (всего {len(dialogs)} чатов)")

import logging
import os
import json
from datetime import datetime
import requests

async def process_all_private_chats(client, private_chats):
    for chat_id in private_chats:
        try:
            print(f"⏳ Обработка чата {chat_id}...")
            messages = await client.get_messages(chat_id, limit=15)
            if not messages:
                print(f"⚠️ Нет сообщений для {chat_id}")
                continue

            # Формируем историю чата
            chat_history = []
            for msg in reversed(messages):
                sender = "Я" if msg.out else "Пользователь"
                content = msg.text or "<медиа-контент>"
                chat_history.append(f"{sender}: {content}")
            chat_text = "\n".join(chat_history)

            # Инструкция для Groq
            prompt = (
                "Ты — профессиональный менеджер по продажам. На основе истории диалога ниже "
                "составь очень короткое сообщение (1-2 предложения) для клиента, чтобы напомнить о себе и предложить что-то полезное по теме обсуждения. "
                "Пиши только на русском языке. Не используй другие языки. Только текст сообщения, без пояснений и кавычек.\n\n"
                f"История диалога:\n{chat_text}"
            )

            GROQ_API_KEY = os.getenv("GROQ_API_KEY")
            GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "meta-llama/llama-4-maverick-17b-128e-instruct",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 100
            }

            logging.info(f"Отправка запроса в Groq: {json.dumps(payload, ensure_ascii=False)}")
            response = requests.post(GROQ_API_URL, headers=headers, json=payload, timeout=30)
            logging.info(f"Ответ Groq: {response.status_code} {response.text}")
            print(response.status_code, response.text)
            response.raise_for_status()
            data = response.json()
            ai_response = data["choices"][0]["message"]["content"]

            # Сохраняем черновик в файл
            draft_file = f"{Config.DATA_DIR}/{chat_id}_draft.txt"
            with open(draft_file, 'w', encoding='utf-8') as f:
                f.write(f"=== Черновик сообщения для {chat_id} от {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")
                f.write(ai_response)
            logging.info(f"Черновик для {chat_id} сохранен в {draft_file}")

            # Получаем username пользователя
            user = await client.get_entity(chat_id)
            username = getattr(user, 'username', None)
            if not username:
                username = f"id{user.id}"

            # Отправляем два сообщения пользователю megavick
            await client.send_message("megavick", ai_response)
            logging.info(f"Отправлено сообщение megavick: {ai_response}")
            await asyncio.sleep(2)  # Пауза 2 секунды между отправками
            await client.send_message("megavick", f"для @{username}")
            logging.info(f"Отправлено сообщение megavick: для @{username}")

        except Exception as e:
            print(f"⚠️ Ошибка при обработке чата {chat_id}: {str(e)}")
            logging.error(f"Ошибка при обработке чата {chat_id}: {str(e)}")

async def get_private_chats(client):
    """
    Возвращает список идентификаторов всех личных чатов (username или id).
    """
    private_chats = []
    dialogs = await client.get_dialogs()
    for dialog in dialogs:
        if dialog.is_user:
            chat_id = dialog.entity.username or f"id{dialog.entity.id}"
            private_chats.append(chat_id)
    return private_chats

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

        # Получаем список всех чатов
        await get_all_chats(client)

        # Получаем список личных чатов
        private_chats = []
        dialogs = await client.get_dialogs()
        for dialog in dialogs:
            if dialog.is_user:
                chat_id = dialog.entity.username or f"id{dialog.entity.id}"
                private_chats.append(chat_id)

        print(f"👥 Найдено {len(private_chats)} личных чатов")

        # Загружаем последние 15 сообщений из каждого личного чата
        for chat_id in private_chats:
            print(f"⏳ Загрузка последних сообщений для чата {chat_id}...")
            try:
                messages = await client.get_messages(chat_id, limit=15)
                if messages:
                    await save_messages(chat_id, messages, is_history=True)
                    # Создаём черновик для каждого чата
                    await create_groq_draft(client, chat_id, messages)
            except Exception as e:
                print(f"⚠️ Ошибка при загрузке сообщений для {chat_id}: {str(e)}")

        # Загружаем историю для каждого канала из настроек
        for channel in Config.CHANNELS:
            if not os.path.exists(f"{Config.DATA_DIR}/{channel}_history.txt"):
                await fetch_history(client, channel)
            else:
                print(f"✓ История {channel} уже загружена")

        # Объединяем каналы и личные чаты для мониторинга
        all_monitored_chats = Config.CHANNELS + private_chats

        @client.on(events.NewMessage(chats=all_monitored_chats))
        async def handler(event):
            try:
                if event.chat is None:
                    print(f"⚠️ Получено сообщение с event.chat=None: {event.message.id}")
                    if hasattr(event.message, 'peer_id') and hasattr(event.message.peer_id, 'user_id'):
                        user_id = event.message.peer_id.user_id
                        try:
                            user = await event.client.get_entity(user_id)
                            chat_id = user.username or f"id{user.id}"
                            print(f"📝 Определён chat_id: {chat_id}")
                            await save_messages(chat_id, [event.message])
                            return
                        except Exception as e:
                            print(f"⚠️ Не удалось получить entity пользователя: {e}")
                            chat_id = f"id{user_id}"
                            await save_messages(chat_id, [event.message])
                            return
                    debug_file = f"{Config.DATA_DIR}/unknown_messages_debug.txt"
                    with open(debug_file, 'a', encoding='utf-8') as f:
                        f.write(f"\n=== {event.message.date} | ID: {event.message.id} ===\n")
                        f.write(f"Текст: {event.message.text or '<нет текста>'}\n")
                else:
                    chat_id = event.chat.username or f"id{event.chat.id}"
                    await save_messages(chat_id, [event.message])
            except Exception as e:
                print(f"⚠️ Ошибка при обработке сообщения: {str(e)}")

        print(f"👂 Ожидание новых сообщений для {len(all_monitored_chats)} чатов...")
        monitor_task = asyncio.create_task(client.run_until_disconnected())

        # Периодически объединяем файлы
        while True:
            await asyncio.sleep(60 * 5)  # Каждые 5 минут
            for channel in all_monitored_chats:
                await merge_files(channel)

    except Exception as e:
        print(f"⚠️ Ошибка: {str(e)}")
    finally:
        await client.disconnect()

async def create_groq_draft(client, chat_id, messages):
    # Отключаем прокси
    for var in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
        os.environ.pop(var, None)
    print(f"🤖 Создание черновика для {chat_id} с помощью Groq...")

    GROQ_API_KEY = os.getenv("GROQ_API_KEY")
    GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

    # Формируем историю чата
    chat_history = []
    for msg in reversed(messages):
        sender = "Я" if msg.out else "Пользователь"
        content = msg.text or "<медиа-контент>"
        chat_history.append(f"{sender}: {content}")

    chat_text = "\n".join(chat_history)

    # Краткая инструкция для Groq на русском
    prompt = (
        "Ты — профессиональный менеджер по продажам. На основе истории диалога ниже "
        "составь очень короткое сообщение (1-2 предложения) для клиента, чтобы напомнить о себе и предложить что-то полезное по теме обсуждения. "
        "Пиши только на русском языке. Не используй другие языки. Только текст сообщения, без пояснений и кавычек.\n\n"
        f"История диалога:\n{chat_text}"
    )

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "meta-llama/llama-4-maverick-17b-128e-instruct",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 100
    }
    try:
        logging.info(f"Отправка запроса в Groq: {json.dumps(payload, ensure_ascii=False)}")
        response = requests.post(GROQ_API_URL, headers=headers, json=payload, timeout=30)
        logging.info(f"Ответ Groq: {response.status_code} {response.text}")
        print(response.status_code, response.text)
        response.raise_for_status()
        data = response.json()
        ai_response = data["choices"][0]["message"]["content"]

        # Сохраняем черновик в файл
        draft_file = f"{Config.DATA_DIR}/{chat_id}_draft.txt"
        with open(draft_file, 'w', encoding='utf-8') as f:
            f.write(f"=== Черновик сообщения для {chat_id} от {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n\n")
            f.write(ai_response)

        print(f"✅ Черновик для {chat_id} сохранен в {draft_file}")
        logging.info(f"Черновик для {chat_id} сохранен в {draft_file}")

        # Получаем username пользователя
        user = await client.get_entity(chat_id)
        username = getattr(user, 'username', None)
        if not username:
            username = f"id{user.id}"

        # Отправляем два сообщения пользователю megavick
        await client.send_message("megavick", ai_response)
        logging.info(f"Отправлено сообщение megavick: {ai_response}")
        await client.send_message("megavick", f"для @{username}")
        logging.info(f"Отправлено сообщение megavick: для @{username}")

    except Exception as e:
        print(f"⚠️ Ошибка при создании черновика: {str(e)}")
        logging.error(f"Ошибка при создании черновика: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🚪 Корректное завершение работы")
        # Перед выходом объединяем файлы
        all_monitored_chats = Config.CHANNELS + private_chats
        for channel in all_monitored_chats:
            asyncio.run(merge_files(channel))
    except NameError:
        print("Переменная private_chats не определена")