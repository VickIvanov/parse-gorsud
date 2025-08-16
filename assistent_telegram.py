#!/usr/bin/env python3
import asyncio
import logging
import signal
import pathlib
import os
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telethon.tl.types import User, PeerUser
from yandex_cloud_ml_sdk import YCloudML
from yandex_cloud_ml_sdk.search_indexes import (
    StaticIndexChunkingStrategy,
    TextSearchIndexType,
)


# Конфигурация
class Config:
    load_dotenv()  # Загружаем переменные из .env файла

    # Telegram API
    API_ID = int(os.getenv("TELEGRAM_API_ID"))
    API_HASH = os.getenv("TELEGRAM_API_HASH")
    PHONE = os.getenv("TELEGRAM_PHONE")
    TARGET_USERS = os.getenv("TARGET_USERS", "").split(",")  # Список пользователей через запятую

    # Yandex Cloud
    YANDEX_FOLDER_ID = os.getenv("YANDEX_FOLDER_ID")
    YANDEX_AUTH_TOKEN = os.getenv("YANDEX_AUTH_TOKEN")

    # Настройки приложения
    SESSION_FILE = os.getenv("SESSION_FILE", "safe_session")
    DATA_PATH = os.getenv("DATA_PATH", "data/tours-example")
    LOGS_DIR = os.getenv("LOGS_DIR", "logs/telega")
    MESSAGE_DELAY = int(os.getenv("MESSAGE_DELAY", "3"))  # Задержка между сообщениями (секунды)


# Настройка логирования
def setup_logging():
    pathlib.Path(Config.LOGS_DIR).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Файловый лог
    file_handler = logging.FileHandler(f"{Config.LOGS_DIR}/assistant.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Консольный вывод
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


logger = setup_logging()


def get_username(user: User) -> str:
    return user.username or str(user.id)


def write_to_user_log(username: str, message: str, is_assistant: bool = False):
    log_file = f"{Config.LOGS_DIR}/{username.lstrip('@')}.log"
    with open(log_file, 'a', encoding='utf-8') as f:
        prefix = "ASSISTANT" if is_assistant else "USER"
        f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {prefix}: {message}\n")


async def initialize_telegram_client():
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
        await client.connect()

        if not await client.is_user_authorized():
            logger.info("Требуется авторизация...")
            await client.send_code_request(Config.PHONE)
            code = input("Введите код из Telegram: ")

            try:
                await client.sign_in(Config.PHONE, code)
            except SessionPasswordNeededError:
                password = input("Введите пароль 2FA: ")
                await client.sign_in(password=password)

        logger.info("Авторизация успешна!")
        return client

    except Exception as e:
        logger.error(f"Ошибка авторизации: {e}")
        raise


async def setup_yandex_assistant():
    sdk = YCloudML(
        folder_id=Config.YANDEX_FOLDER_ID,
        auth=Config.YANDEX_AUTH_TOKEN,
    )

    paths = pathlib.Path(Config.DATA_PATH).iterdir()
    files = [sdk.files.upload(path, ttl_days=5, expiration_policy="static") for path in paths]

    operation = sdk.search_indexes.create_deferred(
        files,
        index_type=TextSearchIndexType(
            chunking_strategy=StaticIndexChunkingStrategy(
                max_chunk_size_tokens=700,
                chunk_overlap_tokens=300,
            )
        ),
    )
    search_index = operation.wait()
    tool = sdk.tools.search_index(search_index)

    assistant = sdk.assistants.create(
        "yandexgpt",
        instruction="Ты — помощник по внутренней документации компании. Отвечай вежливо. Если информация не содержится в документах ниже, не придумывай ответ.",
        tools=[tool]
    )
    thread = sdk.threads.create()

    return sdk, assistant, thread, files, search_index


async def send_initial_messages(client, users):
    for user in users:
        try:
            await client.send_message(
                entity=user,
                message="🤖 Ассистент запущен! Отправьте сообщение для начала работы."
            )
            logger.info(f"Приветствие отправлено {get_username(user)}")
            await asyncio.sleep(Config.MESSAGE_DELAY)
        except Exception as e:
            logger.error(f"Ошибка отправки приветствия {get_username(user)}: {e}")


async def handle_message(event, assistant, thread, users):
    try:
        # Получаем отправителя через event.get_sender()
        sender = await event.get_sender()
        if not sender:
            logger.error("Не удалось получить информацию об отправителе")
            return False

        username = get_username(sender)

        # Проверяем, что отправитель в списке
        if not any(user.id == sender.id for user in users):
            logger.debug(f"Игнорируем сообщение от {username}")
            return False

        message_text = event.message.text
        write_to_user_log(username, message_text)
        logger.info(f"Сообщение от {username}: {message_text}")

        if message_text.lower() == "exit":
            await event.reply("Завершаю работу...")
            return True  # Флаг завершения

        # Обработка запроса
        thread.write(message_text)
        run = assistant.run(thread)
        result = run.wait()

        # Отправка ответа
        await event.reply(result.text)
        logger.info(f"Ответ отправлен {username}")

        # Логирование
        log_response = f"{result.text}\n"
        if hasattr(result, 'citations'):
            for i, citation in enumerate(result.citations, 1):
                for source in citation.sources:
                    if source.type == "filechunk":
                        log_response += (
                            f"Фрагмент {i}:\n"
                            f"Файл: {source.file.id}\n"
                            f"Тип: {source.file.mime_type}\n\n"
                        )
        write_to_user_log(username, log_response, is_assistant=True)
        return False

    except Exception as e:
        logger.error(f"Ошибка обработки: {e}", exc_info=True)
        await event.reply("⚠️ Произошла ошибка при обработке запроса")
        return False


async def run_assistant():
    client = None
    try:
        client = await initialize_telegram_client()
        yandex_objects = await setup_yandex_assistant()
        sdk, assistant, thread, files, search_index = yandex_objects

        # Загружаем пользователей
        users = []
        for username in Config.TARGET_USERS:
            try:
                user = await client.get_entity(username)
                users.append(user)
                logger.info(f"Добавлен пользователь: {get_username(user)}")
            except Exception as e:
                logger.error(f"Ошибка загрузки {username}: {e}")

        if not users:
            logger.error("Нет пользователей для диалога")
            return

        await send_initial_messages(client, users)

        # Обработчик сообщений
        @client.on(events.NewMessage(incoming=True))
        async def handler(event):
            should_exit = await handle_message(event, assistant, thread, users)
            if should_exit:
                raise KeyboardInterrupt

        logger.info("Ассистент готов к работе. Ожидание сообщений...")

        # Ожидаем завершения
        while True:
            await asyncio.sleep(1)

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Получен сигнал завершения")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
    finally:
        if client:
            await client.disconnect()
        logger.info("Работа завершена")


def handle_signal(sig, frame):
    logger.info(f"Получен сигнал {sig}, завершение работы...")
    raise KeyboardInterrupt


if __name__ == "__main__":
    # Проверка наличия обязательных переменных окружения
    required_vars = ["TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_PHONE",
                     "YANDEX_FOLDER_ID", "YANDEX_AUTH_TOKEN"]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"Ошибка: Отсутствуют обязательные переменные окружения: {', '.join(missing_vars)}")
        print("Создайте файл .env и укажите все необходимые переменные")
        exit(1)

    # Обработка сигналов ОС
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Запуск ассистента
    asyncio.run(run_assistant())