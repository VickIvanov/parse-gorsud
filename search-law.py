# !/usr/bin/env python3

from __future__ import annotations

import pathlib
import hashlib
import os
import requests, json
from typing import Dict, List, Optional
from dotenv import load_dotenv
from yandex_cloud_ml_sdk import YCloudML
from yandex_cloud_ml_sdk import YCloudML

# ========== НАСТРОЙКИ ==========
# Загружаем переменные окружения из .env файла
load_dotenv()

# Получаем переменные окружения
FOLDER_ID = os.getenv("YANDEX_FOLDER_ID")  # ID каталога Yandex Cloud
AUTH_TOKEN = os.getenv("YANDEX_AUTH_TOKEN")  # IAM-токен или API-ключ
DATA_PATH = os.getenv("DATA_PATH", "data/test-law-norm")  # Путь к папке с файлами
ASSISTANT_NAME = os.getenv("ASSISTANT_NAME", "legal-assistant")  # Уникальное имя ассистента


# ===============================

class LegalAssistant:
    def __init__(self):
        self.sdk = YCloudML(folder_id=FOLDER_ID, auth=AUTH_TOKEN)
        self.existing_files: Dict[str, dict] = {}  # {filename: {id, hash}}
        self.current_file_ids: List[str] = []
        self.assistant_id: Optional[str] = None

    def _calculate_file_hash(self, file_path: pathlib.Path) -> str:
        """Вычисляем SHA-256 хеш файла"""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _get_existing_files(self):
        """Получаем список уже загруженных файлов"""
        files = {}
        try:
            print("📝 Получение списка файлов с сервера...")
            # Получаем все файлы с сервера
            all_files = list(self.sdk.files.list())
            print(f"📊 Найдено файлов на сервере: {len(all_files)}")

            for file in all_files:
                try:
                    # Получаем URL файла, который содержит имя файла
                    file_url = file.get_url()
                    file_name = file_url.split('/')[-1]  # Извлекаем имя файла из URL

                    if file_name and file.description:  # Проверяем имя и описание
                        files[file_name] = {
                            'id': file.id,
                            'hash': file.description
                        }
                except Exception as e:
                    print(f"⚠️ Ошибка обработки файла: {str(e)}")
                    continue

                if file.name and file.description:  # Проверяем имя и описание
                    try:
                        files[file.name] = {
                            'id': file.id,
                            'hash': file.description
                        }
                    except Exception as e:
                        print(f"⚠️ Ошибка обработки файла {file.name}: {str(e)}")
                        continue
        except Exception as e:
            print(f"⚠️ Ошибка получения файлов: {str(e)}")

        print(f"✅ Обработано файлов: {len(files)}")
        return files

    def delete_all_cloud_files(self):
        """Удаление файлов через REST API"""
        try:
            print("🔄 Получаем список файлов из облака...")
            files = list(self.sdk.files.list())

            if not files:
                print("ℹ️ В облаке нет файлов для удаления")
                return True

            print(f"🗑️ Найдено {len(files)} файлов для удаления")
            deleted_count = 0

            for file in files:
                if not file.id:
                    continue

                print(f"\n⏳ Удаляем файл:")
                print(f"  ID: {file.id}")
                print(f"  Name: {getattr(file, 'name', 'н/д')}")
                print(f"  URL: {getattr(file, 'get_url', lambda: 'н/д')()}")
                print(f"  Description: {getattr(file, 'description', 'н/д')}")
                print(f"  Status: {getattr(file, 'status', 'н/д')}")

                try:
                    delete_url = f"https://llm.api.cloud.yandex.net/v1/assistants/files/{file.id}"
                    print(f"  Delete URL: {delete_url}")

                    response = requests.delete(
                        delete_url,
                        headers={
                            "Authorization": f"Bearer {AUTH_TOKEN}",
                            "x-folder-id": FOLDER_ID,
                            "x-client-request-id": file.id
                        }
                    )

                    if response.status_code == 200 or response.status_code == 204:
                        deleted_count += 1
                        print("✅ Успешно")
                    else:
                        print(f"❌ Ошибка API [{response.status_code}] {response.text}")

                except Exception as e:
                    print(f"❌ Ошибка: {str(e)}")

            print(f"\n🏁 Итог: удалено {deleted_count} файлов из {len(files)}")
            return True

        except Exception as e:
            print(f"⚠️ Критическая ошибка: {str(e)}")
            return False

    def upload_files(self):
        """Загружаем файлы с явным указанием MIME-типа"""
        paths = [p for p in pathlib.Path(DATA_PATH).iterdir()
                 if p.is_file() and p.suffix.lower() == '.txt']

        if not paths:
            print("ℹ️ В папке не найдено .txt файлов")
            return False

        self.existing_files = self._get_existing_files()
        new_files = 0

        for path in paths:
            file_name = path.name
            file_hash = self._calculate_file_hash(path)

            # Проверяем, есть ли файл среди уже загруженных и совпадает ли хеш
            if file_name in self.existing_files:
                existing_hash = self.existing_files[file_name]['hash']
                print(f"📝 Отладка для {file_name}:")
                print(f"  Существующий хеш: {existing_hash}")
                print(f"  Новый хеш: {file_hash}")
                if existing_hash == file_hash:
                    print(f"⏩ Файл {file_name} уже загружен, пропускаем")
                    if self.existing_files[file_name]['id'] not in self.current_file_ids:
                        self.current_file_ids.append(self.existing_files[file_name]['id'])
                    continue

            try:
                print(f"📝 Отладка для {file_name}:")
                print(f"  Существующий хеш: {self.existing_files.get(file_name, {}).get('hash', 'отсутствует')}")
                print(f"  Новый хеш: {file_hash}")
                print(f"⬆️ Загрузка {file_name}...", end=' ')
                file = self.sdk.files.upload(
                    str(path.absolute()),
                    mime_type="text/plain",
                    description=file_hash  # Сохраняем хеш в описании файла
                )
                self.current_file_ids.append(file.id)
                self.existing_files[file_name] = {
                    'id': file.id,
                    'hash': file_hash
                }
                new_files += 1
                print("✅ Успешно")
            except Exception as e:
                print(f"❌ Ошибка: {str(e)}")

        print(f"\nВсего файлов: {len(self.current_file_ids)} ({new_files} новых)")
        return bool(self.current_file_ids)

    def create_assistant(self):
        """Создаем нового ассистента"""
        if not self.current_file_ids:
            print("⚠️ Нет файлов для ассистента")
            return False

        try:
            assistants = self.sdk.assistants.list()
            for assistant in assistants:
                if assistant.name == ASSISTANT_NAME:
                    print(f"ℹ️ Найден существующий ассистент: {assistant.id}")
                    print("ℹ️ Для обновления удалите его вручную через консоль")
                    return False
        except Exception as e:
            print(f"⚠️ Ошибка поиска ассистента: {str(e)}")

        print("\n👨‍💼 Создание нового ассистента...")
        try:
            assistant = self.sdk.assistants.create(
                name=ASSISTANT_NAME,
                model="yandexgpt",
                instruction="Ты — помощник по юридическим документам. Отвечай точно, используя только предоставленные файлы.",
                file_ids=self.current_file_ids
            )
            self.assistant_id = assistant.id
            print(f"🤖 Ассистент создан: {self.assistant_id}")
            return True
        except Exception as e:
            print(f"❌ Ошибка создания ассистента: {str(e)}")
            return False

    def chat_loop(self):
        """Интерактивный чат с ассистентом"""
        if not self.assistant_id:
            print("⚠️ Ассистент не создан")
            return

        print("\n💬 Режим диалога. Введите 'exit' для выхода")

        try:
            while True:
                input_text = input("\nВы: ")
                if input_text.lower() == 'exit':
                    break

                thread = self.sdk.threads.create()
                thread.add_message(input_text)

                run = thread.runs.create(assistant_id=self.assistant_id)
                result = run.wait()

                if result.last_message:
                    print("\n🤖 Ассистент:", result.last_message.text)

                thread.delete()

        except KeyboardInterrupt:
            print("\nЗавершение работы...")

    def run(self):
        """Основной цикл работы"""
        # Проверяем наличие обязательных переменных окружения
        if not FOLDER_ID:
            print("❌ Ошибка: Не указан YANDEX_FOLDER_ID в .env файле")
            return

        if not AUTH_TOKEN:
            print("❌ Ошибка: Не указан YANDEX_AUTH_TOKEN в .env файле")
            return

        if not self.upload_files():
            return

        if not self.create_assistant():
            return

        self.chat_loop()
        print("\n🏁 Работа завершена")

if __name__ == "__main__":
    assistant = LegalAssistant()
    assistant.delete_all_cloud_files()
    assistant.run()