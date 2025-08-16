import re
import shutil
from pathlib import Path

# Конфигурация
PROCESSED_DIR = Path("data/processed_files")
DAMAGED_DIR = PROCESSED_DIR / "damaged"
LOG_FILE = Path("logs/conv.log")


def process_damaged_files():
    """Обработка поврежденных файлов из лога"""

    # Создаем папку для поврежденных файлов
    DAMAGED_DIR.mkdir(parents=True, exist_ok=True)

    # Читаем лог и ищем ошибки
    damaged_files = set()
    pattern = re.compile(r'Ошибка конвертации (.*?\.docx?):')

    with open(LOG_FILE, 'r', encoding='utf-8') as log:
        for line in log:
            match = pattern.search(line)
            if match:
                damaged_files.add(match.group(1))

    print(f"Найдено {len(damaged_files)} поврежденных файлов")

    # Копируем поврежденные файлы
    copied = 0
    for filename in damaged_files:
        src_file = PROCESSED_DIR / filename
        dst_file = DAMAGED_DIR / filename

        if src_file.exists():
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy2(src_file, dst_file)
                copied += 1
            except Exception as e:
                print(f"Ошибка копирования {filename}: {e}")

    print(f"Скопировано {copied} из {len(damaged_files)} файлов")
    print(f"Поврежденные файлы сохранены в: {DAMAGED_DIR}")


if __name__ == "__main__":
    process_damaged_files()
# Упрощенный скрипт для анализа лога и копирования поврежденных файлов