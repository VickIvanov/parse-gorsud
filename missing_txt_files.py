import os
import time
from pathlib import Path
from docx import Document
import win32com.client
from tqdm import tqdm
from datetime import datetime

# ================== КОНФИГУРАЦИЯ ==================
PROCESSED_DIR = Path("data/processed_files")  # Папка с doc/docx файлами
TXT_DIR = Path("data/test_txt")  # Основная папка с txt-файлами
MISSING_DIR = TXT_DIR / "missing"  # Папка для недостающих txt-файлов
LOGS_DIR = Path("logs")  # Папка для логов
LOG_FILE = LOGS_DIR / f"conversion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

# Поддерживаемые форматы исходных файлов
SOURCE_EXTENSIONS = ('.doc', '.docx')


# ================== ФУНКЦИИ КОНВЕРТАЦИИ ==================

def convert_to_txt(doc_path, txt_path):
    """Конвертирует doc/docx в txt"""
    try:
        if doc_path.suffix.lower() == '.docx':
            doc = Document(doc_path)
            text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
            txt_path.write_text(text, encoding='utf-8')
            return True

        elif doc_path.suffix.lower() == '.doc':
            if os.name == 'nt':
                try:
                    word = win32com.client.Dispatch("Word.Application")
                    word.Visible = False
                    doc = word.Documents.Open(str(doc_path.resolve()))
                    doc.SaveAs(str(txt_path.resolve()), FileFormat=2)
                    doc.Close()
                    word.Quit()
                    return True
                except Exception as e:
                    print(f"Ошибка конвертации DOC: {e}")
                    return False
            return False

    except Exception as e:
        print(f"Ошибка конвертации {doc_path.name}: {e}")
        return False


# ================== ОСНОВНАЯ ЛОГИКА ==================
def find_missing_conversions():
    """Находит doc/docx файлы для конвертации с учетом существующих txt"""
    # Получаем существующие txt файлы
    existing_txt = {
        (txt_file.parent.relative_to(TXT_DIR), txt_file.stem.lower())
        for txt_file in TXT_DIR.rglob('*.txt')
    }

    # Собираем все doc/docx файлы по именам
    doc_files_by_name = {}
    for ext in SOURCE_EXTENSIONS:
        for doc_file in PROCESSED_DIR.rglob(f'*{ext}'):
            key = (doc_file.relative_to(PROCESSED_DIR).parent, doc_file.stem.lower())
            if key not in doc_files_by_name:
                doc_files_by_name[key] = []
            doc_files_by_name[key].append(doc_file)

    # Определяем файлы для конвертации
    missing_files = []
    for key, files in doc_files_by_name.items():
        if len(files) == 1:  # Один файл
            if key not in existing_txt:  # Нет txt файла
                missing_files.append(files[0])
        else:  # Несколько файлов с одинаковым именем
            # Сортируем файлы (.doc идут первыми)
            files.sort(key=lambda x: x.suffix.lower())
            if key not in existing_txt:
                # Если нет txt, конвертируем все файлы
                missing_files.extend(files)
            else:
                # Если есть txt, конвертируем только дополнительные файлы
                missing_files.extend(files[1:])

    return missing_files


def process_missing_files():
    """Обрабатывает недостающие файлы"""
    # Создаем необходимые папки
    MISSING_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Получаем список файлов для конвертации
    missing_files = find_missing_conversions()

    if not missing_files:
        print("Все файлы уже сконвертированы в txt")
        return

    start_time = time.time()
    print(f"Найдено {len(missing_files)} файлов для конвертации")
    print(f"Лог будет сохранен в: {LOG_FILE}")

    # Заголовок лог-файла
    with open(LOG_FILE, 'w', encoding='utf-8') as log:
        log.write(f"=== Лог конвертации doc/docx в txt ===\n")
        log.write(f"Дата начала: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        log.write(f"Папка с исходными файлами: {PROCESSED_DIR}\n")
        log.write(f"Папка для txt-файлов: {MISSING_DIR}\n")
        log.write("=" * 50 + "\n")

    success_count = 0
    for doc_file in tqdm(missing_files, desc="Конвертация"):
        rel_path = doc_file.relative_to(PROCESSED_DIR)
        txt_subdir = MISSING_DIR / rel_path.parent
        txt_subdir.mkdir(parents=True, exist_ok=True)

        # Генерируем уникальное имя файла
        base_name = rel_path.stem
        counter = 0
        while True:
            if counter == 0:
                new_name = base_name
            else:
                new_name = f"{base_name}({counter})"

            txt_file = txt_subdir / f"{new_name}.txt"

            # Проверяем существование файла как в MISSING_DIR, так и в TXT_DIR
            main_txt = TXT_DIR / rel_path.parent / f"{new_name}.txt"
            if not txt_file.exists() and not main_txt.exists():
                break
            counter += 1

        if convert_to_txt(doc_file, txt_file):
            success_count += 1
            status = "Успешно"
        else:
            status = "Ошибка"

        # Записываем в лог
        with open(LOG_FILE, 'a', encoding='utf-8') as log:
            log.write(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {status} | {rel_path} | {time.time() - start_time:.2f} сек\n")

    # Итоговая статистика
    with open(LOG_FILE, 'a', encoding='utf-8') as log:
        log.write("=" * 50 + "\n")
        log.write(
            f"ИТОГО: Успешно {success_count}/{len(missing_files)} | Ошибок: {len(missing_files) - success_count}\n")

    print(f"\nГотово. Успешно сконвертировано: {success_count}/{len(missing_files)}")
    print(f"Недостающие txt-файлы сохранены в: {MISSING_DIR}")

# ================== ДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА ==================

def verify_file_counts():
    """Проверяет количество файлов в папках для отладки"""
    doc_files = [f for f in PROCESSED_DIR.rglob('*.*') if f.suffix.lower() in SOURCE_EXTENSIONS]
    txt_files = list(TXT_DIR.rglob('*.txt'))

    # Создаем словарь для подсчета дубликатов
    name_counts = {}
    for f in doc_files:
        base_name = f.stem.lower()
        if base_name not in name_counts:
            name_counts[base_name] = []
        name_counts[base_name].append(str(f))

    # Находим файлы с дубликатами
    duplicates = {name: files for name, files in name_counts.items() if len(files) > 1}

    print("\n=== Проверка количества файлов ===")
    print(f"Всего doc/docx файлов: {len(doc_files)}")
    print(f"Всего txt файлов: {len(txt_files)}")
    print(f"Файлов в PROCESSED_DIR: {len(list(PROCESSED_DIR.rglob('*.*')))}")
    print(f"Файлов в TXT_DIR: {len(list(TXT_DIR.rglob('*.*')))}")
    print(f"Уникальных имен doc/docx файлов: {len(name_counts)}")
    print(f"Количество имен с дубликатами: {len(duplicates)}")

    if duplicates:
        print("\nПримеры дубликатов:")
        for name, files in list(duplicates.items())[:3]:
            print(f"\nИмя файла: {name}")
            for f in files:
                print(f"  - {f}")

# ================== ЗАПУСК СКРИПТА ==================

if __name__ == "__main__":
    print("=== Начало работы скрипта ===")
    print(f"PROCESSED_DIR: {PROCESSED_DIR}")
    print(f"TXT_DIR: {TXT_DIR}")

    # Проверка папок и количества файлов
    verify_file_counts()

    # Основной процесс
    process_missing_files()

    # Дополнительная проверка после выполнения
    verify_file_counts()

    print("\n=== Работа скрипта завершена ===")