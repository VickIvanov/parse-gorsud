import os
import shutil
import docx2txt
from pathlib import Path
from datetime import datetime
import re
from tqdm import tqdm
import zipfile
import time
import win32com.client
import tempfile



# Настройки
SOURCE_DIR = Path("data/source_files")
TXT_OUTPUT_DIR = Path("data/test_txt")
PROCESSED_DIR = Path("data/processed_files")
ERROR_DIR = Path("data/error_files")
LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / f"convert_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

MAX_RETRIES = 2
MOVE_SUCCESS_FILES = True  # Не перемещать исходные файлы
TEST_MODE = False  # Для тестирования на нескольких файлах
TEST_COUNT = 10


def setup_environment():
    """Создание необходимых папок"""
    try:
        SOURCE_DIR.mkdir(exist_ok=True, parents=True)
        TXT_OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
        PROCESSED_DIR.mkdir(exist_ok=True, parents=True)
        ERROR_DIR.mkdir(exist_ok=True, parents=True)
        LOG_DIR.mkdir(exist_ok=True, parents=True)

        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write("timestamp,operation,source_file,target_file,format,status,error\n")

        return True
    except Exception as e:
        print(f"❌ Ошибка инициализации: {e}")
        return False


def log_action(operation, source_file, target_file="", file_format="", status="SUCCESS", error=""):
    """Логирование действий"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(
                f'"{timestamp}","{operation}","{source_file}","{target_file}","{file_format}","{status}","{error}"\n')
    except Exception as e:
        print(f"⚠️ Ошибка логирования: {e}")


def get_unique_filename(directory, filename):
    """Создает уникальное имя файла"""
    filepath = Path(directory) / filename
    if not filepath.exists():
        return filename

    name, ext = os.path.splitext(filename)
    counter = 1
    while True:
        new_name = f"{name}({counter}){ext}"
        new_filepath = Path(directory) / new_name
        if not new_filepath.exists():
            return new_name
        counter += 1


def detect_file_format(file_path):
    """Определяет формат файла ТОЛЬКО по сигнатуре, без учёта расширения"""
    try:
        with open(str(file_path), 'rb') as f:
            header = f.read(8)

            # DOC сигнатура (D0CF11E0)
            if header.startswith(b'\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1'):
                return "doc"

            # DOCX сигнатура (PK...) + проверка внутренней структуры
            if header.startswith(b'PK\x03\x04'):
                try:
                    with zipfile.ZipFile(str(file_path)) as zip_ref:
                        files = zip_ref.namelist()
                        # Проверяем, содержит ли документ правильную структуру DOCX
                        if any('word/document.xml' in name for name in files):
                            return "docx"
                except:
                    pass

        # Не смогли определить формат
        return "unknown"
    except Exception as e:
        print(f"Ошибка при определении формата файла {file_path}: {str(e)}")
        return "unknown"

def convert_docx_to_txt(docx_path):
    """Конвертирует DOCX через docx2txt"""
    try:
        text = docx2txt.process(docx_path)
        if not text or not isinstance(text, str):
            raise Exception("Некорректный результат конвертации")
        return text
    except Exception as e:
        raise Exception(f"docx2txt: {str(e)}")


def extract_text_from_xml(xml_content):
    """Извлекает текст из XML документа Word"""
    result = []
    # Находим все текстовые элементы
    text_elements = re.findall(r'<w:t[^>]*>(.*?)</w:t>', xml_content, re.DOTALL)
    for text in text_elements:
        result.append(text)
    return " ".join(result)


def convert_docx_manually(docx_path):
    """Конвертирует DOCX вручную через ZIP-архив с улучшенной защитой от ошибок"""
    try:
        text_parts = []

        # Пробуем открыть файл как ZIP архив
        try:
            with zipfile.ZipFile(docx_path, strictZIP=False) as zip_ref:
                # Пытаемся извлечь содержимое разными способами
                xml_files_to_check = [
                    'word/document.xml',
                    'word/document2.xml',
                    'word/header1.xml',
                    'word/footer1.xml'
                ]

                for xml_file in xml_files_to_check:
                    try:
                        with zip_ref.open(xml_file) as xml_file:
                            content = xml_file.read().decode('utf-8', errors='replace')
                            if '<w:t' in content:
                                text_parts.append(extract_text_from_xml(content))
                    except:
                        continue

                # Если основной документ не найден, проверяем все XML файлы
                if not text_parts:
                    for file_name in zip_ref.namelist():
                        if file_name.endswith('.xml'):
                            try:
                                with zip_ref.open(file_name) as xml_file:
                                    content = xml_file.read().decode('utf-8', errors='replace')
                                    if '<w:t' in content:
                                        text_parts.append(extract_text_from_xml(content))
                            except:
                                continue
        except zipfile.BadZipFile:
            raise Exception("Файл повреждён и не может быть открыт как ZIP архив")

        if not text_parts:
            raise Exception("Не удалось найти текстовое содержимое в документе")

        return "\n".join(text_parts)
    except Exception as e:
        raise Exception(f"Ручная конвертация DOCX: {str(e)}")

def convert_doc_to_txt_com(doc_path):
    """Извлекает текст из DOC через MS Word (COM)"""
    word = None
    temp_file = None
    try:
        # Создаём временный файл с расширением .doc
        with tempfile.NamedTemporaryFile(suffix=".doc", delete=False) as tf:
            temp_file = tf.name
            with open(doc_path, 'rb') as src_file:
                tf.write(src_file.read())

        word = win32com.client.Dispatch("Word.Application")
        word.visible = False
        doc = word.Documents.Open(
            os.path.abspath(temp_file),
            ReadOnly=True,
            ConfirmConversions=False,
            Format=0,
            NoEncodingDialog=True,
            AddToRecentFiles=False,
            Visible=False,
            OpenAndRepair=True
        )
        text = doc.Content.Text
        doc.Close(False)
        if not text or not isinstance(text, str):
            raise Exception("Некорректный результат конвертации")
        return text
    except Exception as e:
        raise Exception(f"COM: {str(e)}")
    finally:
        if word:
            try:
                word.Quit()
            except:
                pass
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except:
                pass

def convert_docx_to_txt_com(docx_path):
    """Извлекает текст из повреждённого DOCX через MS Word (COM)"""
    word = None
    temp_file = None
    try:
        # Создаём временный файл с расширением .docx
        with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tf:
            temp_file = tf.name
            with open(docx_path, 'rb') as src_file:
                tf.write(src_file.read())

        word = win32com.client.Dispatch("Word.Application")
        word.visible = False
        doc = word.Documents.Open(
            os.path.abspath(temp_file),
            ReadOnly=True,
            ConfirmConversions=False,
            Format=0,
            NoEncodingDialog=True,
            AddToRecentFiles=False,
            Visible=False,
            OpenAndRepair=True
        )
        text = doc.Content.Text
        doc.Close(False)
        if not text or not isinstance(text, str):
            raise Exception("Некорректный результат конвертации")
        return text
    except Exception as e:
        raise Exception(f"COM_DOCX: {str(e)}")
    finally:
        if word:
            try:
                word.Quit()
            except:
                pass
        if temp_file and os.path.exists(temp_file):
            try:
                os.unlink(temp_file)
            except:
                pass

def convert_to_txt(file_path, txt_path):
    """Конвертирует файл в текст используя подходящий метод"""
    file_format = detect_file_format(file_path)
    errors = []

    # 1. Ручная конвертация DOCX через ZIP-архив
    if file_format == "docx":
        try:
            text = convert_docx_manually(file_path)
            if text and isinstance(text, str) and len(text) > 0:
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(text)
                return True, "", file_format
        except Exception as e:
            errors.append(f"Ручная конвертация: {str(e)}")

    # 2. docx2txt
    if file_format == "docx":
        try:
            text = convert_docx_to_txt(file_path)
            if text and isinstance(text, str) and len(text) > 0:
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(text)
                return True, "", file_format
        except Exception as e:
            errors.append(f"docx2txt: {str(e)}")

    # 3. COM для DOC
    if file_format == "doc":
        try:
            text = convert_doc_to_txt_com(file_path)
            if text and isinstance(text, str) and len(text) > 0:
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(text)
                return True, "", file_format
        except Exception as e:
            errors.append(f"COM: {str(e)}")

    # 4. COM для повреждённых DOCX (добавлено!)
    if file_format == "docx":
        try:
            text = convert_docx_to_txt_com(file_path)
            if text and isinstance(text, str) and len(text) > 0:
                with open(txt_path, 'w', encoding='utf-8') as f:
                    f.write(text)
                return True, "", file_format
        except Exception as e:
            errors.append(f"COM_DOCX: {str(e)}")

    # 5. Неизвестный формат — пробуем все методы
    if file_format == "unknown":
        for method_name, method_func in [
            ("Ручная конвертация DOCX", convert_docx_manually),
            ("docx2txt", convert_docx_to_txt),
            ("COM Word", convert_doc_to_txt_com),
            ("COM_DOCX", convert_docx_to_txt_com)
        ]:
            try:
                text = method_func(file_path)
                if text and isinstance(text, str) and len(text) > 0:
                    with open(txt_path, 'w', encoding='utf-8') as f:
                        f.write(text)
                    return True, "", f"unknown (сработал {method_name})"
            except Exception as e:
                errors.append(f"{method_name}: {str(e)}")

    return False, f"Не удалось конвертировать: {'; '.join(errors)}", file_format

def process_file(word_file):
    """Обработка одного файла"""
    file_basename = word_file.name
    txt_basename = os.path.splitext(file_basename)[0] + '.txt'
    unique_txt_name = get_unique_filename(TXT_OUTPUT_DIR, txt_basename)
    txt_path = TXT_OUTPUT_DIR / unique_txt_name

    # Пробуем конвертировать с несколькими попытками
    success = False
    error_msg = ""
    file_format = "unknown"

    for attempt in range(MAX_RETRIES):
        success, error_msg, file_format = convert_to_txt(word_file, txt_path)
        if success:
            break
        time.sleep(0.5)  # Пауза между попытками

    if success:
        # Перемещаем файл при успешной конвертации (всегда, т.к. MOVE_SUCCESS_FILES = True)
        unique_processed_name = get_unique_filename(PROCESSED_DIR, file_basename)
        target_path = PROCESSED_DIR / unique_processed_name
        try:
            shutil.move(word_file, target_path)
            log_action("CONVERT+MOVE", file_basename, unique_txt_name, file_format, "SUCCESS")
        except Exception as e:
            log_action("CONVERT+MOVE_ERROR", file_basename, unique_txt_name, file_format, "ERROR", str(e))
        return True
    else:
        # Удаляем файл с ошибкой если создан
        if os.path.exists(txt_path):
            try:
                os.remove(txt_path)
            except:
                pass

        # Логируем ошибку, но не выводим в консоль
        log_action("CONVERT", file_basename, unique_txt_name, file_format, "ERROR", error_msg)
        return False


def process_files():
    """Обрабатывает файлы по одному"""
    word_files = []
    for pattern in ["*.doc", "*.docx"]:
        word_files.extend(list(SOURCE_DIR.glob(pattern)))

    if not word_files:
        print("📂 Нет файлов для обработки в исходной папке.")
        return 0, 0

    # Сортировка для предсказуемой обработки
    word_files.sort()

    # Ограничение для тестового режима
    file_count = len(word_files)
    if TEST_MODE and TEST_COUNT < file_count:
        word_files = word_files[:TEST_COUNT]
        print(f"⚠️ ТЕСТОВЫЙ РЕЖИМ: обработка только первых {TEST_COUNT} файлов из {file_count}")
    else:
        print(f"📄 Найдено {len(word_files)} файлов для обработки.")

    successful = 0
    failed = 0

    # Обработка с улучшенным прогресс-баром
    progress_bar = tqdm(total=len(word_files), desc="Конвертация", unit="файл")

    for word_file in word_files:
        if process_file(word_file):
            successful += 1
        else:
            failed += 1

        # Обновляем описание прогресс-бара с текущими показателями
        progress_bar.set_postfix(успешно=successful, ошибки=failed)
        progress_bar.update(1)

    progress_bar.close()
    return successful, failed

def main():
    """Основная функция программы"""
    print(f"🔄 Начало обработки файлов: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Исходная папка: {SOURCE_DIR}")
    print(f"📁 Папка для TXT: {TXT_OUTPUT_DIR}")

    if MOVE_SUCCESS_FILES:
        print(f"📁 Папка для обработанных файлов: {PROCESSED_DIR}")

    # Подготавливаем окружение
    if not setup_environment():
        return

    # Обработка файлов
    start_time = datetime.now()
    successful, failed = process_files()
    end_time = datetime.now()
    execution_time = (end_time - start_time).total_seconds()

    print(f"\n📊 Итоги: обработано {successful + failed} файлов")
    print(f"✅ Успешно: {successful}")
    print(f"❌ С ошибками: {failed}")
    print(f"⏱️ Время выполнения: {execution_time:.2f} секунд")
    print(f"\n📝 Детали в лог-файле: {LOG_FILE}")
    print(f"🏁 Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()