import os
import logging
from dotenv import load_dotenv
import requests
import gzip
import xml.etree.ElementTree as ET
from tqdm import tqdm
from datetime import datetime
from email.utils import parsedate_to_datetime
import pytz
import struct
import zlib

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

load_dotenv()

DATA_DIR = "data/cian"
os.makedirs(DATA_DIR, exist_ok=True)

URL = os.getenv("CIAN_URL")
AUTH = (os.getenv("CIAN_LOGIN"), os.getenv("CIAN_PASSWORD"))
ARCHIVE_PATH = os.path.join(DATA_DIR, "feed.xml.gz")
XML_PATH = os.path.join(DATA_DIR, "feed.xml")
TRIMMED_XML_PATH = os.path.join(DATA_DIR, "feed_trimmed.xml")

for var in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    os.environ.pop(var, None)

def file_crc32(path):
    crc = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            crc = zlib.crc32(chunk, crc)
    return crc & 0xFFFFFFFF

def get_gzip_info(path):
    with open(path, "rb") as f:
        f.seek(-8, os.SEEK_END)
        crc32 = struct.unpack("<I", f.read(4))[0]
        isize = struct.unpack("<I", f.read(4))[0]
        f.seek(0)
        f.read(10)
        flg = ord(f.read(1))
        orig_name = None
        if flg & 0x08:
            orig_name = b""
            while True:
                c = f.read(1)
                if c == b"\x00" or not c:
                    break
                orig_name += c
            orig_name = orig_name.decode("latin-1")
        return orig_name, isize, crc32

def get_remote_file_info():
    r = requests.head(URL, auth=AUTH, timeout=30)
    r.raise_for_status()
    size = int(r.headers.get("Content-Length", 0))
    last_modified = r.headers.get("Last-Modified")
    dt = parsedate_to_datetime(last_modified) if last_modified else None
    return size, dt

def get_local_file_info(path):
    if not os.path.exists(path):
        return None, None
    size = os.path.getsize(path)
    mtime = datetime.utcfromtimestamp(os.path.getmtime(path)).replace(tzinfo=pytz.UTC)
    return size, mtime

try:
    logging.info("Проверяем необходимость скачивания файла: %s", URL)
    remote_size, remote_dt = get_remote_file_info()
    local_size, local_dt = get_local_file_info(ARCHIVE_PATH)
    need_download = True
    if local_size == remote_size and local_dt and remote_dt:
        # Округляем до секунд и сравниваем
        local_ts = int(local_dt.timestamp())
        remote_ts = int(remote_dt.timestamp())
        if abs(local_ts - remote_ts) < 2:
            logging.info("Локальный файл актуален, скачивание не требуется.")
            need_download = False
    if need_download:
        logging.info("Начинаем скачивание файла: %s", URL)
        with requests.get(URL, auth=AUTH, stream=True, timeout=30) as r:
            r.raise_for_status()
            total = int(r.headers.get('content-length', 0))
            with open(ARCHIVE_PATH, "wb") as f, tqdm(
                total=total,
                unit='B',
                unit_scale=True,
                desc="Скачивание",
                ncols=80,
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} {unit}"
            ) as pbar:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
        if remote_dt:
            mtime = remote_dt.timestamp()
            os.utime(ARCHIVE_PATH, (mtime, mtime))
        logging.info("Файл успешно скачан: %s", ARCHIVE_PATH)
except Exception as e:
    logging.error("Ошибка при скачивании файла: %s", e)
    raise
'''
try:
    logging.info("Проверяем необходимость распаковки архива: %s", ARCHIVE_PATH)
    need_unpack = True
    if os.path.exists(XML_PATH):
        orig_name, expected_size, expected_crc = get_gzip_info(ARCHIVE_PATH)
        actual_size = os.path.getsize(XML_PATH)
        actual_crc = file_crc32(XML_PATH)
        # Допускаем разницу в размере и сравниваем CRC32
        if abs(actual_size - expected_size) <= 2 and actual_crc == expected_crc:
            logging.info("Распакованный файл уже существует и совпадает по CRC32, распаковка не требуется.")
            need_unpack = False
    if need_unpack:
        file_size = os.path.getsize(ARCHIVE_PATH)
        with gzip.open(ARCHIVE_PATH, "rb") as gz, open(XML_PATH, "wb") as out, tqdm(
            total=file_size,
            unit='B',
            unit_scale=True,
            desc="Распаковка",
            ncols=80,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} {unit}"
        ) as pbar:
            while True:
                chunk = gz.read(8192)
                if not chunk:
                    break
                out.write(chunk)
                pbar.update(len(chunk))
        logging.info("Архив успешно распакован: %s", XML_PATH)
except Exception as e:
    logging.error("Ошибка при распаковке: %s", e)
    raise
'''
from lxml import etree as LET

logging.info("Потоковый парсинг XML и обрезка до 100 элементов <offer> с помощью lxml")
context = LET.iterparse(XML_PATH, events=("end",), tag="offer")
root = None
offers = []
total = 100

for i, (event, elem) in enumerate(context):
    if root is None:
        root = elem.getroottree().getroot()
        trimmed_root = LET.Element(root.tag, root.attrib)
        offers_container = LET.Element("offers")
        trimmed_root.append(offers_container)
    offers_container.append(elem)
    if i + 1 >= total:
        break
    # Удаляем элемент из родителя для освобождения памяти
    parent = elem.getparent()
    if parent is not None:
        parent.remove(elem)

trimmed_tree = LET.ElementTree(trimmed_root)
trimmed_tree.write(TRIMMED_XML_PATH, encoding="utf-8", xml_declaration=True)
logging.info("Обрезанный XML сохранён: %s", TRIMMED_XML_PATH)