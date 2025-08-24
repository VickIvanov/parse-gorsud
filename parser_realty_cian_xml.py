import os, os.path, time
import argparse
import psycopg2
import psycopg2.extensions as exts
import xml.sax
import html
import gzip
from dotenv import load_dotenv
import logging
import sys
from datetime import datetime
from tqdm import tqdm

load_dotenv()

# --- Логирование ---
os.makedirs('logs/cian', exist_ok=True)
log_ts = datetime.now().strftime('%Y%m%d-%H%M%S')
export_log_path = f'logs/cian/export-{log_ts}.log'

# Логгер для файла (только файл)
export_logger = logging.getLogger("export")
export_logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(export_log_path, encoding='utf-8')
fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
export_logger.addHandler(fh)
export_logger.propagate = False  # Не передавать сообщения выше (в консоль)

# Логгер для консоли (только INFO и выше)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def fast_count_offers(filename, pattern=b'<offer'):
    count = 0
    chunk_size = 1024 * 1024 * 8  # 8 МБ
    with open(filename, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            count += chunk.count(pattern)
    return count

def boolex(v):
    return v == 'Да' or v == 'да' or v == 'ДА' or v == '1' or v == 1

def intex(v):
    return int(v) if v else None

# fix floats with ',' instead of '.'
def float_fix(v):
    return float(v.replace(',', '.'))

def notblank(v):
    return v if v else None

class Ignore:
    pass

IgnoreKey = Ignore()

# если не указано - оставляем str
propsTypeMappers = {
    'newobject-id': intex,
    'payed-adv': bool,
    'manually-added': bool,
    'latitude': float_fix,
    'longitude': float_fix,
    'time-on-foot': int,
    'agency-id': intex,
    'value': float_fix,
    'unit': lambda v: str.replace(v, ' ', ''), # 'кв. м' => 'кв.м'
    'description': lambda v: html.unescape(v),
    'rooms': intex,
    # NOTE enum now
    #'bathroom-unit': lambda v: -1 if v == 'совмещенный' else int(v), # <bathroom-unit>совмещенный</bathroom-unit>
    'new-flat': bool,
    'mortgage': boolex,
    'phone': boolex,
    'internet': boolex,
    'room-furniture': boolex,
    'television': boolex,
    'washing-machine': boolex,
    'refrigerator': boolex,
    'floor': int,
    'floors-total': int,
    'built-year': int,
    'lift': boolex,
    'studio': boolex,
    'rubbish-chute': boolex,
    'water-supply': boolex,
    'sewerage-supply': boolex,
    'electricity-supply': boolex,
    'gas-supply': boolex,
    'ceiling-height': float_fix,
    #
    'renovation': notblank,
    # игнорируемые поля
    'district': IgnoreKey,
}

# TODO
pruneLotType = {
    'ижс': 'ИЖС',
    'днп': 'ДНП',
    'усн': 'УСН',
    'лпх': 'ЛПХ',
    'снт': 'СНТ',
}

class CianXmlEventHandler(xml.sax.ContentHandler):

    INSERT_STMT = 'insert into "cian" (%s) values %s'

    def __init__(self, conn, total_offers=None):
        super().__init__()
        self.conn = conn
        self.segments = []
        self.offer = {}
        self.content = None
        self.count = 0
        self.total = 0
        self.pbar = tqdm(total=total_offers, desc="Импорт", unit="шт.", ncols=80)

    def endDocument(self):
        self.conn.commit()
        self.pbar.close()
        logging.info("Обработано всего %d записей, вставлено %d", self.total, self.count)
        export_logger.info("Обработано всего %d записей, вставлено %d", self.total, self.count)
        print("Processed: {} records, Inserted: {}".format(self.total, self.count), flush=True)

    def startElement(self, name, attrs):
        #print("Start element {}: {}".format(name, attrs.items()))
        # пропускаем
        if name == 'realty-feed' or name == 'generation-date':
            return

        # новый оффер
        if name == 'offer':
            self.offer.clear()
            self.offer['id'] = int(attrs['internal-id'])
            self.content = None
            return

        # новый regular offer item, сбрасываем content
        self.segments.append(name)
        self.content = ""

    def characters(self, content):
        #print("char data: {}". format(repr(content)))
        if self.content is not None: # контейнерные элементы не нужны
            self.content += content

    # прямо ща нужны квартиры МО и Мск
    # (self.offer['category'] == 'квартира') и (self.offer['location_country'] == 'Россия') и (self.offer['location_locality_name'] == 'Москва' или self.offer['location_region'] == 'Московская')
    def should_save_offer(self):
        #if self.offer['type'] != 'продажа':
        #   return False

        if self.offer['category'] != 'квартира':
            return False

        if ('location_country' not in self.offer) or (self.offer['location_country'] != 'Россия'):
            return False

        if ('location_locality_name' in self.offer) and (self.offer['location_locality_name'] == 'Москва'):
            return True

        return ('location_region' in self.offer) and (self.offer['location_region'] == 'Московская')

    def endElement(self, name):
        #print("End element: {}".format(name))
        # пропускаем
        if name == 'realty-feed' or name == 'generation-date':
            return

        if name == "offer":
            self.total += 1
            if self.should_save_offer():
                export_logger.debug("Сохраняется оффер: %s", self.offer)
                keys = self.offer.keys()
                vals = [self.offer[key] for key in keys]
                with self.conn.cursor() as cur:
                    cols = [exts.quote_ident(key, cur) for key in keys]
                    try:
                        cur.execute(self.INSERT_STMT, (exts.AsIs(','.join(cols)), tuple(vals)))
                    except:
                        export_logger.error('Ошибка при вставке: %s', self.offer)
                        raise
                self.count += 1
                self.pbar.update(1)
            else:
                export_logger.debug("Оффер не подходит по фильтру: %s", self.offer)
                self.pbar.update(1)
            self.offer.clear()
            return

        # игнорируем контейнерные элементы, их задача - дать нам префикс сегмента, данные не нужны
        if self.content is not None:
            value = self.content.strip()

            # особая обработка для тега image
            if name == 'image':
                # аллоцируем новый список, если еще нет
                if 'image' not in self.offer:
                    self.offer['image'] = []
                self.offer['image'].append(value)
            else:
                if name in propsTypeMappers:
                    fn = propsTypeMappers[name]
                    value = fn(value) if fn is not IgnoreKey else IgnoreKey

                if value is not IgnoreKey:
                    prop = '_'.join(self.segments).replace('-', '_')

                    # особая обработка дублирующегося location.locality-name - подменяется на location.address
                    if (prop == 'location_locality_name') and ('location_locality_name' in self.offer) and (self.offer['location_locality_name']):
                        prop = 'location_address'

                    # regular offer item
                    #print("item: {}: {}".format(prop, value))
                    self.offer[prop] = value

        self.segments.pop() # todo assert with name
        self.content = None # метим parent как контейнерный
def process_cian_xml(args):
    logging.info("Используется база данных: %s", args.database)

    if args.verbose:
        logging.info("Включён подробный режим (verbose)")
    if args.dry_run:
        logging.info("Включён dry-run режим (без обновления БД)")

    if args.mode == "update":
        logging.info("Режим обновления (update) пока не реализован. Скрипт завершает работу.")
        return 0, "Режим обновления (update) пока не реализован\n"

    if not os.path.isfile(args.xml_file):
        logging.error("Исходный XML-файл %s не найден", args.xml_file)
        return 2, f"Source xml file {args.xml_file} is absent"

    try:
        logging.info("Подключение к БД...")
        logging.info(
            "Параметры подключения: host=%s, port=%s, user=%s, dbname=%s",
            args.host, args.port, args.username, args.database
        )
        conn = psycopg2.connect(
            host=args.host, port=args.port, user=args.username,
            password=args.password, dbname=args.database
        )
        conn.autocommit = True
        logging.info("Подключение к БД успешно")

    except (psycopg2.Warning, psycopg2.Error) as e:
        logging.exception("Ошибка подключения к БД")
        return 2, str(e)

    with conn:
        with conn.cursor() as cur:
            # logging.info("Очистка таблицы cian...")
            # cur.execute('TRUNCATE TABLE "cian" RESTART IDENTITY;')
            # logging.info("Таблица очищена")

        # Подсчёт количества офферов для прогресс-бара
        total_offers = fast_count_offers(args.xml_file)

        with open(args.xml_file, 'rb') as fp:
            if args.xml_file.endswith('.gz'):
                logging.info("Файл сжат (gzip), открытие через gzip")
                fp = gzip.open(fp, 'rb')
            logging.info("Запуск SAX-парсера для файла %s", args.xml_file)
            xml.sax.parse(fp, CianXmlEventHandler(conn, total_offers=total_offers))
            logging.info("Завершение SAX-парсера")

    logging.info("Обработка завершена")
    return 0, 'Ok\n'

def build_processor():
    parser = argparse.ArgumentParser()
    parser.add_argument("xml_file", default="data/cian/feed.xml", help="Cian source xml file to parse (may be xml.gz)")
    parser.add_argument("-H", "--host", dest="host", default=os.getenv("PG_HOST", "localhost"),
                        help="Postgres server host (default: %(default)s)")
    parser.add_argument("-P", "--port", dest="port", default=int(os.getenv("PG_PORT", 5432)), type=int,
                        help="Postgres server port (default: %(default)s)")
    parser.add_argument("-u", "--username", dest="username", default=os.getenv("PG_USER", "postgres"),
                        help="postgres username (default: %(default)s)")
    parser.add_argument("-p", "--password", dest="password", default=os.getenv("PG_PASSWORD", "666"),
                        help="postgres password (default: %(default)s)")
    parser.add_argument("-d", "--database", dest="database", default=os.getenv("PG_DATABASE", "realtydata"),
                        help="postgres dest database (default: %(default)s)")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False,
                        help="verbose process output (and store original static files)")
    parser.add_argument("-n", "--dry-run", dest="dry_run", action="store_true", default=False,
                        help="dry run (emulate, not perform upload")
    parser.add_argument("--mode", dest="mode", choices=["parse", "update"], default="update",
                        help="Режим работы: parse - парсить и наполнять базу, update - обновление (по умолчанию, пока не реализовано)")
    return parser

def main():
    parser = build_processor()
    # Если xml_file не передан — добавляем дефолт вручную
    if len(sys.argv) == 1 or sys.argv[1].startswith('-'):
        sys.argv.insert(1, "data/cian/feed.xml")
    args = parser.parse_args()
    parser.exit(*process_cian_xml(args))

if __name__ == "__main__":
    main()