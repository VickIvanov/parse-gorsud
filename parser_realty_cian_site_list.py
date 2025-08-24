#!/usr/bin/python3.8

# pip3.8 install psycopg2
# pip3.8 install random_user_agent


import os
from dotenv import load_dotenv
import time
import argparse

load_dotenv()
import psycopg2.extensions as exts
import cianparser, cianparser.parser, cianparser.helpers

import transliterate
from bs4 import BeautifulSoup
import urllib.request
import urllib.error
import random
import socket
import math
import datetime

import random_user_agent.params
import random_user_agent.user_agent

# Массив newobject_id, по которому будем проходить циклом
NEWOBJECT_IDS = [4115886]  # пример, подставьте нужные значения

SQL_find_objects = """
WITH filtered_data AS (
    SELECT
        *,
        CASE WHEN rooms IS NULL THEN 0 ELSE rooms END AS rooms_normalized,
        price_value / area_value AS price_sqm
    FROM
        cian
    WHERE
        -- Основные критерии
        (type = 'продажа'
        AND sales_agent_organization <> 'А101'
        AND (rooms IS NULL OR rooms = 1)
        AND (renovation = 'без_ремонта' OR renovation IS NULL)
        AND price_value > 0
        AND area_value > 20)
        --OR id = 320010896  -- временно оставим для теста
),
stats_calculation AS (
    SELECT
        rooms_normalized,
        AVG(price_sqm) AS mean_price_sqm,
        STDDEV(price_sqm) AS stddev_price_sqm,
        AVG(price_sqm) - 2 * STDDEV(price_sqm) AS lower_bound,
        AVG(price_sqm) + 2 * STDDEV(price_sqm) AS upper_bound
    FROM
        filtered_data
    GROUP BY
        rooms_normalized
),
normal_objects AS (
    SELECT
        f.*,
        s.lower_bound,
        s.upper_bound
    FROM
        filtered_data f
    JOIN
        stats_calculation s ON f.rooms_normalized = s.rooms_normalized
    WHERE
        f.price_sqm BETWEEN s.lower_bound AND s.upper_bound
),
median_calculation AS (
    SELECT
        rooms_normalized,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_sqm) AS median_price_sqm,
        COUNT(*) AS objects_count
    FROM
        normal_objects
    GROUP BY
        rooms_normalized
)
SELECT
    f.id,
    f.url,
    f.price_value,
    f.area_value,
    f.price_sqm,
    f.rooms,
    f.renovation,
    f.sales_agent_organization,
    f.type,
    f.rooms_normalized,
    s.mean_price_sqm,
    s.stddev_price_sqm,
    s.lower_bound,
    s.upper_bound,
    m.median_price_sqm,
    m.objects_count
FROM
    filtered_data f
JOIN
    stats_calculation s ON f.rooms_normalized = s.rooms_normalized
JOIN
    median_calculation m ON f.rooms_normalized = m.rooms_normalized
WHERE
    (1 - f.price_sqm / m.median_price_sqm) * 100 >= 5.0
    OR f.price_sqm < s.lower_bound
ORDER BY
    f.id;
"""

def process_cian_site_list(args):
    import requests

    print("Use database {0}".format(args.database))

    if args.verbose:
        print("Verbose mode (and store original files)")

    if args.dry_run:
        print("Dry-run mode (don't update database records, only static files)")

    try:
        conn = psycopg2.connect(
            host=os.getenv('PG_HOST', args.host),
            port=int(os.getenv('PG_PORT', args.port)),
            user=os.getenv('PG_USER', args.username),
            password=os.getenv('PG_PASSWORD', args.password),
            dbname=os.getenv('PG_DATABASE', args.database)
        )
    except (psycopg2.Warning, psycopg2.Error) as e:
        err = e
        return 2, str(err)

    os.makedirs('logs/cian/html', exist_ok=True)

    for newobject_id in NEWOBJECT_IDS:
        print(f"Обработка newobject_id={newobject_id}")
        with conn:
            with conn.cursor() as cur:
                cur.execute(SQL_find_objects)
                rows = cur.fetchall()
                urls = [row[1] for row in rows]
                # Сохраняем лог с результатами SQL
                with open(f'logs/cian/sql_result_{newobject_id}.log', 'w', encoding='utf-8') as f:
                    for row in rows:
                        f.write(f"{row}\n")
                print(f"Получено {len(urls)} url")

                for idx, url in enumerate(urls):
                    try:
                        proxy = os.getenv('PROXY_URL')
                        proxies = {"http": proxy, "https": proxy} if proxy else None
                        headers = {'User-Agent': 'Mozilla/5.0'}
                        resp = requests.get(url, headers=headers, proxies=proxies, timeout=15)
                        if "Captcha" in resp.text:
                            print(f"Капча на {url}, требуется обработка!")
                            # Здесь можно добавить обработку капчи
                        else:
                            with open(f'logs/cian/html/page_{newobject_id}_{idx}.html', 'w', encoding='utf-8') as html_file:
                                html_file.write(resp.text)
                            print(f"Сохранено: page_{newobject_id}_{idx}.html")
                    except Exception as e:
                        print(f"Ошибка при скачивании {url}: {e}")

    return 0, 'Ok\n'

def is_available_proxy(url, pip, ua='Mozilla/5.0'):
	try:
		proxy_handler = urllib.request.ProxyHandler({'http': pip, 'https': pip})
		opener = urllib.request.build_opener(proxy_handler)
		opener.addheaders = [('User-agent', ua)]
		urllib.request.install_opener(opener)
		req = urllib.request.Request(url)
		html = urllib.request.urlopen(req)

		try:
			soup = BeautifulSoup(html, 'lxml')
		except:
			soup = BeautifulSoup(html, 'html.parser')

		#print(soup.text)

		if soup.text.find("Captcha") > 0:
			return False, True

		return True, False
	except urllib.error.HTTPError as e:
		print('Error code: ', e.code)
		return not e.code, False
	except Exception as detail:
		print("Error:", detail)
		return False, False

def define_location_address(block, city):

	elements = block.select("div[data-name='LinkArea']")[0]. \
		select("div[data-name='GeneralInfoSectionRowComponent']")

	for index, element in enumerate(elements):

		for index, div in enumerate(element.findAll('div', recursive=False)):

			if ("р-н" in div.text) and (city in div.text) and len(div.text) < 250:
				address_elements = div.text.split(",")
				if len(address_elements) < 2:
					continue

				return div.text

	return ""

def define_photos_urls(block):

	elements =  block.select("div[data-name='Gallery']")[0].select('ul > li')

	urls = []

	for index, el in enumerate(elements):

		url = el.select('img')[0].get('src')

		if url and len(url) > 0:
			urls.append(url)

	return urls

# для опции newobject приходится субклассировать класс парсера
class ParserOffersEx(cianparser.parser.ParserOffers):

	NEWOBJECT = '&newobject%5B{0}%5D={1}'
	FROM_DEVELOPER = '&from_developer={}'

	def __init__(self, deal_type: str, accommodation_type: str, city_name: str, rooms,
				 start_page: int, end_page: int, is_saving_csv=False, is_latin=False, is_express_mode=False,
				 additional_settings=None, proxies=None):

		super().__init__(deal_type=deal_type, accommodation_type=accommodation_type, city_name=city_name,
						 location_id="", rooms=rooms, start_page=start_page, end_page=end_page,
						 is_saving_csv=is_saving_csv, is_latin=is_latin, is_express_mode=is_express_mode,
						 additional_settings=additional_settings, proxies=proxies)

		# https://pypi.org/project/random-user-agent/
		software_names = [random_user_agent.params.SoftwareName.CHROME.value, random_user_agent.params.SoftwareName.FIREFOX.value,
						  random_user_agent.params.SoftwareName.ANDROID.value, random_user_agent.params.SoftwareName.OPERA.value]

		operating_systems = [random_user_agent.params.OperatingSystem.WINDOWS.value, random_user_agent.params.OperatingSystem.LINUX.value]

		self.user_agent_rotator = random_user_agent.user_agent.UserAgent(software_names=software_names, operating_systems=operating_systems, limit=100)
		self.ua = None

	def build_url(self):

		url = super().build_url()

		# TODO переделать 'newobject' из скаляра в list
		if ('newobject' in self.additional_settings) and self.additional_settings['newobject'] and self.additional_settings['newobject'] > 0:
			url += self.NEWOBJECT.format(0, self.additional_settings['newobject'])

		if ('from_developer' in self.additional_settings) and self.additional_settings['from_developer'] is not None:
			url += self.FROM_DEVELOPER.format(self.additional_settings['from_developer'])

		return url

	def load_page(self, number_page=1):
		self.url = self.build_url().format(number_page, self.location_id)

		socket.setdefaulttimeout(10)
		was_proxy = self.proxy_pool is not None
		set_proxy = False
		self.url = self.build_url().format(number_page, self.location_id)

		if was_proxy:
			print("The process of checking the proxies... Search an available one among them...")

		ind = 0
		while self.proxy_pool is not None and set_proxy is False:

			self.ua = self.user_agent_rotator.get_random_user_agent()

			ind += 1
			proxy = random.choice(self.proxy_pool)

			# self.url | 'https://httpbin.org/get'
			available, is_captcha = is_available_proxy(self.url, proxy, self.ua)
			if not available or is_captcha:
				if is_captcha:
					print(f" {ind} | proxy {proxy}: there is captcha.. trying another")
				else:
					print(f" {ind} | proxy {proxy}: unavailable.. trying another..")

				self.proxy_pool.remove(proxy)
				if len(self.proxy_pool) == 0:
					self.proxy_pool = None
			else:
				print(f" {ind} | proxy {proxy}: available.. stop searching")
				self.session.proxies = {"http": proxy, "https": proxy}
				self.session.headers.update({'User-Agent': self.ua})
				set_proxy = True

		if was_proxy and set_proxy is False:
			return None

		# url=self.url
		res = self.session.get(url=self.url)
		res.raise_for_status()

		return res.text

	def parse_page(self, html: str, number_page: int, count_of_pages: int, attempt_number: int):

		try:
			soup = BeautifulSoup(html, 'lxml')
		except:
			soup = BeautifulSoup(html, 'html.parser')

		if number_page == self.start_page and attempt_number == 0:
			print(f"The page from which the collection of information begins: \n {self.url}")

		if soup.text.find("Captcha") > 0:
			print(f"\r{number_page} page: there is CAPTCHA... failed to parse page...")

			if self.proxy_pool is not None:
				proxy = random.choice(self.proxy_pool)
				print(f"\r{number_page} page: new attempt with proxy {proxy}...")
				self.session.proxies = {"http": proxy}
				return False, attempt_number + 1, False

			return False, attempt_number + 1, True

		header = soup.select("div[data-name='HeaderDefault']")
		if len(header) == 0:
			header = soup.select("div[data-name='NewbuildingHeaderWrapper']")
			if len(header) == 0:
				return False, attempt_number + 1, False

		offers = soup.select("article[data-name='CardComponent']")
		page_number_html = soup.select("button[data-name='PaginationButton']")
		if len(page_number_html) == 0:
			return False, attempt_number + 1, True

		if page_number_html[0].text == "Назад" and (number_page != 1 and number_page != 0):
			print(f"\n\r {number_page - self.start_page + 1} | {number_page} page: cian is redirecting from "
				  f"page {number_page} to page 1... there is maximum value of page, you should try to decrease number "
				  f"of page... ending...")

			return False, 0, True

		if number_page == self.start_page and attempt_number == 0:
			print(f"Collecting information from pages with list of announcements", end="")

		print("")
		print(f"\r {number_page} page: {len(offers)} offers", end="\r", flush=True)

		for ind, block in enumerate(offers):
			self.parse_block(block=block)

			if not self.is_express_mode:
				time.sleep(4)

			total_planed_announcements = len(offers) * count_of_pages

			print(f"\r {number_page - self.start_page + 1} | {number_page} page with list: [" + "=>" * (
					ind + 1) + "  " * (
						  len(offers) - ind - 1) + "]" + " " + str(math.ceil((ind + 1) * 100 / len(offers))) + "%" +
				  f" | Count of all parsed: {self.parsed_announcements_count}."
				  f" Progress ratio: {math.ceil(self.parsed_announcements_count * 100 / total_planed_announcements)} %."
				  f" Average price: {'{:,}'.format(int(self.average_price)).replace(',', ' ')} rub", end="\r",
				  flush=True)

		time.sleep(2)

		return True, 0, False

	def parse_block(self, block):

		link = block.select("div[data-name='LinkArea']")[0].select("a")[0].get('href')
		adv_id = cianparser.helpers.define_id_url(link)

		if adv_id in self.result_parsed:
			return

		common_data = {
			"id": adv_id,
			"link": link,
			"city": self.city_name,
			"deal_type": self.deal_type,
			"accommodation_type": self.accommodation_type,
			"title": block.select("span[data-mark='OfferTitle']")[0].select('span')[0].text,
			"description": block.select("div[data-name='Description']")[0].select("p")[0].text,
			'photos': define_photos_urls(block),
		}

		author_data = cianparser.helpers.define_author(block=block)
		location_data = cianparser.helpers.define_location_data(block=block, is_sale=self.is_sale())
		location_data['address'] = define_location_address(block=block, city=self.city_name)
		price_data = cianparser.helpers.define_price_data(block=block)
		specification_data = cianparser.helpers.define_specification_data(block=block)

		if (self.additional_settings is not None and "is_by_homeowner" in self.additional_settings.keys() and
			self.additional_settings["is_by_homeowner"]) and (
				author_data["author_type"] != "unknown" and author_data["author_type"] != "homeowner"):
			return

		if self.is_latin:
			try:
				location_data["district"] = transliterate.translit(location_data["district"], reversed=True)
				location_data["street"] = transliterate.translit(location_data["street"], reversed=True)
			except:
				pass

			try:
				author_data["author"] = transliterate.translit(author_data["author"], reversed=True)
			except:
				pass

			try:
				common_data["city"] = transliterate.translit(common_data["city"], reversed=True)
			except:
				pass

			try:
				location_data["underground"] = transliterate.translit(location_data["underground"], reversed=True)
			except:
				pass

			try:
				location_data["residential_complex"] = transliterate.translit(location_data["residential_complex"],
																			  reversed=True)
			except:
				pass

		page_data = dict()
		if not self.is_express_mode:
			res = self.session.get(url=common_data["link"])
			res.raise_for_status()
			html_offer_page = res.text

			page_data = cianparser.helpers.parse_page_offer(html_offer=html_offer_page)
			if page_data["year_of_construction"] == -1 and page_data["kitchen_meters"] == -1 and page_data[
				"floors_count"] == -1:
				page_data = cianparser.helpers.parse_page_offer_json(html_offer=html_offer_page)

		specification_data["price_per_m2"] = float(0)
		if "price" in price_data:
			self.average_price = (self.average_price * self.parsed_announcements_count + price_data["price"]) / (
					self.parsed_announcements_count + 1)
			price_data["price_per_m2"] = int(float(price_data["price"]) / specification_data["total_meters"])
		elif "price_per_month" in price_data:
			self.average_price = (self.average_price * self.parsed_announcements_count + price_data[
				"price_per_month"]) / (self.parsed_announcements_count + 1)
			price_data["price_per_m2"] = int(float(price_data["price_per_month"]) / specification_data["total_meters"])

		self.parsed_announcements_count += 1

		self.result_parsed.add(adv_id)
		self.result.append(
			cianparser.helpers.union_dicts(author_data, common_data, specification_data, price_data, page_data, location_data))

		if self.is_saving_csv:
			self.save_results()


#
class CianSiteScrapper:

	# image = EXCLUDED.image
	UPSERT_STMT = """INSERT INTO "cian" (%s)
	VALUES %s
	ON CONFLICT (id) DO UPDATE
		SET (last_update_date, price_value, price_currency, rooms, description) =
			(EXCLUDED.last_update_date, EXCLUDED.price_value, EXCLUDED.price_currency, EXCLUDED.rooms, EXCLUDED.description);"""

	def __init__(self, conn, vebose = False, dry_run = False):
		#print("CianXmlEventHandler init")
		self.conn = conn
		self.verbose = vebose
		self.dry_run = dry_run

	def store_advs(self, advs, newobject_id):

		now = datetime.datetime.now()

		for adv in advs:

			address = [adv.get('street'), adv.get('house_number')]
			address = [v for v in address if v is not None]
			address = " ".join(address)

			if not address:
				address = None

			data = {
				'id': int(adv['id']),
				'type': 'продажа',
				'property_type': 'жилая',
				'category': 'квартира',
				'url': adv['link'],
				'newobject_id': int(newobject_id),
				'creation_date': now,
				'last_update_date': now,
				'location_country': 'Россия',
				'location_region': 'Московская',
				'location_locality_name': adv['city'],
				'location_sub_locality_name': adv.get('district'),
				'location_address': address, # adv.get('address'),
				'location_metro_name': adv.get('underground'),
				'price_value': int(adv.get('price', 0)),
				'price_currency': 'RUB',
				'rooms': adv.get('rooms_count'),
				'floor': adv.get('floor'),
				'floors_total': adv.get('floors_count'),
				'description': adv.get('description'),
			}

			author_type = adv.get('author_type')

			if author_type == 'real_estate_agent':
				data['sales_agent_category'] = 'агентство'
				data['sales_agent_organization'] = adv.get('author')
			elif author_type == 'realtor':
				data['sales_agent_name'] = adv.get('author')
			else:
				data['sales_agent_name'] = adv.get('author')

			photos = adv.get('photos')
			if photos and len(photos) > 0:
				data['image'] = photos

			area = adv.get('total_meters')
			if area and area > 0:
				data['area_value'] = area
				data['area_unit'] = 'кв.м'

			# TODO? удаляем None

			keys = data.keys()
			vals = [data[key] for key in keys]

			with self.conn.cursor() as cur:
				cols = [exts.quote_ident(key, cur) for key in keys]

				try:
					#print(cur.mogrify(self.UPSERT_STMT, (exts.AsIs(','.join(cols)), tuple(vals))).decode('utf-8'))
					cur.execute(self.UPSERT_STMT, (exts.AsIs(','.join(cols)), tuple(vals)))
					#print('adv: {}'.format(adv), flush=True)
					print('adv id inserted:', int(adv['id']))
				except:
					print('adv: {}'.format(adv), flush=True)
					raise


	def run(self, newobject_id):

		parser = ParserOffersEx(
			deal_type="sale",
			accommodation_type="flat",
			city_name = "Москва",
			rooms="all",
			start_page=1,
			end_page=100,
			is_saving_csv=False,
			is_express_mode=True,
			additional_settings={
				'newobject': newobject_id,
				'from_developer': 0,
			},
			proxies=[
				os.getenv('PROXY_URL')
			]
		)

		parser.run()

		advs = parser.get_results()

		self.store_advs(advs, newobject_id=newobject_id)

def process_cian_site_list(args):
    import requests

	print("Use database {0}".format(args.database))

	if args.verbose:
		print("Verbose mode (and store original files)")

	if args.dry_run:
		print("Dry-run mode (don't update database records, only static files)")

	try:
		conn = psycopg2.connect(
			host=os.getenv('PG_HOST', args.host),
			port=int(os.getenv('PG_PORT', args.port)),
			user=os.getenv('PG_USER', args.username),
			password=os.getenv('PG_PASSWORD', args.password),
			dbname=os.getenv('PG_DATABASE', args.database)
		)
	except (psycopg2.Warning, psycopg2.Error) as e:
		err = e
		return 2, str(err)

    os.makedirs('logs/cian/html', exist_ok=True)

    for newobject_id in NEWOBJECT_IDS:
        print(f"Обработка newobject_id={newobject_id}")
	with conn:
            with conn.cursor() as cur:
                cur.execute(SQL_find_objects)
                rows = cur.fetchall()
                urls = [row[1] for row in rows]
                # Сохраняем лог с результатами SQL
                with open(f'logs/cian/sql_result_{newobject_id}.log', 'w', encoding='utf-8') as f:
                    for row in rows:
                        f.write(f"{row}\n")
                print(f"Получено {len(urls)} url")

                for idx, url in enumerate(urls):
                    try:
                        proxy = os.getenv('PROXY_URL')
                        proxies = {"http": proxy, "https": proxy} if proxy else None
                        headers = {'User-Agent': 'Mozilla/5.0'}
                        resp = requests.get(url, headers=headers, proxies=proxies, timeout=15)
                        if "Captcha" in resp.text:
                            print(f"Капча на {url}, требуется обработка!")
                            # Здесь можно добавить обработку капчи
                        else:
                            with open(f'logs/cian/html/page_{newobject_id}_{idx}.html', 'w', encoding='utf-8') as html_file:
                                html_file.write(resp.text)
                            print(f"Сохранено: page_{newobject_id}_{idx}.html")
                    except Exception as e:
                        print(f"Ошибка при скачивании {url}: {e}")

	return 0, 'Ok\n'

def build_processor():
	parser = argparse.ArgumentParser()
	parser.add_argument("newobject_id", type=int, help="Cian newobject filter id")
	parser.add_argument("-H", "--host", dest="host", default="localhost", help="Postgres server host (default: %(default)s)")
	parser.add_argument("-P", "--port", dest="port", default=5432, type=int, help="Postgres server port (default: %(default)s)")
	parser.add_argument("-u", "--username", dest="username", default="postgres", help="postgres username (default: %(default)s)")
	parser.add_argument("-p", "--password", dest="password", default="666", help="postgres password (default: %(default)s)")
	parser.add_argument("-d", "--database", dest="database",  default="realtydata", help="postgres dest database (default: %(default)s)")
	#parser.add_argument("-t", "--table", dest="table", default="cian", help="postgres dest database table (default: %(default)s)")
	parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="verbose process output (and store original static files)")
	parser.add_argument("-n", "--dry-run", dest="dry_run", action="store_true", default=False, help="dry run (emulate, not perform upload")
	return parser


def main():
	parser = build_processor()
	args = parser.parse_args()

	parser.exit(*process_cian_site_list(args))

if __name__ == "__main__":
    process_cian_site_list(build_processor().parse_args())