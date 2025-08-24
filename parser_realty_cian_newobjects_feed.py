#!/usr/bin/python3

# wget http://archive.ubuntu.com/ubuntu/pool/main/p/psycopg2/python3-psycopg2_2.6.1-1build2_amd64.deb
# wget http://archive.ubuntu.com/ubuntu/pool/main/p/psycopg2/python-psycopg2-doc_2.6.1-1build2_all.deb
# ### dpkg -i python-psycopg2-doc_2.6.1-1build2_all.deb  python3-psycopg2_2.6.1-1build2_amd64.deb # не пашет
# pip3 install psycopg2
# apt install python3-humanize

import os, os.path, time
import argparse
import psycopg2
import psycopg2.extensions as exts
import psycopg2.extras
import xml.sax
import html
import gzip
import requests
import tempfile
import humanize
import chardet


FEED_URL = os.getenv('FEED_URL')

from pprint import pprint

class CianNewObjectsXmlEventHandler(xml.sax.ContentHandler):

	# https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries
	INSERT_STMT = 'insert into "cian_jk" ("id", "region", "name", "address") VALUES (%(id)s, %(region)s, %(name)s, %(address)s);'
	INSERT_STMT_HOUSES = 'insert into "cian_jk_houses" ("newobject_id", "id", "name", "address") VALUES %s'

	def __init__(self, conn):
		self.conn = conn
		self.object = {}
		self.houses = []
		self.house = {}
		self.in_houses = False
		self.content = ""
		self.count = 0
		self.regions = {}
		super().__init__()

	#def startPrefixMapping(self, prefix, uri):
	#	print("startPrefixMapping", prefix, uri)

	#def endPrefixMapping(self, prefix):
	#	print("endPrefixMapping", prefix)

	#def ignorableWhitespace(self, whitespace):
	#	print("ignorableWhitespace", whitespace)

	#def processingInstruction(self, target, data):
	#	print("processingInstruction", target, data)

	#def skippedEntity(self, name):
	#	print("skippedEntity", name)

	def startDocument(self):
		#pprint(vars(self))
		#print("startDocument")

		# извлекаем справочник регионов: name -> id

		with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
			cur.execute("SELECT * from russia_regions_official")
			for row in cur:
				self.regions[row['name']] = int(row['id'])

		print("Regions: ", self.regions)

	def endDocument(self):
		# last force commit
		self.conn.commit()
		print("Inserted totally {} records".format(self.count), flush=True)

	def startElement(self, name, attrs):
		#print("startElement", name, attrs.getNames())

		# пропускаем
		if name == 'newobjects':
			return

		self.content = ""

		# новый объект
		if name == 'newobject':
			self.object.clear()
			self.object['id'] = int(attrs['id'])

			self.houses.clear()
			self.house.clear()
			self.in_houses = False
			return

		# внутри домов
		if name == 'houses':
			self.house.clear()
			self.in_houses = True
			return

		# новая запись дома
		if self.in_houses and name == 'house':
			self.house.clear()
			#self.house['newobject_id'] = self.object['id'] # WARN делаем это ниже при добавлении в houses
			self.house['id'] = int(attrs['Id'])

	def characters(self, content):
		#print("characters", content)
		self.content += content

	def endElement(self, name):
		#print("endElement", name)

		# пропускаем
		if name == 'newobjects':
			return

		if name == 'newobject':

			with self.conn.cursor() as cur:

				try:
					#print(cur.mogrify(self.INSERT_STMT, self.object).decode('utf-8'))
					cur.execute(self.INSERT_STMT, self.object)
					#print('object: {}'.format(self.object), flush=True)
				except:
					print('object: {}'.format(self.object), flush=True)
					raise

				if len(self.houses):
					try:
						# DERP cur.executemany(self.INSERT_STMT_HOUSES, self.houses)
						psycopg2.extras.execute_values(cur, self.INSERT_STMT_HOUSES, self.houses)
						#print('houses: {}'.format(self.houses), flush=True)
					except:
						print('object: {}'.format(self.object), flush=True)
						print('houses: {}'.format(self.houses), flush=True)
						raise

			self.object.clear()
			self.houses.clear()

			self.count += 1

			return

		# внутри домов
		if name == 'houses':
			self.content = ""
			self.in_houses = False
			return

		value = self.content.strip()

		if not self.in_houses: # объект

			# подменяем название региона на его code
			if name == 'region':
				# суффикс ' Республика' меняется на ' республика', после чего его id берется из справочника
				if value.endswith(' Республика'):
					value = value.replace(' Республика', ' республика')

				if value not in self.regions:
					raise ValueError('cant found value "{}" in regions for object {}'.format(value, self.count + 1))

				value = self.regions[value]

			self.object[name] = value

		else: # дом
			if name == 'house': # дом завершен
				# DERP self.houses.append(self.house.copy()) # copy is mandatory
				# сразу превращаем в tuple "newobject_id", "id", "name", "address" - keep in sync with self.INSERT_STMT_HOUSES
				self.houses.append((self.object['id'], self.house['id'], self.house['name'], self.house['address']))
				self.house.clear()
			else: # элементы дома
				self.house[name] = value

		self.content = ""


def process_cian_new_objects_xml(args):

	print("Source uri: {}".format(FEED_URL))
	print("Use database {}".format(args.database))

	if args.verbose:
		print("Verbose mode (and store original files)")

	if args.dry_run:
		print("Dry-run mode (don't update database records, only static files)")

	with tempfile.TemporaryFile(mode='w+b', prefix='cian_newobjects_feed') as fp:

		# пытаемся скачать файл
		# NOTE можно использовать urllib.request.urlretrieve
		with requests.get(FEED_URL, stream=True) as response:
			for chunk in response.iter_content(10 * 1024):
				if chunk:
					fp.write(chunk)

		# используем текущую позицию в файле как эквивалент его размера
		print("Feed has been downloaded, size: {}".format(humanize.naturalsize(fp.tell(), gnu=True)))

		# отматываем в самое начало
		fp.seek(0)

		try:
			conn = psycopg2.connect(
				host=os.getenv('PG_HOST', args.host),
				port=int(os.getenv('PG_PORT', args.port)),
				user=os.getenv('PG_USER', args.username),
				password=os.getenv('PG_PASSWORD', args.password),
				dbname=os.getenv('PG_DATABASE', args.database)
			)
			conn.autocommit = True
		except (psycopg2.Warning, psycopg2.Error) as e:
			err = e

			"""
			if hasattr(e, 'pgerror'):
				err = e.pgerror

			if hasattr(e, 'message'):
				err = e.message
			"""

			return 2, str(err)

		with conn:

			# truncate tables
			with conn.cursor() as cur:
				cur.execute('TRUNCATE TABLE "cian_jk" RESTART IDENTITY;')
				cur.execute('TRUNCATE TABLE "cian_jk_houses" RESTART IDENTITY;')

			if args.verbose:
				print("starting sax parser...")

			xml.sax.parse(fp, CianNewObjectsXmlEventHandler(conn))

			if args.verbose:
				print("ending sax parser...")

	return 0, 'Ok\n'

def build_processor():
	parser = argparse.ArgumentParser()
	parser.add_argument("-H", "--host", dest="host", default="127.0.0.1", help="Postgres server host (default: %(default)s)")
	parser.add_argument("-P", "--port", dest="port", default=5432, type=int, help="Postgres server port (default: %(default)s)")
	parser.add_argument("-u", "--username", dest="username", default="postgres", help="postgres username (default: %(default)s)")
	parser.add_argument("-p", "--password", dest="password", default="postgres", help="postgres password (default: %(default)s)")
	parser.add_argument("-d", "--database", dest="database",  default="realtydata", help="postgres dest database (default: %(default)s)")
	parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", default=False, help="verbose process output (and store original static files)")
	parser.add_argument("-n", "--dry-run", dest="dry_run", action="store_true", default=False, help="dry run (emulate, not perform upload")
	return parser


def main():
	parser = build_processor()
	args = parser.parse_args()

	parser.exit(*process_cian_new_objects_xml(args))

if __name__ == "__main__":
	main()




