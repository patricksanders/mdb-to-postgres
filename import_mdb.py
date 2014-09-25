#!/usr/bin/python
###############################################################################
# import_mdb
# 
# Filename:		import_mdb.py
# Description:	Python module to import an Access MDB file into PostgreSQL
# Author:		pcs <psanders@ispatechnology.com>
# 
# Depends on mdb-tools (in the Debian repos). That's unfortunate.
###############################################################################
import datetime
import os
import psycopg2
import re
import subprocess
import sys
import traceback

class import_mdb:
	DATABASE_NAME = None
	MDB_PATH = None
	USER_PASSWORD = None
	DATABASE_USER = None
	ADMIN_PASSWORD = None
	ADMIN_USER = None
	DATABASE_HOST = None
	DATABASE_PORT = None
	REPLACEMENTS = []
	WORKING_DIR = None
	schema_sql_filename = None
	table_sql_filenames = None
	log_output = ''

	def __init__(self, mdb_path, db_user_username, db_user_password, db_admin_password, db_admin_username, db_host, db_port):
		self.MDB_PATH = os.path.abspath(mdb_path)
		self.USER_PASSWORD = db_user_password
		self.DATABASE_USER = db_user_username
		self.ADMIN_PASSWORD = db_admin_password
		self.ADMIN_USER = db_admin_username
		self.DATABASE_HOST = db_host
		self.DATABASE_PORT = db_port
		self.WORKING_DIR = os.path.dirname(mdb_path)
	
	def dump(self):
		self.log('MDB is ' + self.MDB_PATH)
		mdb_path = self.MDB_PATH
		db_name = os.path.split(mdb_path)[1].strip('.mdb')
		self.DATABASE_NAME = db_name.lower()
		self.REPLACEMENTS = self.REPLACEMENTS + self.get_replacements()
		self.schema_sql_filename = self.write_schema_to_sql(db_name)
		self.table_sql_filenames = self.write_tables_to_sql(self.get_table_names())
		self.log('Schema file => ' + str(self.schema_sql_filename))
		self.log('Table files => ' + str(self.table_sql_filenames))
	
	def get_replacements(self):
		replacements = []
		tables = self.get_table_names()
		for table in tables:
			table_data = subprocess.Popen(['mdb-export', self.MDB_PATH, table],
										  stdout=subprocess.PIPE).communicate()[0]
			column_names = table_data.split('\n')[0].strip().split(',')
			replacements = replacements + column_names
		replacements = replacements + tables
		return replacements
	
	def get_table_names(self):
		'''Get table names'''
		table_names = subprocess.Popen(['mdb-tables', '-1', self.MDB_PATH],
									   stdout=subprocess.PIPE).communicate()[0]
		tables = table_names.strip().split('\n')
		self.REPLACEMENTS = self.REPLACEMENTS + tables
		self.log('Tables: ' + str(tables))
		return tables
	
	def import_db(self):
		date = datetime.datetime.today().strftime('%Y%m%d%H%M')
		database_name = self.DATABASE_NAME
		database_user = self.DATABASE_USER
		user_password = self.USER_PASSWORD
		try:
			con = psycopg2.connect(dbname='postgres',
								   user=self.ADMIN_USER,
								   host=self.DATABASE_HOST,
								   port=self.DATABASE_PORT,
								   password=self.ADMIN_PASSWORD)
		except psycopg2.OperationalError as e: # thrown on bad password, maybe other things?
			raise
		con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		cur = con.cursor()

		# Drop old database
		try:
			try:
				backup_database_name = database_name + '_' + date
				self.log('Checking for old ' + database_name + '  database')
				cur.execute('ALTER DATABASE ' + database_name + 
							' RENAME TO ' + backup_database_name)
				self.log('Renamed old ' + database_name + ' database to ' + backup_database_name)
			except psycopg2.ProgrammingError as e:
				self.log(str(e))
				self.log(traceback.format_exc())
				pass

			# Create user to own database
			try:
				cur.execute('CREATE USER ' + database_user + 
							' WITH PASSWORD \'' + user_password + '\'')
				self.log('Created user ' + database_user)
			except psycopg2.ProgrammingError as e:
				cur.execute('ALTER USER ' + database_user + 
							' WITH PASSWORD \'' + user_password + '\'')
				self.log('Changed password for user ' + database_user)

			# Create database
			try:
				cur.execute('CREATE DATABASE ' + database_name + 
							' OWNER ' + database_user)
				self.log('Created database ' + database_name + ' with owner ' + database_user)
			except psycopg2.ProgrammingError as e:
				self.log('Uh oh! ' + str(e))
				self.log(traceback.format_exc())

			# Grant privileges to user
			try:
				cur.execute('GRANT ALL PRIVILEGES ON DATABASE ' + database_name + 
							' TO ' + database_user)
			except psycopg2.ProgrammingError as e:
				self.log('Uh oh! ' + str(e))
				self.log(traceback.format_exc())
		except:
			raise
		finally:
			cur.close()
			con.close()

		# Connect to new database with new user
		con = psycopg2.connect(dbname=self.DATABASE_NAME,
							   user=self.DATABASE_USER,
							   host=self.DATABASE_HOST,
							   port=self.DATABASE_PORT,
							   password=user_password)
		con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		cur = con.cursor()

		# Execute schema sql
		try:
			with open(self.schema_sql_filename, 'r') as f:
				try:
					cur.execute(f.read())
					self.log('Imported database schema')
				except psycopg2.ProgrammingError as e:
					self.log('Uh oh! ' + str(e))
					self.log(traceback.format_exc())

			# Execute inserts for each table
			for table in self.table_sql_filenames:
				with open(table, 'r') as f:
					try:
						cur.execute(f.read())
						self.log('Imported table ' + table)
					except psycopg2.ProgrammingError as e:
						self.log('Uh oh! ' + str(e))
						self.log(traceback.format_exc())
					except psycopg2.IntegrityError as e:
						self.log('Uh oh! ' + str(e))
						self.log(traceback.format_exc())
		except:
			raise
		finally:
			cur.close()
			con.close()
		return database_name, database_user, self.log_output
	
	def log(self, text):
		self.log_output = self.log_output + text + '\n'
		
	def replace_with_lower(self, text, terms):
		'''Replace all instances of a list of words with the lowercase of each word'''
		for term in terms:
			expression = re.compile(re.escape(term), re.IGNORECASE)
			text = expression.sub(term.lower(), text)
			self.log(term + ' replaced with ' + term.lower())
		return text
	
	def write_schema_to_sql(self, db_name):
		schema_file = os.path.abspath(os.path.join(self.WORKING_DIR, 'schema_' + db_name.lower() + '.sql'))
		with open(schema_file, 'w') as f:
			self.log('Extracting schema...')
			schema = subprocess.Popen(['mdb-schema', self.MDB_PATH, 'postgres'],
									  stdout=subprocess.PIPE).communicate()[0]
			schema = self.replace_with_lower(schema, self.REPLACEMENTS)
			expression = re.compile(re.escape('BOOL'))
			schema = expression.sub('INTEGER', schema)
			f.write(schema)
			self.log('Schema dumped to ' + schema_file)
		return schema_file

	def write_tables_to_sql(self, tables):
		'''Dump each table in mdb to sql file'''
		table_files = []
		for table in tables:
			if table != '':
				filename = os.path.abspath(os.path.join(self.WORKING_DIR, 'table_' + table.lower() + '.sql'))
				table_files = table_files + [filename]
				self.log('Dumping ' + table + ' table...')
				with open(filename, 'w') as f:
					insert_statements = subprocess.Popen(['mdb-export', 
														  '-I',
														  'postgres',
														  '-q',
														  '\'',
														  self.MDB_PATH,
														  table],
														  stdout=subprocess.PIPE).communicate()[0]
					insert_statements = self.replace_with_lower(insert_statements, self.REPLACEMENTS)
					f.write(insert_statements)
					f.write('\n')
					self.log(table + ' dumped to ' + filename)
		return table_files

if __name__ == '__main__':
	print 'Welcome to import_mdb'
	importer = import_mdb(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
	print 'Starting dump...'
	schema_file, table_files = importer.dump()
	database_name, database_user = importer.import_db(schema_file, table_files)

