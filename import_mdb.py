#! /usr/bin/python
###############################################################################
# import_mdb
# 
# Filename:		import_mdb.py
# Description:	Python module to import an Access MDB file into PostgreSQL
# Author:		pcs <psanders@ispatechnology.com>
# 
# Depends on mdb-tools (in the Debian repos). That's unfortunate.
###############################################################################
import os
import psycopg2
import re
import subprocess
import sys

class import_mdb:
	VERBOSE = False
	DATABASE_NAME = None
	MDB_PATH = None
	PASSWORD = None
	USER = None
	replacements = []
	schema_sql_filename = None
	table_sql_filenames = None

	def __init__(self, mdb_path, db_user, db_password):
		self.MDB_PATH = os.path.abspath(mdb_path)
		self.PASSWORD = db_password
		self.USER = db_user
	
	def dump(self):
		print 'MDB is', self.MDB_PATH
		mdb_path = self.MDB_PATH
		db_name = os.path.split(mdb_path)[1].strip('.mdb')
		self.DATABASE_NAME = db_name.lower()
		self.replacements = self.replacements + self.get_replacements()
		schema_sql_filename = self.write_schema_to_sql(db_name)
		table_sql_filenames = self.write_tables_to_sql(self.get_table_names())
		if self.VERBOSE:
			print 'Schema file =>', schema_sql_filename
			print 'Table files =>', table_sql_filenames
		return schema_sql_filename, table_sql_filenames
	
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
		self.replacements = self.replacements + tables
		if self.VERBOSE:
			print 'Tables:', tables
		return tables
	
	def import_db(self, schema_file, table_files):
		database_name = self.DATABASE_NAME
		database_user = database_name + '_user'
		database_password = self.PASSWORD
		con = psycopg2.connect(dbname='postgres', user='postgres', host='localhost', password=database_password)
		con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		cur = con.cursor()
		# Drop old database
		# TODO: Archive old database before dropping
		try:
			if self.VERBOSE:
				print 'Attempting to drop old', database_name, 'database...'
			cur.execute('DROP DATABASE ' + database_name)
		except psycopg2.ProgrammingError as e:
			if self.VERBOSE:
				print 'Database', database_name, 'not present. Skipping.'
			pass
		# Get rid of old user if present
		try:
			if self.VERBOSE:
				print 'Attempting to drop old', database_user, 'user...'
			cur.execute('DROP USER ' + database_user)
		except psycopg2.ProgrammingError as e:
			if self.VERBOSE:
				print 'User', database_user, 'not present. Skipping.'
			pass
		# Create user to own database
		try:
			cur.execute('CREATE USER ' + database_user + ' WITH PASSWORD \'' + database_password + '\'')
			print 'Created user', database_user, 'with password', database_password
		except psycopg2.ProgrammingError as e:
			print 'Uh oh!', e
		# Create database
		try:
			cur.execute('CREATE DATABASE ' + database_name + ' OWNER ' + database_user)
			print 'Created database', database_name, 'with owner', database_user
		except psycopg2.ProgrammingError as e:
			print 'Uh oh!', e
		# Grant privileges to user
		try:
			cur.execute('GRANT ALL PRIVILEGES ON DATABASE ' + database_name + ' TO ' + database_user)
		except psycopg2.ProgrammingError as e:
			print 'Uh oh!', e
		cur.close()
		con.close()

		# Connect to new database with new user
		con = psycopg2.connect(dbname=database_name, user=database_user, host='localhost', password=database_password)
		con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
		cur = con.cursor()
		# Execute schema sql
		with open(schema_file, 'r') as f:
			try:
				cur.execute(f.read())
			except psycopg2.ProgrammingError as e:
				print 'Uh oh!', e
		# Execute inserts for each table
		for table in table_files:
			with open(table, 'r') as f:
				try:
					cur.execute(f.read())
				except psycopg2.ProgrammingError as e:
					print 'Uh oh!', e
		cur.close()
		con.close()
		return database_name, database_user
		
	def replace_with_lower(self, text, terms):
		'''Replace all instances of a list of words with the lowercase of each word'''
		for term in terms:
			expression = re.compile(re.escape(term), re.IGNORECASE)
			text = expression.sub(term.lower(), text)
			if self.VERBOSE:
				print term, 'replaced with', term.lower()
		return text
	
	def write_schema_to_sql(self, db_name):
		schema_file = 'schema_' + db_name.lower() + '.sql'
		with open(schema_file, 'w') as f:
			print 'Extracting schema...'
			schema = subprocess.Popen(['mdb-schema', self.MDB_PATH, 'postgres'],
									  stdout=subprocess.PIPE).communicate()[0]
			schema = self.replace_with_lower(schema, self.replacements)
			expression = re.compile(re.escape('BOOL'))
			schema = expression.sub('INTEGER', schema)
			f.write(schema)
			print 'Schema dumped to', schema_file
		return schema_file

	def write_tables_to_sql(self, tables):
		'''Dump each table in mdb to sql file'''
		table_files = []
		for table in tables:
			if table != '':
				filename = 'table_' + table.lower() + '.sql'
				table_files = table_files + [filename]
				print 'Dumping', table
				with open(filename, 'w') as f:
					insert_statements = subprocess.Popen(['mdb-export', '-I', 'postgres', '-q', '\'', self.MDB_PATH, table],
												stdout=subprocess.PIPE).communicate()[0]
					insert_statements = self.replace_with_lower(insert_statements, self.replacements)
					f.write(insert_statements)
					f.write('\n')
					print table, 'dumped to', filename
		return table_files

if __name__ == '__main__':
	print 'Welcome to import_mdb'
	importer = import_mdb(sys.argv[1], sys.argv[2], sys.argv[3])
	print 'Starting dump...'
	schema_file, table_files = importer.dump()
	database_name, database_user = importer.import_db(schema_file, table_files)

