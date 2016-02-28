#!/usr/bin/python
###############################################################################
# import_mdb
#
# Filename:		import_mdb.py
# Description:	Python module to import an Access MDB file into PostgreSQL
# Author:		pcs <psanders@ispatechnology.com>
#
# Depends on mdb-tools (in the Debian repos). That's unfortunate.
#
# Usage:
# importer = import_mdb(db_file, db_username, db_user_password, \
#						db_admin_password, db_admin_username, db_host, \
#						db_port, working_folder)
# importer.start_import()
###############################################################################

import datetime
import os
import psycopg2
import re
import subprocess
import sys
import traceback
import uuid


class import_mdb:
    _admin_password = None
    _admin_user = None
    _backup_database_name = None
    _database_host = None
    _database_name = None
    _database_port = None
    _database_user = None
    _finished = False
    _mdb_path = None
    _backup_database_name = None
    _replacements = []
    _user_password = None
    _uuid = None
    _working_dir = None
    schema_sql_filename = None
    table_sql_filenames = None
    log_output = ''

    @property
    def finished(self):
        return self._finished

    @property
    def log_text(self):
        return self.log_output

    @property
    def uuid(self):
        return self._uuid

    def __init__(
            self,
            mdb_path,
            db_user_username,
            db_user_password,
            db_admin_password,
            db_admin_username,
            db_host,
            db_port,
            working_dir):
        # assign values to instance variables
        self._mdb_path = os.path.abspath(os.path.join(working_dir, mdb_path))
        self._user_password = db_user_password
        self._database_user = db_user_username
        self._admin_password = db_admin_password
        self._admin_user = db_admin_username
        self._database_host = db_host
        self._database_port = db_port
        self._working_dir = working_dir
        self._uuid = uuid.uuid4()

    def cancel(self):
        self.log('Canceling...')
        if self._backup_database_name:
            try:
                self.log('Checking for ' + self._database_name + ' database')
                cursor.execute('DROP DATABASE ' + self._database_name)
                self.log('Dropped ' + self._database_name + ' database')
                cursor.execute('ALTER DATABASE ' + self._backup_database_name +
                               ' RENAME TO ' + self._database_name)
                self.log(
                    'Renamed ' +
                    self._backup_database_name +
                    ' database to ' +
                    self._backup_database_name)
            except psycopg2.ProgrammingError as e:
                self.log(str(e))  # "database doesn't exist"
                pass

    def cleanup_schema(self, text, terms):
        '''Replace all instances of a list of words with the
        lowercase of each word
        '''
        if text.startswith('ALTER TABLE') or text.startswith(
                'CREATE INDEX') or text.startswith('CREATE UNIQUE INDEX'):
            return '\n'
        else:
            for term in terms:
                # find and replace term with lowercase
                expression = re.compile(re.escape(term), re.IGNORECASE)
                text = expression.sub(term.lower(), text)
                #self.log(term + ' replaced with ' + term.lower())
            expression = re.compile(re.escape('BOOL'))
            text = expression.sub('INTEGER', text)
            expression = re.compile(re.escape(' NOT NULL'))
            text = expression.sub('', text)
            return text + '\n'

    def dump_tables_to_db(self, tables, cursor):
        '''Dump each table in mdb to sql file'''
        table_files = []
        for table in tables:
            if table != '':
                filename = os.path.abspath(
                    os.path.join(
                        self._working_dir,
                        'table_' +
                        table.lower() +
                        '.sql'))
                table_files = table_files + [filename]
                self.log('Dumping ' + table + ' table...')
                command = [
                    'mdb-export',
                    '-I',
                    'postgres',
                    '-q',
                    "'",
                    self._mdb_path,
                    table.lower()]
                insert_statements = subprocess.Popen(
                    command, stdout=subprocess.PIPE).communicate()[0].strip().split('\n')
                [self.run_insert(line, cursor) for line in insert_statements]

    def get_replacements(self):
        '''Table and column names need to be replace with lowercase
        to avoid problems with case sensitivity in postgres. This method
        compiles all table and column names into a list.
        '''
        replacements = []
        tables = self.get_table_names()
        for table in tables:
            table_data = subprocess.Popen(['mdb-export', self._mdb_path, table],
                                          stdout=subprocess.PIPE).communicate()[0]
            column_names = table_data.split('\n')[0].strip().split(',')
            replacements = replacements + column_names
        replacements = replacements + tables
        return replacements

    def get_table_names(self):
        '''Get table names'''
        table_names = subprocess.Popen(['mdb-tables', '-1', self._mdb_path],
                                       stdout=subprocess.PIPE).communicate()[0]
        tables = table_names.strip().split('\n')
        self._replacements = self._replacements + tables
        self.log('Tables: ' + str(tables))
        return tables

    def log(self, text):
        self.log_output = self.log_output + text + '\n'

    def prepare_database(self, cursor):
        date = datetime.datetime.today().strftime('%Y%m%d%H%M')

        try:
            self._backup_database_name = self._database_name + '_' + date
            self.log('Checking for old ' + self._database_name + ' database')
            cursor.execute('ALTER DATABASE ' + self._database_name +
                           ' RENAME TO ' + self._backup_database_name)
            self.log(
                'Renamed old ' +
                self._database_name +
                ' database to ' +
                self._backup_database_name)
        except psycopg2.ProgrammingError as e:
            self.log(str(e))  # "database doesn't exist"
            pass

        # Create user to own database
        try:
            cursor.execute('CREATE USER ' + self._database_user +
                           ' WITH PASSWORD \'' + self._user_password + '\'')
            self.log('Created user ' + self._database_user)
        except psycopg2.ProgrammingError as e:
            cursor.execute('ALTER USER ' + self._database_user +
                           ' WITH PASSWORD \'' + self._user_password + '\'')
            self.log('Changed password for user ' + self._database_user)

        # Create database
        try:
            cursor.execute('CREATE DATABASE ' + self._database_name +
                           ' OWNER ' + self._database_user)
            self.log(
                'Created database ' +
                self._database_name +
                ' with owner ' +
                self._database_user)
        except psycopg2.ProgrammingError as e:
            self.log('Uh oh! ' + str(e))
            self.log(traceback.format_exc())

        # Grant privileges to user
        try:
            cursor.execute(
                'GRANT ALL PRIVILEGES ON DATABASE ' +
                self._database_name +
                ' TO ' +
                self._database_user)
        except psycopg2.ProgrammingError as e:
            self.log('Uh oh! ' + str(e))
            self.log(traceback.format_exc())

    def run_insert(self, text, cursor):
        '''Prepare and run each insert statement
        Preparation is for case correction
        '''
        # regex for converting table and field names to lowercase
        expression = re.compile('^INSERT INTO "([a-z]+)" \((.*)\) VALUES')
        # replace table name with lower
        text = re.sub(expression, lambda x: x.group(0).lower(), text)
        # replace field name with lower
        text = re.sub(expression, lambda x: x.group(1).lower(), text)
        try:
            # execute insert statement
            cursor.execute(text)
            #self.log('ran insert: ' + text)
        except psycopg2.ProgrammingError as e:
            # some sort of syntax error
            self.log('Uh oh! ' + str(e))
            self.log(traceback.format_exc())
        except psycopg2.IntegrityError as e:
            # thrown if there's a problem with duplicate primary keys or other
            # constraints
            self.log('Uh oh! ' + str(e))
            self.log(traceback.format_exc())

    def start_import(self):
        self.log('MDB is ' + self._mdb_path)

        mdb_path = self._mdb_path
        db_name = os.path.split(mdb_path)[1].strip('.mdb')
        self._database_name = db_name.lower()

        self._replacements = self._replacements + self.get_replacements()
        self.schema_sql_filename = self.write_schema_to_sql()

        try:
            con = psycopg2.connect(dbname='postgres',
                                   user=self._admin_user,
                                   host=self._database_host,
                                   port=self._database_port,
                                   password=self._admin_password)
        except psycopg2.OperationalError as e:  # thrown on bad password, maybe other things?
            raise
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

        # Rename old database; create new user and database
        try:
            self.prepare_database(cur)
        except:
            raise
        finally:
            cur.close()
            con.close()

        # Connect to new database with new user
        con = psycopg2.connect(dbname=self._database_name,
                               user=self._database_user,
                               host=self._database_host,
                               port=self._database_port,
                               password=self._user_password)
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

            self.dump_tables_to_db(self.get_table_names(), cur)

        except:
            raise
        finally:
            cur.close()
            con.close()

        self._finished = True
        self.log('Done.')

        return self._database_name, self._database_user, self.log_output

    def write_schema_to_sql(self):
        schema_file = os.path.abspath(
            os.path.join(
                self._working_dir,
                'schema_' +
                self._database_name +
                '.sql'))
        with open(schema_file, 'w') as f:
            self.log('Extracting schema...')
            schema = subprocess.Popen(['mdb-schema',
                                       self._mdb_path,
                                       'postgres'],
                                      stdout=subprocess.PIPE).communicate()[0].split('\n')
            new_schema = [
                self.cleanup_schema(
                    line,
                    self._replacements) for line in schema]
            f.writelines(new_schema)
            self.log('Schema dumped to ' + schema_file)
        return schema_file
