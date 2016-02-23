## Install Instructions

* Copy mdb-to-postgres folder to destination system
* Fix permissions

	chown -R www-data:www-data mdb-to-postgres/
	chmod -R 750 mdb-to-postgres/

* Add wsgi directives to apache config

	```
	WSGIDaemonProcess mdb-to-postgres user=www-data group=www-data threads=5
	WSGIScriptAlias /importdb /apps/mdb-to-postgres/mdb_to_postgres.wsgi
	Alias /importdb/static /apps/mdb-to-postgres/static

	<Directory /apps/mdb-to-postgres>
		WSGIProcessGroup mdb-to-postgres
		WSGIApplicationGroup %{GLOBAL}
		Order deny,allow
		Allow from all
	</Directory>
	```

* Make any necessary changes to mdb-to-postgres/default-config.py
	* NOTE: Make sure www-data has permissions to write to the working directory
* Restart Apache

