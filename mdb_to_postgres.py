from import_mdb import import_mdb
import os
from flask import Flask, redirect, render_template, request, send_from_directory, url_for
from werkzeug import secure_filename

DEBUG = True
UPLOAD_FOLDER = './working'
ALLOWED_EXTENSIONS = set(['mdb'])

template_params = {'site_title': 'MDB Importer'}

app = Flask(__name__)
app.config.from_object('default_settings')

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def index():
	if request.method == 'POST':
		db_file = request.files['file']
		db_user = request.values.get('inputUser')
		db_user_password = request.values.get('inputUserPassword')
		db_admin_password = request.values.get('inputAdminPassword')
		if db_file and allowed_file(db_file.filename):
			filename = secure_filename(db_file.filename)
			file_path = os.path.join(app.config['WORKING_FOLDER'], filename)
			db_file.save(file_path)
			result, detail = start_import(file_path, db_user, db_user_password, db_admin_password)
			return render_template('result.html', p=app.config['TEMPLATE_PARAMS'], result=result, detail=detail)
	return render_template('index.html', p=app.config['TEMPLATE_PARAMS'])

@app.route('/uploads/<filename>')
def uploaded_file(filename):
	return send_from_directory(app.config['WORKING_FOLDER'], filename)

def start_import(db_file, db_user, db_user_password, db_admin_password):
	importer = import_mdb(db_file, db_user, db_user_password, db_admin_password)
	schema_file, table_files = importer.dump()
	database_name, database_user, detail = importer.import_db(schema_file, table_files)
	overview = 'Database ' + database_name + ' created with owner ' + database_user
	return overview, detail

if __name__ == '__main__':
	app.run()

