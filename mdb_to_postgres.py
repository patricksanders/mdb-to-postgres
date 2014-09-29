from import_mdb import import_mdb
import os
import traceback
from flask import Flask, redirect, render_template, request, send_from_directory, url_for
from werkzeug import secure_filename

ALLOWED_EXTENSIONS = set(['mdb'])

app = Flask(__name__)
app.config.from_object('default_settings')

@app.route('/', methods=['GET', 'POST'])
def index():
	return render_template('form.html',
						   p=app.config['TEMPLATE_PARAMS'])

@app.route('/submit', methods=['POST'])
def submit():
	raw_filename = request.values.get('inputDbFile')
	db_filename = secure_filename(raw_filename)
	db_user_username = request.values.get('inputUser')
	db_user_password = request.values.get('inputUserPassword')
	db_admin_password = request.values.get('inputAdminPassword')
	db_admin_username = request.values.get('inputAdminUser')
	db_host = request.values.get('inputHost')
	db_port = request.values.get('inputPort')
	try:
		result, detail = start_import(db_filename, db_user_username, db_user_password, db_admin_password, db_admin_username, db_host, db_port)
		return render_template('result.html',
							   p=app.config['TEMPLATE_PARAMS'],
							   result=result,
							   detail=detail)
	except Exception as e:
		unfriendly = traceback.format_exc()
		return render_template('error.html',
							   #error=friendly,
							   description=unfriendly,
							   p=app.config['TEMPLATE_PARAMS'])

@app.route('/uploads/<filename>')
def uploaded_file(filename):
	return send_from_directory(app.config['WORKING_FOLDER'], filename)

@app.route('/upload', methods=['POST'])
def upload():
	db_file = request.files['file']
	if db_file and allowed_file(db_file.filename):
		filename = secure_filename(db_file.filename)
		file_path = os.path.join(app.config['WORKING_FOLDER'], filename)
		db_file.save(file_path)
		return 'yep', 200
	else:
		return 'nope', 500

def allowed_file(filename):
	return '.' in filename and filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS

def start_import(db_file, db_username, db_user_password, db_admin_password, db_admin_username, db_host, db_port):
	importer = import_mdb(db_file, db_username, db_user_password, db_admin_password, db_admin_username, db_host, db_port, app.config['WORKING_FOLDER'])
	try:
		database_name, database_user, detail = importer.start_import()
		overview = 'Database ' + database_name + ' created with owner ' + database_user
		return overview, detail
	except Exception as e:
		raise

if __name__ == '__main__':
	app.run()

