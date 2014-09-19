import import_mdb
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
		db_user = request.values['inputUser']
		db_user_password = request.values['inputUserPassword']
		db_admin_password = request.values['inputAdminPassword']
		if db_file and allowed_file(db_file.filename):
			filename = secure_filename(db_file.filename)
			db_file.save(os.path.join(app.config['WORKING_FOLDER'], filename))
			#return redirect(url_for('uploaded_file', filename=filename))
			return str(os.path.abspath(filename) + ' ' + db_user + ' ' + db_user_password + ' ' + db_admin_password)
	return render_template('index.html', p=app.config['TEMPLATE_PARAMS'])

@app.route('/uploads/<filename>')
def uploaded_file(filename):
	return send_from_directory(app.config['WORKING_FOLDER'], filename)

def start_import(db_file, db_user, db_user_password, db_admin_password):
	importer = import_mdb(db_file, db_user, db_user_password, db_admin_password)

if __name__ == '__main__':
	app.run()
