from flask import Flask, request, jsonify
from sql.GlobantDB import GlobantDB

# initiate Flask app
app = Flask(__name__)

# get db class
db = GlobantDB("globant.db")

# route for creating tables


@app.route('/create_tables')
def create_tables():
    db.create_tables()
    return "Tables created!"

# route dedicated to the jobs resource. POST method inserts data from csv, GET performs select all.


@app.route('/jobs', methods=['GET', 'POST'])
def jobs():
    if request.method == 'POST':
        db.bulk_insert_jobs()
        return "Jobs inserted!"
    if request.method == 'GET':
        return jsonify(db.get_jobs())


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=1995)
