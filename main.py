from flask import Flask
from sql.GlobantDB import GlobantDB

# initiate Flask app
app = Flask(__name__)

# get db class
db = GlobantDB()


@app.route('/create_tables')
def create_tables():
    db.create_tables()
    return "Tables created!"


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=1995)
