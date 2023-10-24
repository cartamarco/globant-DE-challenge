from flask import Flask
from sql.GlobantDB import GlobantDB

# initiate Flask app
app = Flask(__name__)

# get db class
db = GlobantDB()


@app.route('/home')
def home():
    return "Hello World!"


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=1995)
