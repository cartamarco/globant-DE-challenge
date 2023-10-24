import sqlite3


class GlobantDB:
    def __init__(self) -> None:
        # get db connection
        con = sqlite3.connect('globant.db')
