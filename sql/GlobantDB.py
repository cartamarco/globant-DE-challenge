import sqlite3


class GlobantDB:
    def __init__(self) -> None:
        # get db connection and save it as a property
        self.con = sqlite3.connect('globant.db')

    def create_tables(self):
        script = """
        
        CREATE TABLE IF NOT EXISTS jobs(
            id INTEGER PRIMARY KEY,
            job TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS departments(
            id INTEGER PRIMARY KEY,
            department TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS hired_employees(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            datetime TEXT NOT NULL,
            department_id INTEGER NOT NULL,
            job_id INTEGER NOT NULL,
            FOREIGN KEY (department_id)
                REFERENCES departments(id)
            FOREIGN KEY (job_id)
                REFERENCES jobs(id)
        );
        """
        self.con.executescript(script)
