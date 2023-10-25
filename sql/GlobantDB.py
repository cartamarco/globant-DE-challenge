import sqlite3
import pandas as pd


class GlobantDB:

    def __init__(self, connection_string='globant.db') -> None:
        self.connection_string = connection_string

    def create_tables(self):
        con = sqlite3.connect(self.connection_string)
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
        con.executescript(script)
        con.commit()

    def bulk_insert_jobs(self):
        con = sqlite3.connect(self.connection_string)
        jobs_df = pd.read_csv('./globant-DE-challenge/csv_files/jobs.csv')
        jobs_df.columns = ['id', 'job']
        # adding method='multi' speeds up process, but needs error handling for duplicates.
        jobs_df.to_sql('jobs', con, if_exists='append', index=False)
        con.commit()

    def get_jobs(self):
        con = sqlite3.connect(self.connection_string)
        query = "SELECT * FROM jobs"
        return con.execute(query).fetchall()

    def bulk_insert_departments(self):
        con = sqlite3.connect(self.connection_string)
        jobs_df = pd.read_csv(
            './globant-DE-challenge/csv_files/departments.csv')
        jobs_df.columns = ['id', 'department']
        # adding method='multi' speeds up process, but needs error handling for duplicates.
        jobs_df.to_sql('departments', con, if_exists='replace', index=False)
        con.commit()

    def get_departments(self):
        con = sqlite3.connect(self.connection_string)
        query = "SELECT * FROM departments"
        return con.execute(query).fetchall()

    def bulk_insert_hired_employees(self):
        con = sqlite3.connect(self.connection_string)
        jobs_df = pd.read_csv(
            './globant-DE-challenge/csv_files/hired_employees.csv')
        jobs_df.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
        # adding method='multi' speeds up process, but needs error handling for duplicates.
        jobs_df.to_sql('hired_employees', con,
                       if_exists='replace', index=False)
        con.commit()

    def get_hired_employees(self):
        con = sqlite3.connect(self.connection_string)
        query = "SELECT * FROM hired_employees"
        return con.execute(query).fetchall()
