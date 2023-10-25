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
            department_id INTEGER NULL,
            job_id INTEGER NULL,
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
        jobs_df = pd.read_csv(
            './globant-DE-challenge/csv_files/jobs.csv', header=None)
        jobs_df.columns = ['id', 'job']
        jobs_df.to_sql('jobs', con, if_exists='replace',
                       index=False, method='multi')
        con.commit()

    def get_jobs(self):
        con = sqlite3.connect(self.connection_string)
        query = "SELECT * FROM jobs"
        return con.execute(query).fetchall()

    def bulk_insert_departments(self):
        con = sqlite3.connect(self.connection_string)
        jobs_df = pd.read_csv(
            './globant-DE-challenge/csv_files/departments.csv', header=None)
        jobs_df.columns = ['id', 'department']
        jobs_df.to_sql('departments', con, if_exists='replace',
                       index=False, method='multi')
        con.commit()

    def get_departments(self):
        con = sqlite3.connect(self.connection_string)
        query = "SELECT * FROM departments"
        return con.execute(query).fetchall()

    def bulk_insert_hired_employees(self):
        con = sqlite3.connect(self.connection_string)
        jobs_df = pd.read_csv(
            './globant-DE-challenge/csv_files/hired_employees.csv', header=None)
        jobs_df.columns = ['id', 'name', 'datetime', 'department_id', 'job_id']
        jobs_df.to_sql('hired_employees', con,
                       if_exists='replace', index=False, method='multi')
        con.commit()

    def get_hired_employees(self):
        con = sqlite3.connect(self.connection_string)
        query = "SELECT * FROM hired_employees"
        return con.execute(query).fetchall()

    def hires_by_quarter_2021(self):
        con = sqlite3.connect(self.connection_string)
        query = '''SELECT 
            d.department as department, 
            j.job as job, 
            COUNT(CASE WHEN cast(strftime("%m", datetime) as integer) IN (1,2,3) THEN 1 END) as "Q1",
            COUNT(CASE WHEN cast(strftime("%m", datetime) as integer) IN (4,5,6) THEN 1 END) as "Q2", 
            COUNT(CASE WHEN cast(strftime("%m", datetime) as integer) IN (7,8,9) THEN 1 END) as "Q3", 
            COUNT(CASE WHEN cast(strftime("%m", datetime) as integer) IN (10,11,12) THEN 1 END) as "Q4" 
            FROM hired_employees e 
            JOIN jobs j ON e.job_id = j.id 
            JOIN departments d ON e.department_id = d.id 
            WHERE cast(strftime("%Y", datetime) as integer) = 2021 
            GROUP BY d.department, j.job 
            ORDER BY d.department, j.job
        '''
        return con.execute(query).fetchall()

    def top_hiring_departments_2021(self):
        con = sqlite3.connect(self.connection_string)
        query = '''select 
            e.department_id,
            d.department,
            count(e.department_id) as "hired"
            FROM hired_employees e 
            JOIN departments d ON e.department_id = d.id 
            WHERE cast(strftime("%Y", e.datetime) as integer) = 2021
            GROUP BY e.department_id
            HAVING count(e.department_id) > 
            (select count(em.department_id) / (select count(*) from departments)
            FROM hired_employees em
            WHERE cast(strftime("%Y", em.datetime) as integer) = 2021)
            ORDER BY count(e.department_id) DESC
        '''
        return con.execute(query).fetchall()
