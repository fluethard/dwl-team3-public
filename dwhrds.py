from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import force_auto_coercion
from sqlalchemy import exc
from dwhormtables import *
import configparser
import psycopg2
import pandas as pd
from sqlalchemy import Column
from sqlalchemy.types import Integer, Text, String, Date, DateTime, Float, Boolean
import sqlalchemy

# Class for interacting with the database
# Class-based approach chosen to allow working with sqlalchemy sessions


force_auto_coercion()


class rdsSession:

    def __init__(self, db):
        self.db = db
        self.config = configparser.ConfigParser()
        self.path = '/home/ubuntu/airflow/dags/dwl-team3/my_config.ini'
        pass

    def open_session(self):
        # Create engine and open session to DB
        self.config.read(self.path)
        self.config.sections()
        conn_string = self.config['dwh_string']['conn_string']
        print(conn_string)

        try:
            engine = create_engine(conn_string)
            Session = sessionmaker(bind=engine)

            self.e = engine

            self.s = Session()
            print('Session opened')


        except:
            print('Failed to open session. connection attempt failed')



    def close_session(self):
        self.s.close
        print('Session closed')

    def create_tables(self):
        # Initial method for creating tables in the database based on definitions in ormtables.py
        Base.metadata.create_all(self.e)
        print('Tables created')



dbsession = rdsSession('dwl_dwh')
dbsession.open_session()
dbsession.create_tables()
dbsession.close_session()

# %%
