import pandas as pd
import yaml
import random
import sqlalchemy as db
import PyPDF2 as pdf
import tabula
from data_cleaning import DataCleaning

# Class to extract data using Sqlalchemy

class DataExtractor:
    def __init__(self):
        pass

    def read_creds(self,yaml_file):
        with open(yaml_file, 'r') as file:
            self.credentials = yaml.safe_load(file)
            return self.credentials

    def init_db_engine(self,creds):
        db_uri = f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = db.create_engine(db_uri)
        conn = engine.connect()
        return conn

    def read_data(self,conn):
        inspector = db.inspect(conn)
        tables = inspector. get_table_names()
        print(tables)
        return tables

    def read_rds_table(self,con,table):
        df = pd.read_sql_table(table,con)
        # print(df.columns)
        return df
    
    def upload_to_db(self,creds,df,table_name):
        db_uri = f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = db.create_engine(db_uri)
        conn = engine.connect()  
        upload =df.to_sql(name=table_name,con=conn, if_exists='replace') 
        return upload

    def retrieve_pdf_data(self,url):
        dfs = pd.concat(tabula.read_pdf(url,pages='all'),ignore_index=True)
        return dfs



