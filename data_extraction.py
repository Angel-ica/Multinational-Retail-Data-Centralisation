import pandas as pd
import yaml
import random
import sqlalchemy as db
import PyPDF2 as pdf
import requests,os
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
        # con = self.init_db_engine()
        df = pd.read_sql_table(table,con)
        # print(df.columns)
        return df
    
    def upload_to_db(self,creds,df,table):
        db_uri = f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = db.create_engine(db_uri)
        conn = engine.connect()  
        upload =df.to_sql(name=table,con=conn, if_exists='replace') 
        return upload

    def retrieve_pdf_data(self,url):
        data = ''
        response = requests.get(url)
        if response.status_code == 200:
            with open('temp.pdf', 'wb') as file:
                file.write(response.content)

            with open('temp.pdf', 'rb') as file:
                reader = pdf.PdfFileReader(file)
                for page in reader.pages:
                    text = page.extract_text()
                    data +=text

            os.remove('temp.pdf')
        else:
            print(f"Error retrieving PDF data. Status code: {response.status_code}")
        return data


