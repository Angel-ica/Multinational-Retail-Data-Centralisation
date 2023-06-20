import pandas as pd
import yaml
import random
import sqlalchemy as db
import PyPDF2 as pdf
import requests,os
from data_cleaning import DataCleaning


# Class to extract data using Sqlalchemy

class DataExtractor:
    def __init__(self,yaml_file):
        self.yaml_file = yaml_file

    def read_creds(self):
        with open(self.yaml_file, 'r') as file:
            credentials = yaml.safe_load(file)
            return credentials

    def init_db_engine(self):
        creds = self.read_creds()
        db_uri = f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = db.create_engine(db_uri)
        conn = engine.connect()
        return conn

    def read_data(self):
        inspector = db.inspect(self.init_db_engine())
        tables = inspector. get_table_names()
        return tables

    def read_rds_table(self,table):
        con = self.init_db_engine()
        df = pd.read_sql_table(table,con)
        # print(df.columns)
        return df
    
    def upload_to_db(self,df,table):
        creds = self.read_creds()
        db_uri = f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = db.create_engine(db_uri)
        conn = engine.connect()  
        df.to_sql(name=table,con=conn, if_exists='replace') 

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


if __name__=='__main__':

    def main():
        dex = DataExtractor('db_creds.yaml')
        table = dex.read_rds_table('legacy_users')
        file = dex.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        clean_cd = DataCleaning('legacy_users')
        clean_cd.clean_card_data(file)
        print(clean_cd)
    main()

#TODO
'Carry out all tasks in a main function to resolve conflicts'