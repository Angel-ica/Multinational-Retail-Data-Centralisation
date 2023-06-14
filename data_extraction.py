import pandas as pd,yaml
import random
import sqlalchemy as db

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
    

if __name__=='__main__':

    def main():
        dex = DataExtractor('db_creds.yaml')
        table = dex.read_rds_table('legacy_users')
        print(table)
    main()

