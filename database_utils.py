import psycopg2
import yaml, pandas as pd
 
# Class to connect to database using psycopg2
class DatabaseConnector:
    def __init__(self,yaml_file):
        self.yaml_file = yaml_file

    def read_creds(self):
        with open(self.yaml_file, 'r') as file:
            creds = yaml.safe_load(file)
            return creds
    
    def establish_conn(self):
        self.creds = self.read_creds()
        conn = psycopg2.connect(user =self.creds['RDS_USER'],password=self.creds['RDS_PASSWORD'],host =self.creds['RDS_HOST'],port =self.creds['RDS_PORT'],dbname= self.creds['RDS_DATABASE'])
        cur = conn.cursor() 
        return cur

    def list_db_tables(self):
        cur = self.establish_conn()
        query = ("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
        cur.execute(query)
        for table in cur.fetchall():
            print(table)

    # def upload_to_db(self,df,table):
    #     # df = pd.DataFrame()
    #     conn = self.establish_conn()
    #     try:
    #         conn.execute(df.to_sql(name=table, con=conn, if_exists='replace', index=False))
    #         conn.close()
    #         # df.to_sql(name=table, con=conn, if_exists='replace', index=False)
    #         print(f"Data uploaded to '{table}' table successfully.")
    #     except Exception as e:
    #         print(f"Error uploading data to '{table}' \n {e}")
    #     # conn.execute(df.to_sql(name=table, con=conn, if_exists='replace', index=False))
    #     conn.close()


if __name__ == '__main__':
    
    dc = DatabaseConnector('db_creds.yaml')
    def main():
        dc.list_db_tables()
    main()


