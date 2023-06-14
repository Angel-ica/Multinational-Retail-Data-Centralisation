import psycopg2
import yaml


# Class to connect to database using psycopg2
class DatabaseConnector:
    def __init__(self,yaml_file):
        self.yaml_file = yaml_file
        pass

    def read_creds(self):
        with open(self.yaml_file, 'r') as file:
            creds = yaml.safe_load(file)
            return creds
    
    def establish_conn(self):
        creds = self.read_creds()
        conn = psycopg2.connect(user =creds['RDS_USER'],password=creds['RDS_PASSWORD'],host =creds['RDS_HOST'],port =creds['RDS_PORT'],dbname= creds['RDS_DATABASE'])
        cur = conn.cursor() 
        return cur

    def list_db_tables(self):
        cur = self.establish_conn()
        query = ("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
        cur.execute(query)
        for table in cur.fetchall():
            print(table)


if __name__ == '__main__':
    
    dc = DatabaseConnector('db_creds.yaml')
    def main():
        dc.list_db_tables()
    main()


