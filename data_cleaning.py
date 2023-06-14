from data_extraction import DataExtractor
import pandas as pd, numpy as np
from datetime import datetime
import re
from pprint import pprint

#Index(['index', 'first_name', 'last_name', 'date_of_birth', 'company','email_address', 'address', 'country', 'country_code', 'phone_number','join_date', 'user_uuid'],dtype='object')

class DataCleaning:
    def __init__(self, config_file, table):
        self.table = DataExtractor(config_file).read_rds_table(table)

    def remove_null(self):
        self.r_table = pd.DataFrame(self.table)
        self.r_table.replace('NULL',np.nan,inplace=True)   
        self.r_table.dropna(inplace=True) 
        # print(self.r_table)  
        return self.r_table 

    def valid_date(self,date_column):
        date_format = '%Y-%m-%d'
        for date in self.r_table[date_column]:
            if not isinstance(date, type(date_format)):
                datetime.strptime(date, date_format)

    def valid_email(self,email_column):
        email_count=0
        for email in self.r_table[email_column]:
            email_count+=1
            pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
            if not re.match(pattern, email):
                self.r_table.replace(email,np.nan,inplace=True)
                email_count -=1
        # print(email_count)

    def valid_phone_no(self,phone_no_column):
        no_count = 0
        invalid = []
        valid = []
        for phone_no in self.r_table[phone_no_column]:
            no_count+=1
            print(no_count)
            pattern = r'^\+?[1-9]\d{1,14}$'
            if not re.match(pattern, phone_no):
                invalid.append(phone_no)
                no_count-=1
            else:
                valid.append(phone_no)
        print(no_count)
        pprint(valid)
    


def clean():
    dcl = DataCleaning('db_creds.yaml','legacy_users')
    dcl.remove_null()
    dcl.valid_date('date_of_birth')
    dcl.valid_email('email_address')
    dcl.valid_phone_no('phone_number')
clean()
    
