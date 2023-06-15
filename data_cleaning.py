import pandas as pd
import numpy as np
from datetime import datetime
import re
from pprint import pprint
from data_extraction import DataExtractor

class DataCleaning:
    def __init__(self, config_file, table):
        self.table = DataExtractor(config_file).read_rds_table(table)
        self.r_table = pd.DataFrame(self.table)

    def remove_null(self):
        self.r_table = pd.DataFrame(self.table)
        self.r_table.replace('NULL', np.nan, inplace=True)
        self.r_table.dropna(inplace=True)
        return self.r_table 

    def valid_date(self, date_column):
        date_format = '%Y-%m-%d'
        for i, date in enumerate(self.r_table[date_column]):
            if not isinstance(date, str):
                self.r_table[date_column][i] = datetime.strftime(date, date_format)
            else:
                try:
                    datetime.strptime(date, date_format)
                except ValueError:
                    self.r_table.loc[i,date_column] = np.nan
        self.r_table.dropna(subset=[date_column], inplace=True)

    def valid_email(self, email_column):
        for i, email in enumerate(self.r_table[email_column]):
            pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
            if not re.match(pattern, email):
                self.r_table.loc[i,email_column] = np.nan
        self.r_table.dropna(subset=[email_column], inplace=True)

    def valid_phone_no(self, phone_no_column):
        for i, phone_no in enumerate(self.r_table[phone_no_column]):
            pattern = r'^(?!.*\s)(?!.{10,11}$)[0-9,./a-zA-Z]+$'
            if re.match(pattern, phone_no):
                self.r_table.loc[i,phone_no_column] = np.nan
        self.r_table.dropna(subset=[phone_no_column], inplace=True)

    def valid_name(self, name_column):
        pattern = r'^\d+$'
        for i, name in enumerate(self.r_table[name_column]):
            if re.match(pattern, name):
                self.r_table.loc[i,name_column] = np.nan
        self.r_table.dropna(subset=[name_column], inplace=True)

    def valid_ccode(self, ccode_column):
        valid = {'Germany': 'DE', 'United Kingdom': 'UK', 'United States': 'US'}
        for i, country in enumerate(self.r_table['country']):
            if country in valid:
                self.r_table.loc[i, ccode_column] = valid[country]
        return self.r_table[ccode_column]


    def clean_user_data(self):
        self.valid_date('date_of_birth')
        self.valid_date('join_date')
        self.valid_email('email_address')
        self.valid_phone_no('phone_number')
        self.valid_name('first_name')
        self.valid_name('last_name')
        self.valid_ccode('country_code')
        self.remove_null()
        return self.r_table

def clean():
    dcl = DataCleaning('db_creds.yaml', 'legacy_users')
    cleaned_data = dcl.clean_user_data()
    pprint(cleaned_data)
    
clean()
