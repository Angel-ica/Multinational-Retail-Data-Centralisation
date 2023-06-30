import pandas as pd
import numpy as np
from datetime import datetime
import re
from pprint import pprint 

class DataCleaning:
    def __init__(self):
        pass

    def remove_null(self,r_table):
        self.r_table = pd.DataFrame(r_table)
        self.r_table.replace('NULL', np.nan, inplace=True)
        self.r_table.dropna(inplace=True)
        # print(self.r_table)
        return self.r_table 

    def valid_date(self,table, date_column):
        table[date_column] = pd.to_datetime(table[date_column],errors='coerce')
        return table[date_column]

    def valid_email(self,table, email_column):
        for i, email in enumerate(table[email_column]):
            pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
            if not re.match(pattern, email):
                table.loc[i,email_column] = np.nan
        table.dropna(subset=[email_column], inplace=True)
        return table[email_column]

# TODO : modify phone num column 
    def valid_phone_no(self,table, phone_no_column):
        pattern = r'^(?!.*\s)(?!.{10,11}$)[0-9,./a-zA-Z]+$'
        for i, phone_no in enumerate(table[phone_no_column]):
            if re.match(pattern, phone_no):
                table.loc[i,phone_no_column] = np.nan
        table.dropna(subset=[phone_no_column], inplace=True)
        print(table[phone_no_column])
        return table[phone_no_column]

    def valid_name(self, table,name_column):
        pattern = r'^\d+$'
        for i, name in enumerate(table[name_column]):
            if re.match(pattern, name):
                table.loc[i,name_column] = np.nan
        table.dropna(subset=[name_column], inplace=True)
        return table[name_column]

    def valid_ccode(self,table, ccode_column):
        valid = {'Germany': 'DE', 'United Kingdom': 'UK', 'United States': 'US'}
        for i, country in enumerate(table['country']):
            if country in valid.keys():
                table.loc[i, ccode_column] = list(valid.values())[list(valid.keys()).index(country)]
                return table[ccode_column]


    def clean_user_data(self,table):
        self.valid_date(table,'date_of_birth')
        self.valid_date(table,'join_date')
        self.valid_email(table,'email_address')
        self.valid_phone_no(table,'phone_number')
        self.valid_name(table,'first_name')
        self.valid_name(table,'last_name')
        self.valid_ccode(table,'country_code')
        self.remove_null(table)
        print(table)
        return table

    def clean_card_data(self,table):
        self.valid_date(table,'date_payment_confirmed')
        self.remove_null(table)
        try:
            assert table['card_number'].str.isdigit()
        except:
            AssertionError

        print (table)
        return table


    