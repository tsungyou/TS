
import pandas as pd
import psycopg2
from psycopg2 import sql
from tqdm import tqdm
import json
import time
from datetime import datetime, timedelta
from config import DB_HOST, DB_NAME, DB_PASS, DB_USER

class SIGNAL(object):
    def __init__(self):
        self.conn = None
        self.cursor = None    
        self.db_init_year_index = 2015
        self.year = datetime.now().year
        self.month = datetime.now().month
        self._connection()
    def _connection(self):
        if self.conn == None:
            print(f"Connecting to database {DB_NAME}...")
            conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
            self.conn = conn
            self.cursor = self.conn.cursor()
            print("Connection Successful!")
        print("Connection already existed")
        return None

    def insert_df_into_db(self, df: pd.DataFrame, table='public.block_trade'):
        df_list = df.values.tolist()
        if table == 'public.block_trade':
            self.cursor.executemany(f'''
            INSERT INTO {table} (code, cname, type_, cl, vol_share, vol, da)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', df_list)
        elif table == 'public.maincode':
            self.cursor.executemany(f'''
            INSERT INTO {table} (code, cname, listed)
            VALUES (%s, %s, %s)
            ''', df_list)
        elif table == 'public.price':
            self.cursor.executemany(f'''
            INSERT INTO {table} (da, vol_share, vol, op, hi, lo, cl, code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', df_list)     
        self.conn.commit()
    
    def create_table_if_not_exist(self, db):
        '''
        db options:
        1. block_trade
        2. signal
        3. stock_code
        4. stock_price
        '''
        create_query = ""
        self.cursor.execute(create_query)
        self.conn.commit()
    def stock_pe_init(self):
        pass
if __name__ == "__main__":
    obj = SIGNAL()