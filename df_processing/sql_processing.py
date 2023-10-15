"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

from sqlite3 import connect
import pandas as pd

class SqlProcessing:
    def __init__(self):
        self.conn = connect(':memory:')

    def init_table(self, df, table_name):
        df.to_sql(table_name, self.conn)
        return True

    def exe_query(self, query):
        # example: query = "SELECT * FROM test"
        result = pd.read_sql(query, self.conn)
        return result