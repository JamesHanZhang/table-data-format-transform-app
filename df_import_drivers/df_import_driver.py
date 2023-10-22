"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

import pandas as pd
import re
import csv
import shlex

# self-made modules
from across_process import ResourcesOperation, SysLog, IntegratedParameters
from basic_operation import IoMethods
from df_processing import NullProcessing
class DfImportDriver(object):
    def __init__(self):
        # 超过15位会使用科学计数法scientific notation导致省略15位以后的数据，需要预先设置
        pd.set_option('display.float_format', '{:.2f}'.format)
        self.log = SysLog()
        self.ip = IntegratedParameters()

        self.table_properties = ResourcesOperation.read_default_setting()
        self.input_path = self.table_properties['params']['input_path']
        self.quote_as_object = self.table_properties['basic_params']['quote_as_object']
        self.input_encoding = self.table_properties['basic_params']['input_encoding']
        self.chunksize = self.table_properties['basic_params']['chunksize']

        self.iom = IoMethods(self.input_encoding)

    def drop_empty_lines_from_df(self, df):
        df, empty_lines_count = NullProcessing.drop_empty_lines(df)
        return df
    def get_preserves(self, df) -> dict[str, str]:
        """method for getting dict of dataframe columns and return it with 'object' type as indicator."""
        # read_excel在面对长数字数据时会出现数据丢失的情况，只能通过转换为string来实现完整的读取
        columns = df.columns.tolist()
        objects = ['object'] * len(columns)
        preserves = dict(zip(columns, objects))
        return preserves

    def get_df_dtypes_by_preserves(self, preserves):
        if self.quote_as_object is True:
            msg = "[WARNING]: we turned all datavalues into string type in case losing the precision of data."
            self.log.show_log(msg)
            return preserves
        else:
            return None
    def decide_df_dtypes(self, df) -> dict[str, str]|None:
        preserves = self.get_preserves(df)
        preserves = self.get_df_dtypes_by_preserves(preserves)
        return preserves

