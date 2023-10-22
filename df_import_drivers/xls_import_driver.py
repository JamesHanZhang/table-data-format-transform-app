"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

import pandas as pd

# self-made modules
from df_import_drivers.df_import_driver import DfImportDriver
from across_process import SysLog

class XlsImportDriver(DfImportDriver):
    def __init__(self):
        super().__init__()
        self.input_sheet = self.table_properties['params']['input_sheet']
    def init_xls_reader_params(self, input_file, input_path, input_sheet):
        if input_path != "":
            self.input_path = input_path
        if input_sheet != "":
            self.input_sheet = input_sheet

        full_input_path = self.iom.join_path(self.input_path, input_file)
        self.iom.check_if_file_exists(full_input_path)

        df = pd.read_excel(full_input_path, sheet_name=self.input_sheet)
        # 把所有类型转为object再次进行读取，以保证得到完整数据
        self.preserves = self.decide_df_dtypes(df)
        return full_input_path

    @SysLog().calculate_cost_time("<import from excel>")
    def fully_import_excel(self, input_file, input_path="",input_sheet=""):
        full_input_path = self.init_xls_reader_params(input_file, input_path, input_sheet)

        df = pd.read_excel(full_input_path, sheet_name=self.input_sheet, dtype=self.preserves)

        msg = "[IMPORT EXCEL]: data from {a} is fully imported via excel reading method.".format(a=full_input_path)
        self.log.show_log(msg)
        return df