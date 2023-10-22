"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

import pandas as pd
# self-made modules
from df_output_drivers.df_output_driver import DfOutputDriver
from across_process import SysLog
from basic_operation import IoMethods

class XlsOutputDriver(DfOutputDriver):
    def __init__(self):
        super().__init__()
        self.output_sheet = self.table_properties['params']['output_sheet']

    def store_as_excel(self, df, full_output_path, overwrite: bool):
        # 去掉空行
        df = self.drop_empty_lines_from_df(df)
        # 根据overwrite判断是添加到原有文件，还是重写，当overwrite==True，则重写，False则添加到原文件下
        if overwrite is True or self.iom.check_if_file_exists(full_output_path, False) is False:
            df.to_excel(full_output_path, sheet_name=self.output_sheet, index=False, encoding=self.output_encoding,
                        float_format='%f')
        elif overwrite is False:
            # 使用ExcelWriter目前尚且不稳定，容易出现数据遗失的问题，故暂时搁置，等待未来稳定后再采用该方法
            writer = pd.ExcelWriter(full_output_path, engine='openpyxl', mode='a', if_sheet_exists="overlay")
            df.to_excel(writer, sheet_name=self.output_sheet, startrow=writer.sheets[self.output_sheet].max_row,
                        index=False, header=False, float_format='%f')
            # writer.save()
            writer.close()
        return

    def init_xls_output_params(self, output_path, output_sheet):
        if output_path != "":
            self.output_path = output_path
        if output_sheet != "":
            self.output_sheet = output_sheet
        IoMethods.mkdir_if_no_dir(self.output_path)
        return

    @SysLog().calculate_cost_time("<store as excel>")
    def store_all_as_excel(self, df, output_file, output_path="", output_sheet="", overwrite=True):
        self.init_xls_output_params(output_path, output_sheet)

        type = '.xlsx'
        output_file = self.set_file_extension(output_file, type)
        full_output_path = self.iom.join_path(self.output_path, output_file)

        self.store_as_excel(df, full_output_path, overwrite)
        msg = "[EXCEL OUTPUT]: file created: {a}".format(a=full_output_path)
        self.log.show_log(msg)
        return