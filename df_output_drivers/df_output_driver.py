"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

# self-made modules
from across_process import ResourcesOperation, SysLog, IntegratedParameters
from basic_operation import IoMethods
from df_processing import NullProcessing


class DfOutputDriver(object):
    def __init__(self):
        self.log = SysLog()
        self.table_properties = ResourcesOperation.read_default_setting()
        self.output_path = self.table_properties['params']['output_path']
        self.output_encoding = self.table_properties['basic_params']["output_encoding"]

        self.iom = IoMethods(self.output_encoding)

    def drop_empty_lines_from_df(self, df):
        df, empty_lines_count = NullProcessing.drop_empty_lines(df)
        return df

    def set_file_extension(self, file_name, type: str=".csv") -> str:
        # 规定文件拓展名
        extension = self.iom.get_file_extension(file_name)
        # 只有excel目前有多种导出拓展名
        if type == '.xlsx':
            if extension not in ['.xls', '.xlsx', '.xltx', '.xlsm', '.xlt', '.xltm', '.xlam', '.xla']:
                file_name = self.iom.get_main_file_name(file_name)
                file_name += '.xlsx'
        elif extension != type:
            file_name = self.iom.get_main_file_name(file_name)
            file_name += type
        return file_name