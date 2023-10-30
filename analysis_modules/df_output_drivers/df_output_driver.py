"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""
import pandas as pd
import math
# self-made modules
from analysis_modules.params_monitor import ResourcesOperation, SysLog, OutputParams
from basic_operation import IoMethods
from analysis_modules.df_processing import NullProcessing


class DfOutputDriver(object):
    def __init__(self, output_params: OutputParams):
        self.log = SysLog()
        self.output_path = output_params.output_path
        self.output_encoding = output_params.output_encoding
        self.overwrite = output_params.overwrite
        self.if_sep = output_params.if_sep
        self.chunksize = output_params.chunksize
        self.only_one_chunk = output_params.only_one_chunk

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

    def count_sep_num(self, df:pd.DataFrame) -> int:
        # 将DF拆分成多个小excel或者csv
        size = df.index.size
        pieces_count = math.ceil(size / self.chunksize)
        return pieces_count

    def get_nth_chunk_df(self, df: pd.DataFrame, nth_chunk: int) -> pd.DataFrame:
        # df.iloc[0:3,:]涵盖的index=[0,1,2]，df.iloc[3:4,:]涵盖的index=[3]
        # 且df.iloc[500:1000,:]如果index超过500但不足1000，则会把超过500的都截取到
        nth_chunk_df = df.iloc[nth_chunk*self.chunksize:(nth_chunk+1)*self.chunksize, :]
        return nth_chunk_df

    def get_nth_chunk_full_output_path(self, output_file: str, nth_chunk: int, extension_type: str) -> str:

        main_file_name = self.iom.get_main_file_name(output_file)
        new_output_path = self.iom.join_path(self.output_path, main_file_name)
        self.iom.mkdir_if_no_dir(new_output_path)

        new_file_name = main_file_name + "_" + str(nth_chunk)
        new_file_name = self.set_file_extension(new_file_name, extension_type)

        full_output_path = self.iom.join_path(new_output_path, new_file_name)
        return full_output_path