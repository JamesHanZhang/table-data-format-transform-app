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
from basic_operation import IoMethods, count_sep_num, get_nth_chunk_df
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

    def set_file_extension(self, file_name, add_part:str="", type: str=".csv") -> str:
        if add_part != "" and self.if_sep is True:
            add_part = f"_{add_part}_slice"
        
        # 规定文件拓展名
        extension = self.iom.get_file_extension(file_name)
        file_name = self.iom.get_main_file_name(file_name)
        # 只有excel目前有多种导出拓展名
        if type == '.xlsx':
            if extension in ['.xls', '.xlsx', '.xltx', '.xlsm', '.xlt', '.xltm', '.xlam', '.xla']:
                file_name = file_name + add_part + extension
                return file_name

        file_name = file_name + add_part + type
        
        return file_name

    def count_sep_num(self, df:pd.DataFrame) -> int:
        # 将DF拆分成多个小excel或者csv
        pieces_count = count_sep_num(df, self.chunksize)
        return pieces_count

    def get_nth_chunk_df(self, df: pd.DataFrame, nth_chunk: int) -> pd.DataFrame:
        nth_chunk_df = get_nth_chunk_df(df, nth_chunk, self.chunksize)
        return nth_chunk_df

    def get_nth_chunk_full_output_path(self, output_file: str, nth_chunk: int, extension_type: str) -> str:

        main_file_name = self.iom.get_main_file_name(output_file)
        new_output_path = self.iom.join_path(self.output_path, main_file_name)
        self.iom.mkdir_if_no_dir(new_output_path)

        new_file_name = self.set_file_extension(main_file_name, str(nth_chunk), extension_type)

        full_output_path = self.iom.join_path(new_output_path, new_file_name)
        return full_output_path
    
    def mkdir_sep_path(self, output_params: OutputParams, output_file, output_path=""):
        if output_path == "":
            output_path = output_params.output_path
        folder = IoMethods.get_main_file_name(output_file)
        output_path = IoMethods.join_path(output_path, folder)
        return output_path
