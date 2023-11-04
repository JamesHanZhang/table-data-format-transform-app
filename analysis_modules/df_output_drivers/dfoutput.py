"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""
import pandas as pd
# self-made modules
from analysis_modules import default_properties as prop
from analysis_modules.df_output_drivers.csv_output_driver import CsvOutputDriver
from analysis_modules.df_output_drivers.md_output_driver import MdOutputDriver
from analysis_modules.df_output_drivers.xls_output_driver import XlsOutputDriver
from analysis_modules.params_monitor import OutputParams
from basic_operation import IoMethods


class DfOutput(object):
    def __init__(self):
        self.csv_extensions = ['.csv']
        self.md_extensions = ['.md']
        self.xls_extensions = ['.xls', '.xlsx', '.xltx', '.xlsm', '.xlt', '.xltm', '.xlam', '.xla']
        self.supported_extensions = self.csv_extensions + self.md_extensions + self.xls_extensions
        
    def init_output_params(self, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET, if_sep:bool=None, only_one_chunk:bool=None, output_path=""):
        if if_sep is not None:
            output_params.if_sep = if_sep
        if only_one_chunk is not None:
            output_params.only_one_chunk = only_one_chunk
        if output_path != "":
            output_params.output_path = output_path
        self.output_params = output_params
        self.params_set = params_set
        self.if_sep = output_params.if_sep
        self.only_one_chunk = output_params.only_one_chunk
        self.overwrite = output_params.overwrite
        self.output_path = output_params.output_path
        self.output_params.store_output_params(self.params_set)

    def output_whole_df(self, df: pd.DataFrame, output_params: OutputParams,  output_file, output_path="", overwrite:bool=None, chunk_no:int=""):
        self.count_row_num(df, self.output_params, output_file, self.params_set)
        extension = IoMethods.get_file_extension(output_file)
        if extension in self.xls_extensions:
            output_driver = XlsOutputDriver(output_params)
            output_driver.store_df_as_excel(df, output_file, output_path=output_path, overwrite=overwrite, chunk_no=chunk_no)
        elif extension in self.csv_extensions:
            output_driver = CsvOutputDriver(output_params)
            output_driver.store_df_as_csv(df, output_file, output_path=output_path, overwrite=overwrite, chunk_no=chunk_no)
        elif extension in self.md_extensions:
            output_driver = MdOutputDriver(output_params)
            output_driver.store_df_as_md(df, output_file, output_path=output_path, overwrite=overwrite, chunk_no=chunk_no)
        else:
            msg = f"file name must contains extension {str(self.supported_extensions)} as required."
            raise NameError(msg)
        return

    def output_df_in_pieces(self, df: pd.DataFrame, output_params: OutputParams,  output_file, output_path="", only_one_chunk: bool=None):
        self.count_row_num(df, self.output_params, output_file, self.params_set)
        extension = IoMethods.get_file_extension(output_file)
        if extension in self.xls_extensions:
            output_driver = XlsOutputDriver(output_params)
            output_driver.sep_df_as_multi_excel(df, output_file, output_path=output_path, only_one_chunk=only_one_chunk)
        elif extension in self.csv_extensions:
            output_driver = CsvOutputDriver(output_params)
            output_driver.sep_df_as_multi_csv(df, output_file, output_path=output_path, only_one_chunk=only_one_chunk)
        elif extension in self.md_extensions:
            output_driver = MdOutputDriver(output_params)
            output_driver.sep_df_as_multi_md(df, output_file, output_path=output_path, only_one_chunk=only_one_chunk)
        else:
            msg = f"file name must contains extension {str(self.supported_extensions)} as required."
            raise NameError(msg)
        return
    
    def mkdir_sep_path(self, output_params: OutputParams, output_file, output_path=""):
        if output_path == "":
            output_path = output_params.output_path
        folder = IoMethods.get_main_file_name(output_file)
        output_path = IoMethods.join_path(output_path, folder)
        return output_path
    
    def count_row_num(self, df: pd.DataFrame, output_params: OutputParams, output_file, params_set:str=prop.DEFAULT_PARAMS_SET):
        extension = IoMethods.get_file_extension(output_file)
        if extension in self.xls_extensions:
            output_params.xls_output_params.output_index_size += df.index.size
        elif extension in self.csv_extensions:
            output_params.csv_output_params.output_index_size += df.index.size
        elif extension in self.md_extensions:
            output_params.md_output_params.output_index_size += df.index.size
        output_params.store_output_params(params_set)

    def output_on_extension(self, df: pd.DataFrame, output_params: OutputParams,  output_file, output_path="", chunk_no:int="", if_sep:bool=None, only_one_chunk:bool=None, params_set:str=prop.DEFAULT_PARAMS_SET):
        """
        将拆分和保存为一合并到一起
        """
        self.init_output_params(output_params, params_set, if_sep, only_one_chunk, output_path)
        if self.if_sep is True and str(chunk_no) == "":
            self.output_df_in_pieces(df, output_params, output_file, self.output_path, self.only_one_chunk)
        elif self.if_sep is True and str(chunk_no) != "":
            # 一般是通过circular import导入，再拆分导出，这个时候导入和导出的记录条数都是chunksize，相当于小范围的整取整存
            output_path = self.mkdir_sep_path(output_params, output_file, self.output_path)
            overwrite = True
            if self.only_one_chunk is True and chunk_no > 0:
                return
            self.output_whole_df(df, output_params, output_file, output_path, overwrite, chunk_no=chunk_no)
        elif self.if_sep is False and str(chunk_no) == "":
            # 正常的整体存储
            self.output_whole_df(df, output_params, output_file, self.output_path, self.overwrite)
        elif self.if_sep is False and str(chunk_no) != "":
            # 一般是通过生成器循环读取，切片的需要叠加，最终合并为一个文件
            if chunk_no == 0:
                # 第一次循环新建，根据需求设置判断是否添加
                self.output_whole_df(df, output_params, output_file, self.output_path, self.overwrite)
            else:
                # 第二次开始直接添加到末尾
                self.output_whole_df(df, output_params, output_file, self.output_path, False)
    
    def output_on_activation(self, df: pd.DataFrame, output_params: OutputParams,  output_file, output_path="", chunk_no:int="", if_sep:bool=None, only_one_chunk:bool=None, params_set:str=prop.DEFAULT_PARAMS_SET):
        output_file = IoMethods.get_main_file_name(output_file)
        output_files = list()
        if output_params.csv_output_params.activation is True:
            output_files.append(output_file+'.csv')
        if output_params.xls_output_params.activation is True:
            output_files.append(output_file + '.xlsx')
        if output_params.md_output_params.activation is True:
            output_files.append(output_file + '.md')
        for file in output_files:
            self.output_on_extension(df, output_params, file, output_path, chunk_no, if_sep, only_one_chunk, params_set)