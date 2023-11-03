"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""
import pandas as pd
# self-made modules
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

    def output_whole_df(self, df: pd.DataFrame, output_params: OutputParams,  output_file, output_path="", overwrite:bool=None, chunk_no:int=""):
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

    def output_on_extension(self, df: pd.DataFrame, output_params: OutputParams,  output_file, output_path="", if_sep: bool=None, overwrite=None, only_one_chunk=None):
        if if_sep is None:
            if_sep = output_params.if_sep
        if if_sep is True:
            self.output_df_in_pieces(df, output_params, output_file, output_path, only_one_chunk)
        else:
            self.output_whole_df(df, output_params, output_file, output_path, overwrite)
        return