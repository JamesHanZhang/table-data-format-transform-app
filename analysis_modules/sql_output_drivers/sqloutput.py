
import pandas as pd
# self-made modules
from basic_operation import IoMethods
import analysis_modules.default_properties as prop
from analysis_modules.params_monitor import OutputParams
from analysis_modules.sql_output_drivers.oracle_output_driver import OracleOutputDriver
from analysis_modules.sql_output_drivers.mysql_output_driver import MySqlOutputDriver
from analysis_modules.sql_output_drivers.gbase_output_driver import GBaseOutputDriver
from analysis_modules.sql_output_drivers.postgresql_output_driver import PostgreSqlOutputDriver
from analysis_modules.sql_output_drivers.sqlserver_output_driver import SqlServerOutputDriver

class SqlOutput:
    def __init__(self):
        pass
    
    def mkdir_sep_path(self, output_params: OutputParams, output_file, output_path=""):
        if output_path == "":
            output_path = output_params.output_path
        folder = IoMethods.get_main_file_name(output_file)
        output_path = IoMethods.join_path(output_path, folder)
        IoMethods.mkdir_if_no_dir(output_path)
        return output_path
    
    def init_output_params(self, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET, table_name="",
                           output_encoding="", if_sep:bool=None, only_one_chunk:bool=None):
        self.database = output_params.sql_output_params.database
        if if_sep is not None:
            output_params.if_sep = if_sep
        if only_one_chunk is not None:
            output_params.only_one_chunk = only_one_chunk
        if table_name != "":
            output_params.sql_output_params.table_name = table_name
        if output_encoding != "":
            output_params.output_encoding = output_encoding
        
        self.table_name = output_params.sql_output_params.table_name
        self.if_sep = output_params.if_sep
        self.overwrite = output_params.overwrite
        self.only_one_chunk = output_params.only_one_chunk
        self.output_encoding = output_params.output_encoding
        self.output_index_size = output_params.sql_output_params.output_index_size
        
        if self.database == "Oracle":
            self.sql_out_driver = OracleOutputDriver(output_params, params_set)
        elif self.database == "MySql":
            self.sql_out_driver = MySqlOutputDriver(output_params, params_set)
        elif self.database == "GBase":
            self.sql_out_driver = GBaseOutputDriver(output_params, params_set)
        elif self.database == "PostgreSql":
            self.sql_out_driver = PostgreSqlOutputDriver(output_params, params_set)
        elif self.database == "SqlServer":
            self.sql_out_driver = SqlServerOutputDriver(output_params, params_set)
        else:
            raise TypeError("[TypeError] the database you choose didn't follow the rule in the list of database choices.")
        output_params.store_output_params(params_set)
        
    def store_output_params(self, output_params: OutputParams, params_set:str, table_name, output_path, output_encoding):
        if table_name != "":
            output_params.sql_output_params.table_name = table_name
        output_params.store_output_params(params_set)
    
    def output_as_sql(self, df: pd.DataFrame, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET, table_name="", output_path="",
                      output_encoding="", overwrite=None, chunk_no:int="", base_num=0):
        self.init_output_params(output_params, params_set, table_name=table_name, output_encoding=output_encoding)
        self.sql_out_driver.store_df_as_sql(df, table_name, output_path, output_encoding, overwrite, chunk_no, base_num)
        return
    
    def output_as_sql_in_pieces(self, df: pd.DataFrame, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET,
                                table_name="", output_path="", output_encoding="", only_one_chunk:bool=None):
        self.init_output_params(output_params, params_set, table_name=table_name, output_encoding=output_encoding, only_one_chunk=only_one_chunk)
        self.sql_out_driver.sep_df_as_multi_sql(df, table_name, output_path, output_encoding, only_one_chunk)
        return
    
    def count_row_num(self, df:pd.DataFrame, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET):
        self.base_num = output_params.sql_output_params.output_index_size
        output_params.sql_output_params.output_index_size += df.index.size
        output_params.store_output_params(params_set)
    
    def output_as_sql_control(self, df: pd.DataFrame, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET,
                              table_name="", output_path="", if_sep:bool=None, chunk_no:int="", only_one_chunk=None):
        """
        :param if_sep: 判断是否切割, 通常如何导入if_circular=True，需要切割的时候，则这个在调用的时候为False，实际为True
        :param chunk_no: 用来在循环读取的时候给文件切片保存的时候修改导出名词，如为""，表示不涉及拆分
        :param base_num: 未开始时已导出的数据记录条数，用于多个文件的导入导出
        :param only_one_chunk: 是否只截取一个样例
        """
        self.init_output_params(output_params, params_set, table_name=table_name, if_sep=if_sep, only_one_chunk=only_one_chunk)
        self.count_row_num(df, output_params, params_set)
        if self.if_sep is True and str(chunk_no) == "":
            # 整个文件切分
            output_path = self.mkdir_sep_path(output_params, self.table_name, output_path)
            self.output_as_sql_in_pieces(df, output_params, params_set, table_name, output_path, self.output_encoding, self.only_one_chunk)
        elif self.if_sep is True and str(chunk_no) != "":
            # 循环导入，循环切片导出
            if self.only_one_chunk is True and chunk_no > 0:
                # 取样例只取第一个
                return
            output_path = self.mkdir_sep_path(output_params, self.table_name, output_path)
            self.output_as_sql(df, output_params, params_set, self.table_name, output_path, overwrite=True, chunk_no=chunk_no, base_num=self.base_num)
        elif self.if_sep is False and str(chunk_no) == "":
            # 整取整存
            self.output_as_sql(df, output_params, params_set, self.table_name, output_path, overwrite=self.overwrite, chunk_no=chunk_no, base_num=self.base_num)
        elif self.if_sep is False and str(chunk_no) != "":
            # 循环读取，循环存，合并为一
            if chunk_no == 0:
                self.output_as_sql(df, output_params, params_set, self.table_name, output_path, overwrite=self.overwrite, chunk_no=chunk_no, base_num=self.base_num)
            else:
                self.output_as_sql(df, output_params, params_set, self.table_name, output_path, overwrite=False, chunk_no=chunk_no, base_num=self.base_num)
    
    def output_as_sql_on_activation(self, df: pd.DataFrame, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET,
                              table_name="", output_path="", if_sep:bool=None, chunk_no:int="", only_one_chunk=None):
        if output_params.sql_output_params.activation is True:
            self.output_as_sql_control(df, output_params, params_set, table_name, output_path, if_sep, chunk_no, only_one_chunk)