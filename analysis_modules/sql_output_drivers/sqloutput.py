
import pandas as pd
# self-made modules
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
    
    def init_output_params(self, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET):
        self.database = output_params.sql_output_params.database
        self.if_sep = output_params.if_sep
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
    
    def output_as_sql(self, df: pd.DataFrame, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET, table_name="", output_path="",
                      output_encoding="", overwrite=None, chunk_no:int="", base_num=0):
        self.init_output_params(output_params, params_set)
        self.sql_out_driver.store_df_as_sql(df, table_name, output_path, output_encoding, overwrite, chunk_no, base_num)
        return
    
    def output_as_sql_in_pieces(self, df: pd.DataFrame, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET,
                                table_name="", output_path="", output_encoding="", only_one_chunk:bool=None):
        self.init_output_params(output_params, params_set)
        self.sql_out_driver.sep_df_as_multi_sql(df, table_name, output_path, output_encoding, only_one_chunk)
        return
    
    def output_as_sql_control(self, df: pd.DataFrame, output_params: OutputParams, params_set:str=prop.DEFAULT_PARAMS_SET,
                              table_name="", output_path="", output_encoding="", if_sep:bool=None, overwrite=None,
                              chunk_no:int="", base_num=0, only_one_chunk=None):
        """
        :param if_sep: 判断是否切割, 通常如何导入if_circular=True，需要切割的时候，则这个在调用的时候为False，实际为True
        :param chunk_no: 用来在循环读取的时候给文件切片保存的时候修改导出名词，如为""，表示不涉及拆分
        :param base_num: 未开始时已导出的数据记录条数，用于多个文件的导入导出
        :param only_one_chunk: 是否只截取一个样例
        """
        if if_sep is not None:
            self.if_sep = if_sep
        
        if if_sep is True:
            self.output_as_sql_in_pieces(df, output_params, params_set, table_name, output_path, output_encoding, only_one_chunk)
        else:
            self.output_as_sql(df, output_params, params_set, table_name, output_path, output_encoding, overwrite, chunk_no, base_num)
        return
    
        