
import pandas as pd
import math
import numpy as np
from tqdm import tqdm
# self-made modules
import analysis_modules.default_properties as prop
from analysis_modules.params_monitor import ResourcesOperation, SysLog, OutputParams
from basic_operation import IoMethods
from analysis_modules.df_processing import NullProcessing
from analysis_modules.df_output_drivers.df_output_driver import DfOutputDriver


class SqlOutputDriver(DfOutputDriver):
    def __init__(self, output_params: OutputParams, params_set: str=prop.DEFAULT_PARAMS_SET):
        super().__init__(output_params)
        self.params_set = params_set

        # SQL导出参数
        self.output_params = output_params
        self.table_name = output_params.sql_output_params.table_name
        self.table_comment = output_params.sql_output_params.table_comment
        self.table_structure = output_params.sql_output_params.table_structure
        self.column_comments = output_params.sql_output_params.column_comments
        self.database = output_params.sql_output_params.database
        self.database_options = output_params.sql_output_params.database_options
        self.repl_to_sub_comma = output_params.sql_output_params.repl_to_sub_comma
        
        # 默认设置的基本参数
        self.commit_range = 1000

        # 实例化
        self.nulp = NullProcessing()
        
        # 不同数据库的从pandas的类型到数据库的类型的转换
        # 需要依据数据库的不同，重写的变量
        self.db_types_format = {
            'category': '',
            'object': '',
            'int64': '',
            'int32': '',
            'int16': '',
            'int8': '',
            'float64': '',
            'float32': '',
            'bool': '',
            'datetime64': '',
            'timedelta64': ''
        }
        self.var_list = ['category', 'object']
        self.int_list = ['int64','int32','int16','int8']
        self.float_list = ['float64', 'float32']
        self.bool_list = ['bool']
        self.time_list = ['datetime64', 'timedelta64']
        
    
    def get_date_format_element(self, element, col, col_type):
        """
        需要依据数据库的不同，重写的函数
        如果元素为datetime64的时候，元素在sql里应该怎么写，如不重写，默认返回字符串
        """
        return f"\'{str(element)}\'"
    
    def get_commit_line(self):
        """
        需要依据数据库的不同，重写的函数
        判断是否加入"COMMIT;\n" 毕竟大部分数据库都是自动提交模式
        """
        commit_line = ""
        return commit_line
    
    def construct_creation_cmd(self, columns:list[str], dtypes: dict[str, type]) -> str:
        """
        需要依据数据库的不同，重写的函数
        根据table_name, table_structure, 各列类型进行重写
        :return: 构建建表语句的内容
        """
        return ""
    

    def element_processing(self, element, col, col_type):
        element = self.nulp.replace_null_value(element)
        num_types = self.int_list + self.float_list + self.bool_list
        if element == "NULL":
            return element
        elif col_type in num_types:
            return element
        # 时间的处理
        elif col_type in self.time_list:
            element = self.get_date_format_element(element, col, col_type)
        
        # 字符串的处理，替换掉逗号，以免影响最终数据
        element = element.replace(',', self.repl_to_sub_comma)
        element = f"\'{element}\'"
        return element
        
    def get_row_values(self, df: pd.DataFrame, row: int, columns: list[str], dtypes: dict[str, str]) -> list[str]:
        df_line = df.iloc[row]
        values = list()
        for col in columns:
            element = self.element_processing(df_line[col], col, str(dtypes[col]))
            values.append(element)
        return values
    
    
    def add_commit_symbol_in_lines(self, collected_sql:str, row:int, base_num:int)->str:
        """
        :return: 为插入语句加注释，以及为ORACLE加commit语句来提交事务，其他的数据库都是默认自动提交模式的
        """
        if (row+1)%self.commit_range == 0:
            start_row = base_num + row + 1 - self.commit_range
        else:
            start_row = base_num + row + 1 - row % self.commit_range
        end_row = base_num + row + 1
        desc_eng = f"\n-- {self.database}: table <{self.table_name}>: sql table insertion commands from row {str(start_row)} to row {str(end_row)}\n"
        desc_chn = f"-- {self.database}: 表 <{self.table_name}>: 从第{str(start_row)}条数据到第{str(end_row)}条数据的插入语句\n"
        commit_line = self.get_commit_line()
        collected_sql = desc_eng + desc_chn + collected_sql + commit_line
        return collected_sql

    def create_table_insert_sql(self, df: pd.DataFrame, full_output_path: str, overwrite: bool, base_num: int):
        """
        :param df: 可能是chunk of df
        :param base_num: 前一个chunk的总记录条数
        创建sql的基础方法
        """
        # 跳过空行
        df, empty_lines_count = self.nulp.drop_empty_lines(df)
        # 当调用chunk时，index不变会导致位置往后推移，无法选择相对位置的数值，所以要重设index
        df = df.reset_index(drop=True)
        # 列名
        columns = df.columns.tolist()
        columns_to_insert = ','.join(columns)
        # 列类型
        dtypes = df.dtypes.to_dict()

        # 看是否需要覆盖
        self.iom.store_file(full_output_path, content="", encoding=self.output_encoding, overwrite=overwrite)

        row_num = df.index.size
        collected_sql = ""
        for row in tqdm(range(row_num),position=0,leave=True,desc="process for table insert sql creation..."):
            one_row_value = self.get_row_values(df, row, columns, dtypes)
            values_to_insert = ','.join(one_row_value)
            insert_sql = f"INSERT INTO {self.table_name} ({columns_to_insert}) \nVALUES({values_to_insert});\n"
            collected_sql += insert_sql
            if (row+1) % self.commit_range == 0:
                collected_sql = self.add_commit_symbol_in_lines(collected_sql, row, base_num)
                self.iom.store_file(full_output_path, content=collected_sql, encoding=self.output_encoding, overwrite=False)
                collected_sql = ""
        # 防止出现刚好为等于commit_range，以至于重复录入的情况
        if row_num % self.commit_range != 0:
            collected_sql = self.add_commit_symbol_in_lines(collected_sql, row_num-1, base_num)
            self.iom.store_file(full_output_path, content=collected_sql, encoding=self.output_encoding, overwrite=False)
        return
    
    def get_column_max_length(self, df: pd.DataFrame) -> dict[str, int]:
        columns = df.columns.tolist()
        col_max_len = dict.fromkeys(set(columns), 0)
        for col in columns:
            # 只能针对str格式进行最长长度提取
            series = df[col].apply(str)
            # 中文的长度计算要采用unicode码值长度计算，而非直接计算，所以引入encode('utf-8')，否则结论错误
            series = series.str.encode('utf-8')
            max_len =series.str.len().max()
            # 默认最少为1
            if np.isnan(max_len):
                max_len = 1
            max_len = int(max_len)
            col_max_len[col] = max_len
        return col_max_len
    
    def create_table_remark(self, columns):
        remark_sql = ""
        if self.table_comment != "":
            remark_sql = f"\nCOMMENT ON TABLE {self.table_name} IS '{self.table_comment}';\n"
        for col in columns:
            try:
                remark_sql += f"COMMENT ON COLUMN {self.table_name}.{col} IS '{self.column_comments[col]}';\n"
            except (KeyError):
                continue
        return remark_sql
    
    @SysLog().calculate_cost_time("<store as sql table creation>")
    def create_table_creation_sql(self, df:pd.DataFrame, full_output_path:str):
        columns = df.columns.tolist()
        col_max_len = self.get_column_max_length(df)
        for col in columns:
            try:
                # 假如尚未设立就直接赋值，如已设立长度则比较
                if self.table_structure[col] < col_max_len[col]:
                    self.table_structure[col] = col_max_len[col]
            except (KeyError):
                self.table_structure[col] = col_max_len[col]
        # 结果赋值到参数表里
        self.output_params.sql_output_params.table_structure = self.table_structure
        self.output_params.store_output_params(self.params_set)
        dtypes = df.dtypes.to_dict()
        table_creation_sql = self.construct_creation_cmd(columns, dtypes)
        self.iom.store_file(full_output_path, content=table_creation_sql, encoding=self.output_encoding, overwrite=True)
        return
    
    def init_sql_output_params(self, table_name, output_path, output_encoding):
        if table_name != "":
            table_name = IoMethods.get_main_file_name(table_name)
            self.table_name = table_name
        if output_path != "":
            self.output_path = output_path
        if output_encoding != "":
            self.output_encoding = output_encoding
        
        IoMethods.mkdir_if_no_dir(self.output_path)
        return
    
    @SysLog().calculate_cost_time("<store as sql insert commands>")
    def store_df_as_sql(self, df: pd.DataFrame, table_name="", output_path="", output_encoding="", overwrite:bool=None, chunk_no:int="", base_num:int=0):
        """
        :param table_name: 导出的表名
        :param chunk_no: 用来在循环读取的时候给文件切片保存的时候修改导出名词，如为""，表示不涉及拆分
        """
        self.init_sql_output_params(table_name, output_path, output_encoding)
        if type(overwrite) is bool:
            self.overwrite = overwrite
        
        add_part = ""
        if str(chunk_no) != "":
            # 说明是要拆分的，所以每个被拆分的文件应该是新建而非添加
            self.overwrite = True
            base_num = chunk_no * self.chunksize + base_num
            add_part = f"_{str(chunk_no)}_slice"
            
        table_creation_file = self.table_name + "_table_creation.sql"
        table_insertion_file = self.table_name + f"_table_insert{add_part}.sql"
        output_creation_path = IoMethods.join_path(self.output_path, table_creation_file)
        output_insertion_path = IoMethods.join_path(self.output_path, table_insertion_file)
        
        
        
        self.create_table_insert_sql(df, output_insertion_path, self.overwrite, base_num)
        self.create_table_creation_sql(df, output_creation_path)
        return
    
    @SysLog().calculate_cost_time("<store as sql insert commands in pieces>")
    def sep_df_as_multi_sql(self, df: pd.DataFrame, table_name="", output_path="", output_encoding="", only_one_chunk:bool=None):
        """
        :param df: 整体的dataframe，而非循环读取的dataframe
        """
        if only_one_chunk is not None:
            self.only_one_chunk = only_one_chunk

        pieces_count = self.count_sep_num(df)
        # 当切片数量只有1的时候，默认直接转正常存储
        if pieces_count == 1:
            self.store_df_as_sql(df, table_name, output_path, output_encoding)
            return
        for nth_chunk in tqdm(range(pieces_count), position=True, leave=True, desc="creating separation of sql insertion commands..."):
            nth_chunk_df = self.get_nth_chunk_df(df, nth_chunk)
            self.store_df_as_sql(nth_chunk_df, table_name, output_path, output_encoding, chunk_no=nth_chunk)
            if self.only_one_chunk is True and nth_chunk == 0:
                self.log.show_log(f"[ONLY ONE CHUNK AS EXAMPLE]: file created under the path: {self.output_path}")
                break
            else:
                self.log.show_log(f"[SQL OUTPUT] sql based on database {self.database} created under the path: {self.output_path}")
            return
        


