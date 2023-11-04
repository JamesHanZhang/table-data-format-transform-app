"""
*****************************************
***       DATA-CLEAN-ANALYSIS-TOOL    ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""
import pandas as pd
import math
# self-made modules
from analysis_modules.df_import_drivers.xls_import_driver import XlsImportDriver
from analysis_modules.df_import_drivers.csv_import_driver import CsvImportDriver
from analysis_modules.df_import_drivers.md_import_driver import MdImportDriver
from basic_operation import *
from analysis_modules.params_monitor import SysLog, ImportParams
from analysis_modules import default_properties as prop

class DfCreation:
    def __init__(self):
        self.log = SysLog()
        self.csv_extensions = ['.csv']
        self.md_extensions = ['.md']
        self.xls_extensions = ['.xls', '.xlsx', '.xltx', '.xlsm', '.xlt', '.xltm', '.xlam', '.xla']
        self.supported_extensions = self.csv_extensions + self.md_extensions + self.xls_extensions

    def check_extension(self, input_file: str) -> str:
        extension = IoMethods.get_file_extension(input_file)
        if extension not in self.supported_extensions:
            msg = "[TypeError]: the input file type is limited in these choices:" \
                  "             [Excel, CSV, MarkDown].\n" \
                  "             And the input file {a}'s extension doesn't follow the rules of these types.\n" \
                  "".format(a=input_file)
            self.log.show_log(msg)
            raise TypeError(msg)
        return extension

    def import_as_df(self, import_params: ImportParams, input_file: str, input_path: str=""):
        extension = self.check_extension(input_file)
        if extension in self.csv_extensions:
            impdriver = CsvImportDriver(import_params)
            df = impdriver.fully_import_csv(input_file, input_path)
        elif extension in self.xls_extensions:
            impdriver = XlsImportDriver(import_params)
            df = impdriver.fully_import_excel(input_file, input_path)
        elif extension in self.md_extensions:
            impdriver = MdImportDriver(import_params)
            df = impdriver.fully_import_md(input_file, input_path)
        return df

    def import_as_df_generator(self, import_params: ImportParams, input_file: str, input_path: str=""):
        extension = self.check_extension(input_file)
        circular_reading_types = self.csv_extensions + self.xls_extensions
        if extension in self.csv_extensions:
            impdriver = CsvImportDriver(import_params)
            chunk_reader = impdriver.circular_import_csv(input_file, input_path)
        elif extension in self.xls_extensions:
            impdriver = XlsImportDriver(import_params)
            chunk_reader = impdriver.circular_import_excel(input_file, input_path)

        else:
            msg = f"only {str(circular_reading_types)} can be imported as generator for processing data piece by piece."
            raise TypeError(msg)
        return chunk_reader
    
    def init_import_params(self, import_params: ImportParams, params_set:str=prop.DEFAULT_PARAMS_SET, input_path="", if_batch:bool=None):
        self.input_path = import_params.input_path
        if input_path != "":
            self.input_path = input_path
        if if_batch is not None:
            import_params.batch_import_params.if_batch = if_batch
        
        self.if_batch = import_params.batch_import_params.if_batch
        self.if_circular = import_params.if_circular
        self.import_type = import_params.batch_import_params.import_type
        self.input_encoding = import_params.input_encoding
        self.chunksize = import_params.chunksize
        import_params.store_import_params(params_set)
        return

    def import_one_file_on_extension(self, import_params: ImportParams, input_file: str, input_path="", params_set:str=prop.DEFAULT_PARAMS_SET):
        """
        :param import_params: 参数表
        :param input_file: 导入文件名（含拓展名）
        :param input_path: 导入文件的路径
        :param if_circular: 是否循环读取
        :param params_set: 参数表的保存名称
        :return: 依据是否循环读取，返回生成器或表dataframe
        """
        self.init_import_params(import_params, params_set, input_path)
        if self.if_circular is False:
            df = self.import_as_df(import_params, input_file, input_path)
            return df
        else:
            chunk_reader = self.import_as_df_generator(import_params, input_file, input_path)
            return chunk_reader
    
    def generator_combine_multi_file_generators(self, import_params: ImportParams, input_path="", params_set:str=prop.DEFAULT_PARAMS_SET):
        """
        循环读取数据，返回以chunksize为间隔的生成器
        """
        self.init_import_params(import_params, params_set, input_path)
        path_finder = FindChildPaths(self.input_encoding)
        path_file_pairs = path_finder.gain_child_certain_type_path_file_pairs(self.input_path, self.import_type)
        
        last_df = None
        for path in path_file_pairs.keys():
            for file in path_file_pairs[path]:
                chunk_reader = self.import_as_df_generator(import_params, file, path)
                for chunk in chunk_reader:
                    df = chunk
                    if last_df is not None:
                        df = concat_dfs(last_df, df)
                    pieces = count_exact_sep_num(df, self.chunksize)
                    if pieces >= 1:
                        for nth_chunk in range(math.ceil(pieces)):
                            nth_chunk_df = get_nth_chunk_df(df, nth_chunk, self.chunksize)
                            nth_chunk_df = nth_chunk_df.reset_index(drop=True)
                            if nth_chunk_df.index.size < self.chunksize:
                                last_df = nth_chunk_df
                                break
                            yield nth_chunk_df
                        if last_df is not None and last_df.index.size == self.chunksize:
                            last_df = None
                    else:
                        last_df = df
        if last_df is not None:
            last_df = last_df.reset_index(drop=True)
            yield last_df
    
    def generator_combine_multi_file_dfs(self, import_params: ImportParams, input_path="", params_set:str=prop.DEFAULT_PARAMS_SET):
        """
        直接读取文件
        :return: 返回以chunksize间隔的生成器
        """
        self.init_import_params(import_params, params_set, input_path)
        path_finder = FindChildPaths(self.input_encoding)
        path_file_pairs = path_finder.gain_child_certain_type_path_file_pairs(self.input_path, self.import_type)
        
        last_df = None
        for path in path_file_pairs.keys():
            for file in path_file_pairs[path]:
                df = self.import_as_df(import_params, file, path)
                if last_df is not None:
                    # 如果上一个留下来了一部分，就合并
                    df = concat_dfs(last_df, df)
                pieces = count_exact_sep_num(df, self.chunksize)
                # 如果片超过了self.chunksize，就先导入
                if pieces >= 1:
                    for nth_chunk in range(math.ceil(pieces)):
                        nth_chunk_df = get_nth_chunk_df(df, nth_chunk, self.chunksize)
                        nth_chunk_df = nth_chunk_df.reset_index(drop=True)
                        if nth_chunk_df.index.size < self.chunksize:
                            # 取得最后一片
                            last_df = nth_chunk_df
                            break
                        yield nth_chunk_df
                    if last_df is not None and last_df.index.size == self.chunksize:
                        # 如果是刚好结束的情况，则报空
                        last_df = None
                else:
                    # 如果都不能凑成一整片，就放下一个循环
                    last_df = df
        if last_df is not None:
            # 将最后剩下的部分导入
            last_df = last_df.reset_index(drop=True)
            yield last_df
    
    def import_multi_files_as_generator(self, import_params: ImportParams, input_path="", params_set:str=prop.DEFAULT_PARAMS_SET):
        """
        如果一开始确定是批量，可以直接用这个函数导入(直接读取数据)，返回生成器
        """
        self.init_import_params(import_params,params_set, input_path)
        if self.if_circular is True:
            chunk_reader = self.generator_combine_multi_file_generators(import_params, input_path, params_set)
        else:
            chunk_reader = self.generator_combine_multi_file_dfs(import_params, input_path, params_set)
        return chunk_reader
    
    def circular_import_data(self, import_params: ImportParams, input_file="", input_path="", if_batch:bool=None, params_set:str=prop.DEFAULT_PARAMS_SET):
        """
        用来最终导入数据的函数
        :param import_params: 导入参数
        :param input_file: 导入文件, 如批量则不填写
        :param input_path: 导入路径
        """
        # 有多重的调用，及批量导入的子目录问题，所以只能放在最终函数这里进行路径的保存
        if input_path != "":
            import_params.input_path = input_path
        self.init_import_params(import_params, params_set, input_path, if_batch)
        
        if self.if_batch is True:
            chunk_reader = self.import_multi_files_as_generator(import_params, input_path, params_set)
            return chunk_reader
        if self.if_circular is True:
            chunk_reader = self.import_one_file_on_extension(import_params, input_file, input_path, params_set)
            return chunk_reader
        raise ImportError("[NOT CIRCULAR IMPORT] when if_batch is False and if_circular is False, it's for fully import, not circular import.")
    
    def fully_import_data(self, import_params: ImportParams, input_file="", input_path="", if_batch:bool=None, params_set:str=prop.DEFAULT_PARAMS_SET):
        # 有多重的调用，及批量导入的子目录问题，所以只能放在最终函数这里进行路径的保存
        if input_path != "":
            import_params.input_path = input_path
        self.init_import_params(import_params,params_set, input_path, if_batch)
        
        if self.if_batch is True or self.if_circular is True:
            chunk_reader = self.circular_import_data(import_params, input_file, input_path, if_batch, params_set)
            frames = list()
            for chunk in chunk_reader:
                frames.append(chunk)
            full_df = concat_dfs_list(frames)
            return full_df
        full_df = self.import_one_file_on_extension(import_params, input_file, input_path, params_set)
        return full_df