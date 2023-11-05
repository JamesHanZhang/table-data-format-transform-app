import time

from analysis_modules import default_properties as prop
from analysis_modules.params_monitor import *
from basic_operation import IoMethods
from analysis_modules.df_import_drivers import DfCreation
from analysis_modules.df_processing import BasicProcessing
from analysis_modules.df_output_drivers import DfOutput
from analysis_modules.sql_output_drivers import SqlOutput

class FormatTransformation:
    def __init__(self):
        self.params_set = prop.DEFAULT_PARAMS_SET
        self.ro = ResourcesOperation()
        self.dc = DfCreation()
        self.bp = BasicProcessing()
        self.do = DfOutput()
        self.so = SqlOutput()
    
    def get_params(self, params_set:str="", overwrite:bool=True) -> tuple[ImportParams, OutputParams, BasicProcessParams]:
        if params_set != "":
            self.params_set = params_set
        if overwrite is True:
            import_params, output_params, basic_process_params = IntegrateParams.get_params_from_settings(self.params_set)
        else:
            import_params, output_params, basic_process_params = IntegrateParams.get_params_from_resources(self.params_set)
        
        return import_params, output_params, basic_process_params
    
    def activate_based_on_activation(self, output_file:str="", based_on_activation:bool=None):
        """
        :param output_file: 判断是按照导出文件的拓展名来判断导出，还是 <按照激活功能activation来判断导出>
        :param based_on_activation: 直接根据设置判断是否激活 <按照激活功能activation来判断导出>
        :return:
        """
        self.based_on_activation = True
        if IoMethods.get_file_extension(self.output_params.output_file) != "":
            self.based_on_activation = False
        if IoMethods.get_file_extension(output_file) != "":
            self.based_on_activation = False
        if based_on_activation is not None and type(based_on_activation) is bool:
            self.based_on_activation = based_on_activation
        return
    
    def reset_params(self, params_set=prop.DEFAULT_PARAMS_SET, overwrite=True, input_file="", input_path="", output_file="", output_path="",table_name="", if_batch:bool=None,
                     if_circular:bool=None, if_sep:bool=None, only_one_chunk:bool=None, chunksize:int=0, based_on_activation:bool=None):
        """
        :param params_set: .json的参数表名称, 不加拓展名
        :param overwrite: true: 适用_params_setting.py的参数, 生成/覆盖同名参数表; false: 直接引用同名参数表的参数
        :param input_file: 导入文件
        :param input_path: 导入路径
        :param output_file: 导出文件名(如含拓展名, 则除非设置固定了按照激活方式, 否则自动转按照拓展名方式导出)
        :param output_path: 导出路径
        :param table_name: 导出sql的table name, 如果 based_on_activation is False, 则无效化
        :param if_batch: 是否根据文件夹批量导入(使得导入文件名无效化)
        :param if_circular: 是否循环导入(针对大文件)
        :param if_sep: 是否拆分(针对大文件)
        :param only_one_chunk: 是否只拆出第一个样例即可
        :param chunksize: 拆分/循环读取的各切片最大的记录条数
        :param based_on_activation: true: 按照激活参数执行数据导出; false: 按照导出文件的拓展名执行数据导出; .sql文件的主体自动转为table name
        :return: 修订公用参数并保存到.json参数表
        """
        self.check_params_set(params_set, overwrite)
        self.import_params, self.output_params, self.basic_process_params = self.get_params(params_set, overwrite)
        self.activate_based_on_activation(output_file, based_on_activation)
        
        if IoMethods.get_file_extension(input_file) != "":
            self.import_params.input_file = input_file
        if input_path != "" and os.path.isabs(input_path) is True:
            self.import_params.input_path = input_path
        if if_batch is not None and type(if_batch) is bool:
            self.import_params.batch_import_params.if_batch = if_batch
        if if_circular is not None and type(if_circular) is bool:
            self.import_params.if_circular = if_circular
        if chunksize > 0:
            self.import_params.chunksize = chunksize
            self.output_params.chunksize = chunksize
        if table_name != "":
            self.output_params.sql_output_params.table_name = table_name
        if output_path != "" and os.path.isabs(output_path) is True:
            self.output_params.output_path = output_path
        if if_sep is not None and type(if_sep) is bool:
            self.output_params.if_sep = if_sep
        if only_one_chunk is not None and type(only_one_chunk) is bool:
            self.output_params.only_one_chunk = only_one_chunk
            
        if self.based_on_activation is True:
            if output_file != "":
                self.output_params.output_file = output_file
        else:
            extension = IoMethods.get_file_extension(output_file)
            if extension != "":
                self.output_params.output_file = output_file
            if extension == ".sql" and table_name == "":
                self.output_params.sql_output_params.table_name = IoMethods.get_main_file_name(output_file)
                
        
        # 保存参数
        self.import_params.store_import_params(self.params_set)
        self.output_params.store_output_params(self.params_set)
    
    def process_data(self, df, chunk_no:int=""):
        # 计算记录条数
        self.dc.count_row_num(df, self.import_params, self.params_set)
        # 执行处理过程
        df = self.bp.basic_process_data(df, self.basic_process_params)
        if self.based_on_activation is True:
            self.do.output_df_on_activation(df, self.output_params, self.params_set, chunk_no=chunk_no)
            self.so.output_sql_on_activation(df, self.output_params, self.params_set, chunk_no=chunk_no)
        else:
            self.do.output_df_on_extension(df, self.output_params, self.params_set, chunk_no=chunk_no)
            self.so.output_sql_on_extension(df, self.output_params, self.params_set, chunk_no=chunk_no)
        return
    
    def run_processing_chunk_reader(self):
        chunk_reader = self.dc.fully_import_data(self.import_params, self.params_set)
        chunk_no = 0
        for chunk in chunk_reader:
            self.process_data(chunk, chunk_no)
            chunk_no += 1
        return
    
    def run_processing_whole_data(self):
        df = self.dc.import_one_file_on_extension(self.import_params, self.params_set)
        self.process_data(df)
        return
    
    def check_params_set(self, params_set, overwrite=False):
        """
        :return: 判断参数表是否存在, 仅在overwrite is False的时候生效, 因为要引用参数表
        """
        if overwrite is True:
            return
        if params_set == "":
            params_set = self.params_set
        all_params_sets = self.ro.list_resources()
        if params_set not in all_params_sets:
            msg = f"parameters set {params_set} doesn't exist under the folder resources, so you can't import the parameters directly.\n" \
                  f"please use 'overwrite = True' instead, it can create parameters set based on _params_setting.py."
            print(msg)
            time.sleep(3)
            raise FileNotFoundError(msg)
        
    
    def run_based_on_params_set(self, params_set=prop.DEFAULT_PARAMS_SET, overwrite=True, based_on_activation:bool=None, if_multi=False):
        """
        主程序入口
        :param params_set: .json的参数表名称, 不加拓展名
        :param overwrite: true: 适用_params_setting.py的参数, 生成/覆盖同名参数表; false: 直接引用同名参数表的参数
        :param based_on_activation: 判断是按照导出文件的拓展名来判断导出，还是按照激活功能activation来判断导出
        :param if_multi: 看是不是批量跑多个参数表的程序
        """
        self.check_params_set(params_set, overwrite)
        
        if if_multi is False:
            # 判断是否这个函数只跑一次
            start_time = start_program()
            
        # 执行程序
        self.import_params, self.output_params, self.basic_process_params = self.get_params(params_set, overwrite)
        self.activate_based_on_activation(based_on_activation=based_on_activation)
        if_batch = self.import_params.batch_import_params.if_batch
        if_circular = self.import_params.if_circular
        
        if if_batch is False and if_circular is False:
            # 单文件全量读取并操作
            self.run_processing_whole_data()
        else:
            # 批量导入/单文件循环读取
            self.run_processing_chunk_reader()
            
        if if_multi is False:
            end_program(start_time)
    
    def run_multi_params_sets(self, params_sets:list[str], overwrite=False, based_on_activation=True):
        """
        :param params_sets: 把参数名称集中起来跑，常用来跑不同文件的批量
        :param overwrite: 因为是不同文件的批量，肯定不能直接拿setting来填, 所以要引入文件参数表
        :param based_on_activation: 判断是按照导出文件的拓展名来判断导出，还是按照激活功能activation来判断导出
        """
        start_time = start_program()
        for params_set in params_sets:
            # 先判断有没有参数表
            self.check_params_set(params_set, overwrite)
            
        for params_set in params_sets:
            # 再执行参数表
            self.run_based_on_params_set(params_set, overwrite, based_on_activation, if_multi=True)
        end_program(start_time)
        
if __name__ == "__main__":
    # 单元测试
    ft = FormatTransformation()
    ft.run_based_on_params_set(overwrite=True, based_on_activation=True)