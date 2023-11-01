
import copy
# self-made modules
from analysis_modules import default_properties as prop
from analysis_modules.params_monitor.params_basic_setting import ParamsBasicSetting
import output_params_setting as oparams
from basic_operation import IoMethods

class OutputParams(ParamsBasicSetting):
    def __init__(self):
        super().__init__()

        # 初始化变量
        self.output_path = self.get_abspath(IoMethods.get_folder_path_under_project('output_dataset',prop.PROJECT_NAME), oparams.output_path)
        self.output_encoding = self.get_default_value(prop.DEFAULT_ENCODING, oparams.output_encoding)
        self.chunksize = oparams.chunksize # 默认OUTPUT的时候拆分成片也是按照INPUT的来算，方便过程处理
        self.if_sep = oparams.if_sep
        self.only_one_chunk = oparams.only_one_chunk
        # 只有在if_sep为False的情况下，才可以调整overwrite
        self.overwrite = oparams.overwrite if oparams.if_sep is False else True
        self.csv_output_params = self.CsvOutputParams()
        self.xls_output_params = self.XlsOutputParams()
        self.sql_output_params = self.SqlOutputParams()

    def get_output_params(self) -> dict:
        # 深拷贝: 完整将元素及嵌套的元素复制，确保没有共享引用;否则修改__dict__就是在修改该对象的元素
        params = copy.deepcopy(self.__dict__)
        params['csv_output_params'] = copy.deepcopy(self.csv_output_params.__dict__)
        params['xls_output_params'] = copy.deepcopy(self.xls_output_params.__dict__)
        params['sql_output_params'] = copy.deepcopy(self.sql_output_params.__dict__)
        return params

    def store_output_params(self, params_set: str=prop.DEFAULT_PARAMS_SET):
        # 将参数保存到json参数表内
        output_params_dict = self.get_output_params()
        self.store_params("output_params", output_params_dict, params_set)

    def load_output_params(self, params_set: str=prop.DEFAULT_PARAMS_SET):
        # 从json参数表里读取参数
        process_params = self.read_process_params(params_set)
        output_params = process_params['output_params']
        self.output_path = output_params['output_path']
        self.output_encoding = output_params['output_encoding']
        self.chunksize = output_params['chunksize']
        self.if_sep = output_params['if_sep']
        self.only_one_chunk = output_params['only_one_chunk']
        self.overwrite = output_params['overwrite']

        # csv params
        self.csv_output_params.output_sep = output_params['csv_output_params']['output_sep']
        self.csv_output_params.repl_to_sub_sep = output_params['csv_output_params']['repl_to_sub_sep']

        # xls params
        self.xls_output_params.output_sheet = output_params['xls_output_params']['output_sheet']

        # sql params
        self.sql_output_params.table_name = output_params['sql_output_params']['table_name']
        self.sql_output_params.table_structure = output_params['sql_output_params']['table_structure']
        self.sql_output_params.database = output_params['sql_output_params']['database']
        self.sql_output_params.database_options = output_params['sql_output_params']['database_options']
        self.sql_output_params.repl_to_sub_comma = output_params['sql_output_params']['repl_to_sub_comma']

    class CsvOutputParams:
        def __init__(self):
            self.output_sep = oparams.csv_output_params['output_sep']
            self.repl_to_sub_sep = oparams.csv_output_params['repl_to_sub_sep']

    class XlsOutputParams:
        def __init__(self):
            self.output_sheet = oparams.xls_output_params['output_sheet']

    class SqlOutputParams:
        def __init__(self):
            self.table_name = oparams.sql_output_params['table_name']
            self.table_structure = oparams.sql_output_params['table_structure']
            self.database = oparams.sql_output_params['database']
            self.database_options = oparams.sql_output_params['database_options']
            self.repl_to_sub_comma = oparams.sql_output_params['repl_to_sub_comma']

