
import copy
# self-made modules
from analysis_modules import default_properties as prop
from analysis_modules.params_monitor.params_basic_setting import ParamsBasicSetting
import import_params_setting as iparams

class ImportParams(ParamsBasicSetting):
    def __init__(self):
        super().__init__()

        # 初始化变量
        self.input_path = self.get_abspath(prop.INPUT_PATH, iparams.input_path)
        self.input_encoding = self.get_default_value(prop.DEFAULT_ENCODING, iparams.input_encoding)
        self.quote_as_object = iparams.quote_as_object
        self.if_circular = iparams.if_circular
        self.chunksize = iparams.chunksize
        self.csv_import_params = self.CsvImportParams()
        self.xls_import_params = self.XlsImportParams()

    def get_import_params(self) -> dict:
        # 深拷贝: 完整将元素及嵌套的元素复制，确保没有共享引用;否则修改__dict__就是在修改该对象的元素
        params = copy.deepcopy(self.__dict__)
        params['csv_import_params'] = copy.deepcopy(self.csv_import_params.__dict__)
        params['xls_import_params'] = copy.deepcopy(self.xls_import_params.__dict__)
        return params

    def store_import_params(self, params_set: str=prop.DEFAULT_PARAMS_SET):
        # 将参数保存到json参数表内
        import_params_dict = self.get_import_params()
        self.store_params("import_params", import_params_dict, params_set)

    def load_import_params(self, params_set: str=prop.DEFAULT_PARAMS_SET):
        # 从json参数表里读取参数
        process_params = self.read_process_params(params_set)
        import_params = process_params['import_params']
        self.input_path = import_params['input_path']
        self.input_encoding = import_params['input_encoding']
        self.quote_as_object = import_params['quote_as_object']
        self.if_circular = import_params['if_circular']
        self.chunksize = import_params['chunksize']
        self.csv_import_params.input_sep = import_params['csv_import_params']['input_sep']
        self.csv_import_params.character_size = import_params['csv_import_params']['character_size']
        self.csv_import_params.quote_none = import_params['csv_import_params']['quote_none']
        self.csv_import_params.sep_to_sub_multi_char_sep = import_params['csv_import_params']['sep_to_sub_multi_char_sep']
        self.csv_import_params.repl_to_sub_sep = import_params['csv_import_params']['repl_to_sub_sep']
        self.xls_import_params.input_sheet = import_params['xls_import_params']['input_sheet']

    class CsvImportParams:
        def __init__(self):
            self.input_sep = iparams.csv_import_params['input_sep']
            self.character_size = iparams.csv_import_params['character_size']
            self.quote_none = iparams.csv_import_params['quote_none']
            self.sep_to_sub_multi_char_sep = iparams.csv_import_params['sep_to_sub_multi_char_sep']
            self.repl_to_sub_sep = iparams.csv_import_params['repl_to_sub_sep']

    class XlsImportParams:
        def __init__(self):
            self.input_sheet = iparams.xls_import_params['input_sheet']

