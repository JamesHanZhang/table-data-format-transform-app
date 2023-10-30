"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

from analysis_modules.df_import_drivers.xls_import_driver import XlsImportDriver
from analysis_modules.df_import_drivers.csv_import_driver import CsvImportDriver
from analysis_modules.df_import_drivers.md_import_driver import MdImportDriver
from basic_operation import IoMethods
from analysis_modules.params_monitor import SysLog, ImportParams

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

    def check_if_circular(self, if_circular: bool, import_params: ImportParams):
        if if_circular is None:
            self.if_circular = import_params.if_circular
        elif type(if_circular) == bool:
            self.if_circular = if_circular
            import_params.if_circular = if_circular
        else:
            raise TypeError("if_circular parameter must be bool type or None!")
        return

    def import_on_extension(self, import_params: ImportParams, input_file: str, input_path="", if_circular: bool=None):
        self.check_if_circular(if_circular, import_params)
        if self.if_circular is False:
            df = self.import_as_df(import_params, input_file, input_path)
            return df
        else:
            chunk_reader = self.import_as_df_generator(import_params, input_file, input_path)
            return chunk_reader

