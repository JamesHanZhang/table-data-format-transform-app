"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

from df_import_drivers.xls_import_driver import XlsImportDriver
from df_import_drivers.csv_import_driver import CsvImportDriver
from df_import_drivers.md_import_driver import MdImportDriver
from basic_operation import IoMethods
from across_process import SysLog

class DfCreation:
    def __init__(self):
        self.log = SysLog()

    def check_extension(self, input_file: str) -> str:
        extension = IoMethods.get_file_extension(input_file)
        if extension not in ['.csv', '.md', '.xls', '.xlsx', '.xltx', '.xlsm', '.xlt', '.xltm', '.xlam', '.xla']:
            msg = "[TypeError]: the input file type is limited in these choices:" \
                  "             [Excel, CSV, MarkDown].\n" \
                  "             And the input file {a}'s extension doesn't follow the rules of these types.\n" \
                  "".format(a=input_file)
            self.log.show_log(msg)
            raise TypeError(msg)
        return extension

    def import_as_df(self, input_file, input_path):
        extension = self.check_extension(input_file)
        if extension == ".csv":
            impdriver = CsvImportDriver()
            df = impdriver.fully_import_csv(input_file, input_path)
        elif extension in ['.xls', '.xlsx', '.xltx', '.xlsm', '.xlt', '.xltm', '.xlam', '.xla']:
            impdriver = XlsImportDriver()
            df = impdriver.fully_import_excel(input_file, input_path)
        elif extension == ".md":
            impdriver = MdImportDriver()
            df = impdriver.fully_import_md(input_file, input_path)
        return df

    def import_as_df_generator(self, input_file, input_path):
        extension = self.check_extension(input_file)
        circular_reading_types = ['.csv']
        if extension == ".csv":
            impdriver = CsvImportDriver()
            chunk_reader = impdriver.circular_import_csv(input_file, input_path)
        else:
            msg = f"only {str(circular_reading_types)} can be imported as generator for processing data piece by piece."
            raise TypeError(msg)
        return chunk_reader


    def import_on_extension(self, input_file, input_path="", if_circular = False):
        if if_circular is False:
            df = self.import_as_df(input_file, input_path)
            return df
        else:
            chunk_reader = self.import_as_df_generator(input_file, input_path)
            return chunk_reader