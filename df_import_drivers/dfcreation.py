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

    def import_as_df(self, input_file, input_path=""):
        extension = IoMethods.get_file_extension(input_file)
        try:
            if extension == ".csv":
                impdriver = CsvImportDriver()
                df = impdriver.import_csv(input_file, input_path)
            elif extension in ['.xls', '.xlsx', '.xltx', '.xlsm', '.xlt', '.xltm','.xlam','.xla']:
                impdriver = XlsImportDriver()
                df = impdriver.import_excel(input_file, input_path)
            elif extension == ".md":
                impdriver = MdImportDriver()
                df = impdriver.import_md(input_file, input_path)
            else:
                msg = "[TypeError]: the input file type is limited in these choices:" \
                      "             [Excel, CSV, MarkDown].\n" \
                      "             And the input file {a}'s extension doesn't follow the rules of these types.\n" \
                      "".format(a=input_file)
                self.log.show_log(msg)
                raise TypeError(msg)
        except (PermissionError) as reason:
            # 确保在调用文件的时候，你并未打开它，否则报错
            msg = f"[PermissionError]: {reason}\n" \
                  f"[Explanation]: Please make sure when importing data file, you're not using the file."
            raise PermissionError(msg)
        return df