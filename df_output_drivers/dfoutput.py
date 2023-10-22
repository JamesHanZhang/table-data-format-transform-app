"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

# self-made modules
from df_output_drivers.csv_output_driver import CsvOutputDriver
from df_output_drivers.md_output_driver import MdOutputDriver
from df_output_drivers.xls_output_driver import XlsOutputDriver
from across_process import ResourcesOperation
from basic_operation import IoMethods


class DfOutput(object):
    def __init__(self):
        self.table_properties = ResourcesOperation.read_default_setting()
        self.output_path = self.table_properties['params']['output_path']
    def output_on_extension(self, df, output_file, output_path="", overwrite=True):
        if output_path != "":
            self.output_path = output_path

        extension = IoMethods.get_file_extension(output_file)
        if extension in ['.xls', '.xlsx', '.xltx', '.xlsm', '.xlt', '.xltm','.xlam','.xla']:
            output_driver = XlsOutputDriver()
            output_driver.store_all_as_excel(df, output_file, output_path=self.output_path, overwrite=overwrite)
        elif extension == ".csv":
            output_driver = CsvOutputDriver()
            output_driver.store_all_as_csv(df, output_file, output_path=self.output_path, overwrite=overwrite)
        elif extension == ".md":
            output_driver = MdOutputDriver()
            output_driver.store_all_as_md(df, output_file, output_path=self.output_path, overwrite=overwrite)
        else:
            msg = "file name must contains extension as required."
            raise NameError(msg)
        return