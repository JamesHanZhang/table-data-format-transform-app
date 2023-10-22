"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

from df_import_drivers.csv_import_driver import CsvImportDriver
from across_process import SysLog

class MdImportDriver(CsvImportDriver):
    def __init__(self):
        super().__init__()
        self.quote_as_object = True
        self.quote_none = True

    def init_md_reader_params(self, input_path, input_encoding):
        if input_path != "":
            self.input_path = input_path
        if input_encoding != "":
            self.input_encoding = input_encoding

    @SysLog().calculate_cost_time("<import from markdown(using csv reading method)>")
    def fully_import_md(self, input_file, input_path = "", input_encoding=""):
        self.init_md_reader_params(input_path, input_encoding)

        df = self.fully_import_csv(input_file, self.input_path, input_sep="|", input_encoding=self.input_encoding)
        df = df.dropna(
            axis=1,
            how='all'
        ).iloc[1:]
        df.columns = df.columns.str.strip()

        msg = f"[IMPORT MARKDOWN]: data from {input_file} is fully imported."
        self.log.show_log(msg)
        return df