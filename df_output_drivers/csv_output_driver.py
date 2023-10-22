"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

# self-made modules
from df_output_drivers.df_output_driver import DfOutputDriver
from across_process import SysLog
from basic_operation import IoMethods

class CsvOutputDriver(DfOutputDriver):
    def __init__(self):
        super().__init__()
        self.output_sep = self.table_properties['params']['output_sep']

    def store_as_csv(self, df, full_output_path, overwrite: bool):
        # 去掉空行
        df = self.drop_empty_lines_from_df(df)
        # 根据overwrite判断是添加到原有文件，还是重写，当overwrite==True，则重写，False则添加到原文件下
        if overwrite is True or self.iom.check_if_file_exists(full_output_path, False) is False:
            header = True
            storage_mode = 'w'
        else:
            header = False
            storage_mode = 'a'
        df.to_csv(full_output_path, mode=storage_mode, header=header, sep=self.output_sep, index=False, encoding=self.output_encoding, float_format='%f')
        return

    def init_csv_output_params(self, output_path, output_sep, output_encoding):
        if output_path != "":
            self.output_path = output_path
        if output_sep != "":
            self.output_sep = output_sep
        if output_encoding != "":
            self.output_encoding = output_encoding

        IoMethods.mkdir_if_no_dir(self.output_path)
        return

    @SysLog().calculate_cost_time("<store as csv>")
    def store_all_as_csv(self, df, output_file, output_path="", output_sep="", output_encoding="", overwrite=True):
        self.init_csv_output_params(output_path, output_sep, output_encoding)
        # 获得参数
        type = '.csv'
        output_file = self.set_file_extension(output_file, type)
        full_output_path = self.iom.join_path(self.output_path, output_file)
        self.store_as_csv(df, full_output_path, overwrite)
        msg = "[CSV OUTPUT]: file created: {a}".format(a=full_output_path)
        self.log.show_log(msg)
        return