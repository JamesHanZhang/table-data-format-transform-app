"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

import pandas as pd
from tqdm import tqdm
# self-made modules
from analysis_modules.df_output_drivers.df_output_driver import DfOutputDriver
from analysis_modules.params_monitor import SysLog, OutputParams
from basic_operation import IoMethods

class XlsOutputDriver(DfOutputDriver):
    def __init__(self, output_params: OutputParams):
        super().__init__(output_params)
        self.output_sheet = output_params.xls_output_params.output_sheet

    def store_as_excel(self, df, full_output_path, overwrite: bool):
        # 去掉空行
        df = self.drop_empty_lines_from_df(df)
        # 根据overwrite判断是添加到原有文件，还是重写，当overwrite==True，则重写，False则添加到原文件下
        if overwrite is True or self.iom.check_if_file_exists(full_output_path, False) is False:
            df.to_excel(full_output_path, sheet_name=self.output_sheet, index=False, engine='openpyxl', float_format='%f')
        elif overwrite is False:
            # 使用ExcelWriter目前尚且不稳定，容易出现数据遗失的问题，故暂时搁置，等待未来稳定后再采用该方法
            writer = pd.ExcelWriter(full_output_path, engine='openpyxl', mode='a', if_sheet_exists="overlay")
            df.to_excel(writer, sheet_name=self.output_sheet, startrow=writer.sheets[self.output_sheet].max_row,
                        index=False, header=False, float_format='%f')
            # writer.save()
            writer.close()
        return

    def init_xls_output_params(self, output_path, output_encoding, output_sheet):
        if output_path != "":
            self.output_path = output_path
        if output_encoding != "":
            self.output_encoding = output_encoding
        if output_sheet != "":
            self.output_sheet = output_sheet

        IoMethods.mkdir_if_no_dir(self.output_path)
        return

    @SysLog().calculate_cost_time("<store as excel>")
    def store_df_as_excel(self, df, output_file, output_path="", output_encoding="", output_sheet="", overwrite:bool=None, chunk_no:int=""):
        """
        :param chunk_no: 如果是循环读取且带切片，可以根据这个值直接生成多个文件名
        """
        self.init_xls_output_params(output_path, output_encoding, output_sheet)
        if overwrite is not None:
            self.overwrite = overwrite

        type = '.xlsx'
        output_file = self.set_file_extension(output_file, str(chunk_no), type)
        full_output_path = self.iom.join_path(self.output_path, output_file)

        self.store_as_excel(df, full_output_path, self.overwrite)
        msg = "[EXCEL OUTPUT]: file created: {a}".format(a=full_output_path)
        self.log.show_log(msg)
        return

    @SysLog().calculate_cost_time("<store as excel in pieces>")
    def sep_df_as_multi_excel(self, df: pd.DataFrame, output_file: str, output_path="", output_encoding="", output_sheet="", only_one_chunk=None):
        self.init_xls_output_params(output_path, output_encoding, output_sheet)
        if only_one_chunk is not None:
            self.only_one_chunk = only_one_chunk

        pieces_count = self.count_sep_num(df)
        # 当切片数量只有1的时候，默认直接转正常存储
        if pieces_count == 1:
            self.store_df_as_excel(df, output_file, output_path, output_encoding, output_sheet)
            return
        for nth_chunk in tqdm(range(pieces_count),position=True,leave=True,desc="creating separation of excel..."):
            nth_chunk_df = self.get_nth_chunk_df(df, nth_chunk)
            nth_full_path = self.get_nth_chunk_full_output_path(output_file, nth_chunk, '.xlsx')
            self.store_as_excel(nth_chunk_df, nth_full_path, overwrite=True)

            if self.only_one_chunk is True and nth_chunk == 0:
                self.log.show_log(f"[ONLY ONE CHUNK AS EXAMPLE]: file created: {nth_full_path}")
                break
            else:
                self.log.show_log(f"[EXCEL SEPARATION OUTPUT]: file created: {nth_full_path}")
        return