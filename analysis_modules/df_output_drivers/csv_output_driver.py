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

class CsvOutputDriver(DfOutputDriver):
    def __init__(self, output_params: OutputParams):
        super().__init__(output_params)
        self.output_sep = output_params.csv_output_params.output_sep
        self.repl_to_sub_sep = output_params.csv_output_params.repl_to_sub_sep

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
        # 通过调整 lineterminator='\n' ，使得导出的数据为LF格式，即换行符为纯粹的\n的linux模式
        df.to_csv(full_output_path, mode=storage_mode, header=header, sep=self.output_sep, index=False,
                  encoding=self.output_encoding, float_format='%f', lineterminator='\n')
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
    def store_df_as_csv(self, df: pd.DataFrame, output_file: str, output_path="", output_sep="", output_encoding="", overwrite: bool=None):
        self.init_csv_output_params(output_path, output_sep, output_encoding)
        if overwrite is not None:
            self.overwrite = overwrite

        # 获得参数
        type = '.csv'
        output_file = self.set_file_extension(output_file, type)
        full_output_path = IoMethods.join_path(self.output_path, output_file)
        self.store_as_csv(df, full_output_path, self.overwrite)
        msg = "[CSV OUTPUT]: file created: {a}".format(a=full_output_path)
        self.log.show_log(msg)
        return

    @SysLog().calculate_cost_time("<store as csv in pieces>")
    def sep_df_as_multi_csv(self, df: pd.DataFrame, output_file: str, output_path="", output_sep="", output_encoding="", only_one_chunk=None):
        self.init_csv_output_params(output_path, output_sep, output_encoding)
        if only_one_chunk is not None:
            self.only_one_chunk = only_one_chunk

        pieces_count = self.count_sep_num(df)
        # 当只需要一个切片，或者切片数量只有1的时候，默认直接转正常存储
        if pieces_count == 1:
            self.store_df_as_csv(df, output_file, output_path, output_sep, output_encoding)
            return
        for nth_chunk in tqdm(range(pieces_count),position=True,leave=True,desc="正在切片存储csv"):
            nth_chunk_df = self.get_nth_chunk_df(df, nth_chunk)
            nth_full_path = self.get_nth_chunk_full_output_path(output_file, nth_chunk, '.csv')
            self.store_as_csv(nth_chunk_df, nth_full_path, overwrite=True)

            if self.only_one_chunk is True and nth_chunk == 0:
                self.log.show_log(f"[ONLY ONE CHUNK AS EXAMPLE]: file created: {nth_full_path}")
                break
            else:
                self.log.show_log(f"[CSV SEPARATION OUTPUT]: file created: {nth_full_path}")
        return


