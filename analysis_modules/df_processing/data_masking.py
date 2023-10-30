"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

from tqdm import tqdm
import pandas as pd
import math
# self-made modules
from analysis_modules.params_monitor import SysLog
from analysis_modules import default_properties as prop


class DataMasking:
    def __init__(self):
        self.null_list = prop.NULL_LIST

    def sub_asterisk(self, df, col):
        row_num = df.index.size
        # 当调用chunk时，index不变会导致位置往后推移，无法选择相对位置的数值，所以要重设index
        df = df.reset_index(drop=True)
        replacement = "*"
        for row in range(row_num):
            element = df[col][row]
            if element in self.null_list or pd.isna(element):
                continue
            if len(element) == 1:
                df[col][row] = replacement
                continue
            pos = math.ceil(len(element) / 3)
            new_element = element[:pos] + replacement * len(element[pos:])
            df[col][row] = new_element
        return df

    @classmethod
    @SysLog().calculate_cost_time("<data masking>")
    def data_masking(cls, df, simple_repl_cols, masking_type="simple"):
        dma = cls()
        """
        :param df: dataframe
        :param simple_repl_cols: list type, columns need data masking
        :return: dataframe
        """
        # 简单替换脱敏
        for col in tqdm(simple_repl_cols, desc="data masking for each column..."):
            if masking_type == "simple":
                df = dma.sub_asterisk(df, col)
        SysLog.show_log(
            "[DATA MASKING]: data masking for certain columns {0} is finished.".format(str(simple_repl_cols)))
        return df
