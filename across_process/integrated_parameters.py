"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

import os
# self-made modules
from basic_operation import IoMethods
import default_properties as prop
import df_properties as dfprop
from across_process.resources_operation import ResourcesOperation
from across_process.sys_log import SysLog

class IntegratedParameters:
    def __init__(self):
        self.ro = ResourcesOperation()
        self.log = SysLog()

    @SysLog().direct_show_log("[INITIALIZATION] initialize the default parameters for processing.")
    def init_params(self):
        self.input_path = self.get_abspath(prop.INPUT_PATH, dfprop.input_path)
        self.output_path = self.get_abspath(prop.OUTPUT_PATH, dfprop.output_path)
        self.basic_params = {
            "input_encoding": dfprop.INPUT_ENCODING,
            "output_encoding": dfprop.OUTPUT_ENCODING,
            "quote_none": dfprop.QUOTE_NONE,
            "quote_as_object": dfprop.QUOTE_AS_OBJECT,
            "chunksize": dfprop.CHUNKSIZE
        }
        self.params = {
            "input_sep": dfprop.input_sep,
            "output_sep": dfprop.output_sep,
            "input_sheet": dfprop.input_sheet,
            "output_sheet": dfprop.output_sheet,
            "input_path": self.input_path,
            "output_path": self.output_path
        }
        self.table_properties = {
            "params": self.params,
            "basic_params": self.basic_params
        }
        self.ro.store_default_setting(self.table_properties)
        return
    def get_abspath(self, parent_path, target_path=""):
        # target_path为选填目录，绝对路径则直接采用，相对路径则接到默认目录parent_path下
        if target_path=="":
            # 没填则为默认值
            return parent_path
        elif os.path.isabs(target_path) is True:
            # 绝对路径则直接返回
            return target_path
        # 延长路径
        return IoMethods.join_path(parent_path, target_path)

    def change_param(self, key, new_value):
        self.table_properties = self.ro.read_default_setting()
        change_symbol = False
        for root_key in self.table_properties.keys():
            if key in self.table_properties[root_key]:
                self.table_properties[root_key][key] = new_value
                change_symbol = True
        if change_symbol is False:
            raise AttributeError(f"key {key} doesn't exist in properties. please check again.")
        self.ro.store_default_setting(self.table_properties)
        self.log.show_log(f"[PARAMETER TUNER]'{key}' parameter is changed as '{new_value}'.")
        return

    def change_multi_params(self, params: dict):
        for key in params.keys():
            self.change_param(key, params[key])
        return


if __name__ == "__main__":
    ip = IntegratedParameters()
    ip.init_params()