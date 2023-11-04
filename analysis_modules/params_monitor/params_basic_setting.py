"""
*****************************************
***     DATA-FORMAT-TRANSFORMATION    ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

import os
import json
# self-made modules
from basic_operation import IoMethods
import analysis_modules.default_properties as prop
from analysis_modules.params_monitor.resources_operation import ResourcesOperation

class ParamsBasicSetting:
    def __init__(self):
        pass

    def get_default_value(self, default_value, new_value):
        if new_value in [None, ""]:
            return default_value
        return new_value

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

    def read_process_params(self, params_set: str=prop.DEFAULT_PARAMS_SET):
        process_params = ResourcesOperation.read_resource(params_set)
        return process_params

    def store_params(self, app_name: str, params: dict, params_set: str=prop.DEFAULT_PARAMS_SET):
        ro = ResourcesOperation()
        process_params = dict()
        try:
            process_params = ro.read_resource(params_set)
        except FileNotFoundError:
            pass
        process_params[app_name] = params
        ro.store_params_as_json(params_set, process_params)
        return
