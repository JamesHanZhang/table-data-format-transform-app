"""
*****************************************
***        DATA-ANALYSIS-PROJECT      ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

import json
import io
import os
from os import listdir
from os.path import isfile, join

# self-made modules
# 底层类，尽可能不调用外包，仅以直接调用的方式调用参数，不走包调用
from basic_operation.basic_io_operation import IoMethods
import default_properties as prop
class ResourcesOperation():
    def __init__(self):
        self.iom = IoMethods()
        self.default_params_set = prop.DEFAULT_PARAMS_SET
        self.resources_path = prop.RESOURCES_PATH

    def list_resources(self) -> list[str]:
        file_list = [file for file in listdir(self.resources_path) if isfile(join(self.resources_path, file))]
        file_name_list = []
        for each_file in file_list:
            file_name_list.append(self.iom.get_main_file_name(each_file))
        return file_name_list

    def read_resource(self, json_file:str) -> dict[str, dict]:
        if json_file[-5:] != ".json":
            json_file += ".json"
        full_path = self.iom.join_path(self.resources_path, json_file)
        # 为正常打开json文件，要明确encoding type
        with open(full_path, mode='r', encoding='utf-8') as load_file:
            json_object = json.load(load_file)
        return json_object
    @classmethod
    def read_default_setting(cls) -> dict[str, dict]:
        log = cls()
        table_properties = log.read_resource(log.default_params_set)
        return table_properties

    def check_exists_resource(self, json_file: str) -> dict[str, dict]:
        json_object = self.read_resource(json_file)
        if json_object == {}:
            raise FileNotFoundError("{a} file not found!".format(a=json_file))
        return json_object

    def store_params_as_json(self, json_file: str, dict_content: dict) -> None:
        if json_file[-5:] != ".json":
            json_file += ".json"
        self.iom.mkdir_if_no_dir(self.resources_path)
        full_path = self.iom.join_path(self.resources_path, json_file)

        # 防止中文转义为ascii符号，要添加ensure_ascii=False,同时打开文件以encoding='utf-8'打开
        json_object = json.dumps(dict_content, indent=4, ensure_ascii=False)
        with io.open(full_path, mode='w', newline='\n', encoding='utf-8') as output_file:
            output_file.write(json_object)
        return None

    def store_default_setting(self, dict_content) -> None:
        self.store_params_as_json(self.default_params_set, dict_content)
        return

    def remove_resources_file(self, json_file: str) -> bool:
        if json_file[-5:] != ".json":
            json_file += ".json"
        full_path = self.iom.join_path(self.resources_path, json_file)
        try:
            os.remove(full_path)
        except (FileNotFoundError):
            pass
        return True


if __name__=="__main__":
    # test
    test_dict = {
        1: [111, 222, 333],
        "test": 'nice weather'
    }
    ro = ResourcesOperation()
    ro.store_params_as_json('test', test_dict)
    read_output = ro.read_resource('test')
    print(read_output)