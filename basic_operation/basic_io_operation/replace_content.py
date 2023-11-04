"""
*****************************************
***       DATA-CLEAN-ANALYSIS-TOOL    ***
***         AUTHOR: JamesHanZhang     ***
***        jameshanzhang@foxmail.com  ***
*****************************************
"""

import re
from tqdm import tqdm
from basic_operation.basic_io_operation.find_child_paths import FindChildPaths

class ReplaceContent(FindChildPaths):
    def __init__(self, encoding):
        # 使用super函数
        super().__init__(encoding)

    def sub_paths_content(self, file_paths: list[str], target: str, substitution: str) -> None:
        """
        替换内容
        target:= 正则表达式
        substitution:= 替换的内容（非正则表达式）
        """
        for each_path in tqdm(file_paths,desc="to replace certain part of the files under archive..."):
            each_content = self.read_content(each_path)
            new_content = re.sub(target, substitution, each_content)
            self.store_file(each_path, new_content)
        return

    def sub_path_common_content(self, parent_path: str, target: str, substitution: str) -> None:
        """
        用来替换所有子文件的内容
        """
        file_paths = self.gain_child_file_paths(parent_path)
        self.sub_paths_content(file_paths, target, substitution)
        return

    def sub_type_path_common_content(self, parent_path: str, target: str, substitution: str, extension: str='.py')->None:
        """
        用来替换特定的文件类型（根据后缀名替换）的内容
        """
        file_paths = self.gain_child_certain_type_paths(parent_path, extension)
        self.sub_paths_content(file_paths, target, substitution)
        return

if __name__ == '__main__':
    parent_path = "D:\\CODE-PROJECTS\\PYTHON-PROJECTS\\DATA-ANALYSIS-PROJECT"
    target_content = "\*\*\*        DATA\-ANALYSIS\-PROJECT      \*\*\*"
    substitution = "***       DATA-CLEAN-ANALYSIS-TOOL    ***"
    encoding = "utf-8"
    extension = '.py'
    rc = ReplaceContent(encoding)
    rc.sub_type_path_common_content(parent_path, target_content, substitution, extension)

