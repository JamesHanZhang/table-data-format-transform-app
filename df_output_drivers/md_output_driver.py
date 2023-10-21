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
from df_processing import NullProcessing
from basic_operation import IoMethods

class MdOutputDriver(DfOutputDriver):
    def __init__(self):
        super().__init__()

    def store_as_md(self, df, full_output_path, overwrite):
        # 将空值替换为空字符串
        df = NullProcessing.replace_null_with_emtpy_str(df)
        # 根据overwrite判断是添加到原有文件，还是重写，当overwrite==True，则重写，False则添加到原文件下
        if overwrite is True or self.iom.check_if_file_exists(full_output_path, False) is False:
            storage_mode = 'wt'
            df.to_markdown(buf=full_output_path, mode=storage_mode, index=False)
            return
        # 如果使用storage_mode = 'a' ，是自动把全部添加到最后，不仅没有换行，而且表头还在
        # 这里写添加的部分
        str_content = df.to_markdown(buf=None, index=False)
        list_content = str_content.split("\n")
        list_content = list_content[2:]
        str_content = '\n' + '\n'.join(list_content)
        self.iom.store_file(full_output_path, str_content, overwrite)
        return

    @SysLog().calculate_cost_time("<store as markdown>")
    def store_all_as_md(self, df, output_file, output_path="", overwrite=True):
        if output_path != "":
            self.output_path = output_path
        IoMethods.mkdir_if_no_dir(self.output_path)
        # 获得参数
        type = '.md'
        output_file = self.set_file_extension(output_file, type)
        full_output_path = self.iom.join_path(self.output_path, output_file)
        self.store_as_md(df, full_output_path, overwrite)
        msg = "[MARKDOWN OUTPUT]: file created: {a}".format(a=full_output_path)
        self.log.show_log(msg)
        return