import os
from analysis_modules import *
from basic_operation import IoMethods
import_params = ImportParams()
output_params = OutputParams()
dc = DfCreation()
do = DfOutput()

def check_file(input_file):
    extends = IoMethods.get_file_extension(input_file)
    if extends not in ['.csv']:
        print("仅支持拓展名为.csv的文件")
        return False
    return True

def check_path(input_path):
    if_abs = os.path.isabs(input_path)
    if if_abs is False:
        print("请输入该文件所在的绝对路径")
        return False
    return True

def check_sep(input_sep):
    if len(input_sep) != 1:
        print("分隔符不能为空，且分隔符长度不能大于等于2个字符")
        return False
    return True

while True:
    input_file = input("请输入您希望导入的文件名(仅针对csv文档, 需含拓展名): ").strip()
    input_sep = input("请输入您希望导入文件的分隔符: ").strip()
    input_path = input("请输入您希望导入的文件所在的绝对路径: ").strip()
    print(f"导入的文件名: '{input_file}'\n分隔符为: '{input_sep}'\n导入的文件路径: '{input_path}'")
    check = input("请确认以上数据是否正确, 如正确请输入YES并回车: ").strip()
    check_file_type = check_file(input_file)
    check_abs_path = check_path(input_path)
    check_sep_type = check_sep(input_sep)
    if check == "YES" and check_file_type is True and check_abs_path is True and check_sep_type is True:
        break
    else:
        print("************************************************")
        print("请重新输入信息")

start_time = start_program()

import_params.csv_import_params.input_sep = input_sep

df_reader = dc.import_as_df_generator(import_params, input_file, input_path)
main_name = IoMethods.get_main_file_name(input_file)
output_file = main_name+'.xlsx'

count = 0
for chunk in df_reader:
    count +=1
    if count == 1:
        overwrite = True
    else:
        overwrite = False
    do.output_whole_df(chunk, output_params, output_file, input_path, overwrite)

end_program(start_time)
print(f"数据已经成功导出为{output_file}, 导出所在路径为: {input_path}.")

exit_symbol = input("您可以退出程序了").strip()
exit()
