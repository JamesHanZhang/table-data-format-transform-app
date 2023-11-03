import os
from analysis_modules import *
from basic_operation import IoMethods
import_params = ImportParams()
output_params = OutputParams()
dc = DfCreation()
do = DfOutput()
log = SysLog()

def print_log(msg):
    print("************* 输入出现错误 ****************\n")
    print(msg)
    print("************** 请重新输入 *****************\n")

def check_input_file(input_file):
    extends = IoMethods.get_file_extension(input_file)
    if extends not in ['.csv']:
        print_log("仅支持拓展名为.csv的文件")
        return False
    return True

def check_path(input_path):
    if_abs = os.path.isabs(input_path)
    if if_abs is False:
        print_log("请输入该文件所在的绝对路径")
        return False
    return True

def check_app_opt(app_opt):
    if app_opt not in ['1','2']:
        print_log("请选择功能")
        return False
    return True

def set_chunksize(app_opt):
    chunksize = None
    if app_opt == '2':
        chunksize = input("请输入每个切片应记录的最大记录条数(仅输入数字): ").strip()
        try:
            chunksize = int(chunksize)
        except:
            raise TypeError("输入的记录条数格式错误")
    return chunksize

while True:
    app_opt = input("欢迎使用csv转excel功能，该功能支持多字符分隔符，会自动筛选出脏数据并保存；\n"
                    "请选择基本功能:\n"
                    "1 将csv直接转为excel文件(如csv记录条数过多, 或文件过大则会失败);\n"
                    "2 将csv切分成多个excel文件;\n"
                    "请输入希望使用的功能编号: ").strip()
    if check_app_opt(app_opt) is False:
        continue
    chunksize = set_chunksize(app_opt)

    input_file = input("请输入您希望导入的文件名(仅针对csv文档, 需含拓展名): ").strip()
    if check_input_file(input_file) is False:
        continue

    input_sep = input("请输入您希望导入文件的分隔符(不支持空格为分隔符): ").strip()
    input_path = input("请输入您希望导入的文件所在的绝对路径: ").strip()
    if check_path(input_path) is False:
        continue

    print("\n************** 请确认信息 *****************\n")
    msg = ""
    if app_opt == '2':
        msg += f"选择的功能为: 2 将csv切分成多个excel文件; 每个excel文件最大切片记录条数为: {str(chunksize)}\n"
    msg += f"导入的文件名: '{input_file}'\n" + f"分隔符为: '{input_sep}'\n" + f"导入的文件路径: '{input_path}'\n"
    print(msg)
    check = input("\n请确认以上数据是否正确, 如正确请输入YES并回车, 否则要求重新输入: ").strip()

    if check == "YES":
        break
    else:
        print("************** 请重新输入 *****************")


log.write_log(msg)
new_sep = import_params.csv_import_params.sep_to_sub_multi_char_sep
repl_sep = import_params.csv_import_params.repl_to_sub_sep

if len(input_sep) > 1:
    msg = f"导入文件会首先转存为以单符号分隔符'{new_sep}'为分隔符的文件(以'_repl_sep.csv'结尾)\n" \
          f"并将数据内所有该符号'{new_sep}'替换为'{repl_sep}'.以便于读取"
    log.show_log(msg)

start_time = start_program()

import_params.csv_import_params.input_sep = input_sep

if chunksize is not None:
    import_params.chunksize = chunksize



df_reader = dc.import_as_df_generator(import_params, input_file, input_path)
main_name = IoMethods.get_main_file_name(input_file)
output_file = main_name + ".xlsx"

count = 0
for chunk in df_reader:
    if count == 0 or chunksize is not None:
        overwrite = True
    else:
        overwrite = False

    if chunksize is None:
        chunk_no = ""
    else:
        chunk_no = count
        
    do.output_whole_df(chunk, output_params, output_file, input_path, overwrite=overwrite, chunk_no=chunk_no)
    count += 1

end_program(start_time)
msg = f"导出所在路径为: {input_path}.\n" \
      f".log文件是过程记录文件, 在程序执行结束后可删除\n"
log.show_log(msg)

msg = "*************************** 免责声明 ******************************\n" \
      "    本程序遵循MIT开源协议, 如有疑问, 可依据以下链接查看源代码:\n" \
      "    https://github.com/JamesHanZhang/DATA-ANALYSIS-PROJECT\n" \
      "    请用户自行负责校验数据转换质量"

log.show_log(msg)

exit_symbol = input("您可以通过回车或关闭终端退出程序了").strip()
exit()
