import os

from analysis_modules import default_properties as prop
from basic_operation import IoMethods
from analysis_modules.params_monitor import *
from analysis_modules.df_import_drivers import DfCreation
from analysis_modules.df_processing import BasicProcessing
from analysis_modules.df_output_drivers import DfOutput
from analysis_modules.sql_output_drivers import SqlOutput

def get_params(params_set, overwrite) -> tuple[ImportParams, OutputParams, BasicProcessParams]:
    if overwrite is True:
        import_params, output_params, basic_process_params = IntegrateParams.get_params_from_settings(params_set)
    else:
        import_params, output_params, basic_process_params = IntegrateParams.get_params_from_resources(params_set)
    return import_params, output_params, basic_process_params
def get_params_set(params_set=prop.DEFAULT_PARAMS_SET):
    if params_set == "":
        params_set = prop.DEFAULT_PARAMS_SET
    return params_set

def get_overwrite(check_overwrite):
    msg = None
    overwrite = None
    if check_overwrite == '1':
        msg = "1 根据调整好的参数执行程序;"
        overwrite = True
    elif check_overwrite == '2':
        msg = "2 根据已存在的json参数表执行程序;"
        overwrite = False
    else:
        print("功能选择错误, 请重新选择!")
    return msg, overwrite

def get_if_batch(check_batch):
    if_batch = None
    if check_batch in ['YES','Y', 'y', 'yes', 'Yes']:
        if_batch = True
    elif check_batch in ['NO', 'N', 'n', 'no', 'No']:
        if_batch = False
    else:
        print("功能选择错误, 请重新选择!")
    return if_batch

def check_path(path):
    if os.path.isabs(path) is True:
        return path
    elif path == "":
        return path
    else:
        print("请输入绝对路径! 请重新输入参数!")
        return None

################################################### 参数 PARAMETERS #####################################################


print("######################## 欢迎使用数据格式转换软件 ###############################\n\n"
      "本程序需要和参数表搭配执行, 由参数表确定基本参数, 由互动界面确定常需修改的参数\n"
      "基于上述参数, 本程序可以实现如下功能:\n"
      "1. 无视大小读取excel, csv, md文档为数据源;\n"
      "2. 可将数据源转换成excel, csv, md文档;\n"
      "3. 可将数据源转换成sql建表语句及插入语句;\n"
      "4. sql建表及插入语句的创建支持5种数据库, 分别是: Oracle, MySql, PostgreSql, SqlServer, GBase;\n"
      "5. 可以文件夹的形式批量读取excel, csv, md为数据源, 并可指定数据类型进行读取;\n"
      "6. 仅支持同数据结构的文件批量读取, 批量读取的文件最终会统合形成单一文件;\n"
      "7. 可基于chunksize的大小设定将数据源拆分成多个记录数上限为chunksize的小文件;\n"
      "8. 支持基本的数据处理方法: 修改列名, 修改数据类型, 选择特定列输出, 数据脱敏;\n"
      "9. CSV数据导入过程中, 会自动筛选出脏数据并保存为后缀名为`_error_lines.csv`的数据; 支持多分隔符导入;"
      "10. 导入导出皆会保存数据记录条数到json参数表内; 导出SQL的时候会将各列最长的长度记录到表结构中; \n\n"
      "######################## 请根据提示输入参数以执行 ###############################\n\n")

run = 0
while True:
    if run == 0:
        print("\n请在下面输入参数\n")
    else:
        print("\n请在下面重新输入参数\n")
    run+=1
    
    input_params_set = input("请输入参数表名, 直接回车则为默认值DEFAULT: ").strip()
    params_set = get_params_set(input_params_set)
    
    check_overwrite = input("请选择:\n    1 根据调整好的参数执行程序;\n    2 根据已存在的json参数表执行程序;\n请输入数字: ").strip()
    msg, overwrite = get_overwrite(check_overwrite)
    if msg is None:
        continue
        
    if overwrite is False:
        import_params, output_params, basic_process_params = get_params(params_set, overwrite)
        if_batch = import_params.batch_import_params.if_batch
    
    if overwrite is True:
        check_batch = input("请问是否根据文件夹进行的批量导入? Y/N: ").strip()
        if_batch = get_if_batch(check_batch)
        if if_batch is None:
            continue
        batch_msg = f"是否根据文件夹进行的批量导入: {check_batch}\n"
    else:
        batch_msg = ""
        
    input_path = input("请输入导入文件的绝对路径, 如为默认导入路径./input_dataset或参数表内的其他路径, 则请直接回车.\n请输入: ").strip()
    input_path = check_path(input_path)
    if input_path is None:
        continue
    
    output_path = input("请输入导出文件的绝对路径, 如为默认导出路径./output_dataset或参数表内的其他路径, 则请直接回车.\n请输入: ").strip()
    output_path = check_path(output_path)
    if output_path is None:
        continue
    
    if if_batch is False:
        input_file = input("请输入导入的文件名, 需有后缀名: ").strip()
        if input_file == "":
            print("请输入文件名!")
            continue
        elif IoMethods.get_file_extension(input_file) == "":
            print("文件必须有后缀名! 例如: test.csv, test.xlsx, test.md...")
            continue
    else:
        input_file = "test.csv"
        
    output_file = input("请输入导出的EXCEL/MD/CSV文件名, 如本次不涉及, 可直接回车跳过: ").strip()
    
    if overwrite is True:
        table_name = input("请输入导出的SQL对应的表名, 如本次不涉及, 可直接回车跳过: ").strip()
        table_msg = f"导出的SQL对应的表名(如不涉及可空置): {table_name}\n"
    else:
        table_msg = ""
    
    file_msg = "" if if_batch is True else f"导入的文件名: {input_file}\n"
    print(f"\n######################## 输入的参数如下所示 ###############################\n"
          f"参数表名: {params_set}\n"
          f"参数表: {msg}\n"
          f"{batch_msg}"
          f"导入的绝对路径(如遵循参数表则空置): {input_path}\n"
          f"导出的绝对路径(如遵循参数表则空置): {output_path}\n"
          f"{file_msg}"
          f"导出的EXCEL/MD/CSV文件名(如不涉及可空置): {output_file}\n"
          f"{table_msg}")
    
    if_correct = input("请确认是否正确 (Y/N): ").strip()
    if_correct = get_if_batch(if_correct)
    if if_correct is True:
        break


################################################### 执行 EXECUTION ######################################################

dc = DfCreation()
bp = BasicProcessing()
do = DfOutput()
so = SqlOutput()

output_file = "test.csv" if output_file == "" else output_file

if overwrite is False:
    if_batch = None
    input_path = ""
    output_path = ""
    table_name = ""


start_time = start_program()

import_params, output_params, basic_process_params = get_params(params_set, overwrite)
chunk_reader = dc.circular_import_data(import_params, input_file, input_path, if_batch, params_set)

pos = 0
for chunk in chunk_reader:
    chunk = bp.basic_process_data(chunk, basic_process_params)
    do.output_on_activation(chunk, output_params, output_file, output_path, chunk_no=pos, params_set=params_set)
    so.output_as_sql_on_activation(chunk, output_params, params_set=params_set, table_name=table_name, output_path=output_path, chunk_no=pos)
    pos += 1

end_program(start_time)



