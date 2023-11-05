from analysis_modules import FormatTransformation
from analysis_modules import default_properties as prop
from analysis_modules import ResourcesOperation, SysLog

print("######################## 欢迎使用数据格式转换软件 ###############################\n\n"
      "本程序需要和参数表搭配执行, 由参数表确定基本参数, 由互动界面确定常需修改的参数\n"
      "基于上述参数, 本程序可以实现如下功能:\n"
      "1. 通过`if_circular=True`可无视大小读取excel, csv, md文档为数据源;\n"
      "2. 数据源的读取过程中, 会自动将脏数据抽离出来形成`_error_lines`文件保存在导入路径下, 并仅导入可正确读取的记录;\n"
      "3. csv导入过程中, 支持多字符作为分隔符, 会在导入路径下自动生成`_repl_sep.csv`为后缀名的单字符分隔符的文件, 在进行读取;\n"
      "4. 可将数据源转换成excel, csv, md文档;\n"
      "5. 可将数据源转换成sql建表语句(自动爬取数据结构)及插入语句; 其中oracle插入语句穿插`commit;`命令不对系统造成负担;\n"
      "6. sql建表及插入语句的创建支持5种数据库, 分别是: Oracle, MySql, PostgreSql, SqlServer, GBase;\n"
      "7. 可以文件夹的形式批量读取文件夹下所有的excel, csv, md(包含子文件夹下的数据)为数据源, 并可指定单一数据类型进行读取;\n"
      "8. 仅支持同数据结构的文件(可以不同数据格式, 例如同结构的xlsx, csv文件)批量读取, 批量读取的文件最终会统合形成单一文件, 或通过`if_sep=True`参数拆分成多个文件;\n"
      "9. 可基于chunksize的大小设定将数据源拆分成多个记录数上限为chunksize的小文件;\n"
      "10. 支持基本的数据处理方法: 修改列名, 修改数据类型, 选择特定列输出, 数据脱敏;\n"
      "11. CSV数据导入过程中, 会自动筛选出脏数据并保存为后缀名为`_error_lines.csv`的数据; 支持多分隔符导入;"
      "12. 导入导出皆会保存数据记录条数到json参数表内; 导出SQL的时候会将各列最长的长度记录到表结构中; \n\n"
      "######################## 程序开始执行, 请耐心等候 ################################\n\n")


def get_overwrite(check_overwrite, msg_1, msg_2):
    if check_overwrite == '1':
        overwrite = True
        msg = msg_1
    elif check_overwrite == '2':
        overwrite = False
        msg = msg_2
    else:
        overwrite = None
        msg = "无效选择, 请根据数字重新选择;"
    return overwrite, msg

def get_based_on_activation(check_activation):
    if check_activation == '1':
        based_on_activation = True
        msg = "1 <根据激活导出模式>"
    elif check_activation == '2':
        based_on_activation = False
        msg = "2 <根据拓展名导出模式>"
    elif check_activation == '3':
        based_on_activation = True
        msg = "3 <自动模式>"
    else:
        msg = "默认模式 3 <自动模式>"
        print(msg)
        based_on_activation = None
    return based_on_activation, msg

################################################### 参数 PARAMETERS #####################################################

count = 0
while True:
    if count == 0:
        print("\n*********请输入参数*********\n")
    else:
        print("\n*********请重新输入参数*********\n")
    count += 1
    
    # params_set
    params_set = input(f"请输入参数表名(没有拓展名), 如不输入并直接回车则默认为'{prop.DEFAULT_PARAMS_SET}': ").strip()
    if params_set == "":
        params_set = prop.DEFAULT_PARAMS_SET
    
    # overwrite
    overwrite_app_1 = f"1 调用'_params_setting.py'的参数进行执行, 并在'resources'目录下自动生成或覆盖'{params_set}'同名.json参数文件;"
    overwrite_app_2 = f"2 直接调用'resources'目录下的'.json'参数表'{params_set}'进行执行;"
    check_overwrite = input(f"请根据数字选择功能:\n    {overwrite_app_1}\n    {overwrite_app_2}\n"
                            "输入数字并回车: ").strip()
    overwrite, overwrite_msg = get_overwrite(check_overwrite, overwrite_app_1, overwrite_app_2)
    if overwrite is None:
        print(overwrite_msg)
        continue
    
    # 校验params_set是否存在
    if overwrite is False:
        if_params_set_exist = ResourcesOperation.check_if_params_set_exists(params_set)
        if if_params_set_exist is False:
            print(f"\n未在'resources'目录下找到您希望调用的参数表'{params_set}', 请确认是否填写错误并重新输入!")
            continue
    
    # based_on_activation
    check_activation = input("请根据数字选择数据导出模式:\n"
                             "    1 <根据激活导出模式>: 采用'output_params_setting.py'中的激活功能判断是否导出对应格式的数据;\n"
                             "      该模式激活几个功能就导出几个文件(激活拆分功能则更多);\n"
                             "      该模式在导出'.sql'文件时, 采用参数'table_name'确定导出的表名及文件名;\n"
                             "    ***********************************************************\n"
                             "    2 <根据拓展名导出模式>采用导出文件output_file的拓展名(例如'test.xlsx')判断是导出哪种格式数据;\n"
                             "      该模式一次仅激活一个导出功能;\n"
                             "      该模式在导出'.sql'文件时, 默认将临时表的表名设置为导出文件的主体名(去掉拓展名);\n"
                             "    ***********************************************************\n"
                             "    3 <自动模式>, 根据导出的文件名'output_file'是否有拓展名(例如'test.xlsx')来判断采取以上两种模式的哪种模式;\n"
                             "      如导出的文件名没有拓展名, 即只有纯粹的文件名(例如'test'), 则激活<根据激活导出模式>;\n"
                             "      如导出的文件名有拓展名(例如'test.xlsx'), 则激活<根据拓展名导出模式>;\n"
                             "    ***********************************************************\n"
                             "    其他: 如直接回车, 默认激活<自动模式>;\n"
                             "请根据数字选择您希望激活的模式: ").strip()
    based_on_activation, act_msg = get_based_on_activation(check_activation)
    
    print(f"\n######################## 您所输入的参数如下, 请重新确认 ################################\n\n"
          f"您所输入的参数表名为: '{params_set}'\n"
          f"您所选择的参数导入模式为: {overwrite_msg}\n"
          f"您所选择的数据导出模式为: {act_msg}\n"
          f"请再次确认...")
    
    if_process = input("请选择是否执行(Y/N): ").strip()
    if if_process in ['YES','Y','Yes','yes','y']:
        break
    else:
        continue

print("\n\n\n\n")

msg = f"######################## 围绕参数表'{params_set}'的数据格式转换程序开始执行 ################################\n\n"
SysLog.show_log(msg)

################################################### 执行 EXECUTION ######################################################

ft = FormatTransformation()
ft.reset_params(params_set, overwrite=overwrite, based_on_activation=based_on_activation)
ft.run_based_on_params_set(params_set, overwrite)


msg = f"######################## 程序已顺利结束执行 ################################\n\n"
SysLog.show_log(msg)
