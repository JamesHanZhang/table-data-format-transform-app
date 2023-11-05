from analysis_modules import FormatTransformation

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
      "######################## 程序开始执行, 请耐心等候 ################################\n\n")

################################################### 参数 PARAMETERS #####################################################

# 此处填写参数表名，不填写则为默认值
params_set = ""

# overwrite=True, 是读取调整好的参数并覆盖已保存的参数表
# overwrite=False, 是读取已保存的参数表
overwrite = True

# based_on_activation = True: 根据激活activation判断是否导出
# based_on_activation = False: 根据导出文件名的拓展名判断是否导出
# based_on_activation = None: 如导出文件没有拓展名, 自动按激活导出; 如导出文件有拓展名, 自动按拓展名导出
based_on_activation = None

# 是否根据文件夹批量导入文件, 如选择参数表默认值则为None
if_batch = None

# 导入的绝对路径, 如使用参数表路径或默认路径, 则不填写
input_path = ""

# 导出的绝对路径, 如使用参数表路径或默认路径, 则不填写
output_path = ""

# 导入的文件名，如果是批量/或采取默认值则不需要写，文件必须有后缀名! 例如: test.csv, test.xlsx, test.md...
input_file = ""

# 导出的文件名, 如未激活XLS/MD/CSV导出功能, 可空置
output_file = ""

# 导出的表名, 如使用参数表默认值, 则不填写
table_name = ""

################################################### 执行 EXECUTION ######################################################

ft = FormatTransformation()
ft.reset_params(params_set, overwrite=overwrite, based_on_activation=based_on_activation, if_batch=if_batch,
                input_path=input_path, input_file=input_file, output_path=output_path, output_file=output_file,
                table_name=table_name)
ft.run_based_on_params_set(params_set, overwrite)



