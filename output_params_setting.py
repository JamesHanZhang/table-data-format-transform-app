from import_params_setting import chunksize
# 以下可添加绝对路径，如为空值""，则为默认导出路径output_dataset，也可添加默认路径下output_dataset的子文件夹，例如:
# 如添加子文件夹child_folder/split1，则实际路径为:../output_dataset/child_folder/split1
output_path = ""

# 加载时的解码格式默认值：INPUT_ENCODE = "utf-8" 或者中文环境下常用"gbk"
output_encoding = ""

# 判断是否要拆分，如拆分，则按照导入的chunksize的大小进行拆分
if_sep = False

# 是否只是拆分出一个样例即可，默认为否，即拆分到底。如为真，则仅拆分出一个文件即停止，作为样例
only_one_chunk = False

# 导出是否覆盖，如不覆盖，则默认添加到同名文件的末尾，拆分的情况只允许OVERWRITE = True
overwrite = True

csv_output_params = {
    'output_sep': ',',
    # 可能内容里也有该分隔符，容易导致错误，所以内容里该分隔符的部分可以替换为新的符号
    'repl_to_sub_sep': ' '
}

xls_output_params = {
    'output_sheet': 'Sheet1'
}

# 暂时没有参数需要调整
md_output_params = {}

sql_output_params = {
    # 选择导出的数据库类型
    'database': 'oracle',
    # DATABASE里的可选项，作用仅为提示DATABASE里可写的数据库引擎
    'database_options': ['oracle','gbase','mysql'],
    # 可能内容里有半角逗号，这个在插入语句中是不被允许的，所以要替换成其他符号
    'repl_to_sub_comma': ';',
}

