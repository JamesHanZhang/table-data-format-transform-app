from import_params_setting import chunksize
# 以下可添加绝对路径，如为空值""，则为默认导出路径output_dataset，也可添加默认路径下output_dataset的子文件夹，例如:
# 如添加子文件夹child_folder/split1，则实际路径为:../output_dataset/child_folder/split1
output_path = ""

# 加载时的解码格式默认值：INPUT_ENCODE = "utf-8" 或者中文环境下常用"gbk"
output_encoding = "gb18030"

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
    # 导出表的基本信息 - 表名，默认值为temp_table
    'table_name': 'table_for_temp_use',
    # 导出表的备注
    'table_comment': '',
    # 导出表的基本信息 - 各字段长度，默认为空字典，导出时自动填充数据; 如填写则会和实际数据进行比较, dict[key: str, value: int]
    'table_structure': {},
    # 导出表的各个字段的备注, dict[str, str], key: column, value: comment for column. e.g. {'col1': 'comment1', 'col2': 'comment2'}
    'column_comments': {},
    # 选择导出的数据库类型，请注意大小写必须严格遵循下面的可选项
    'database': 'Oracle',
    # DATABASE里的可选项，作用仅为提示DATABASE里可写的数据库引擎
    'database_options': ['Oracle', 'GBase', 'MySql', 'PostgreSQL', 'SqlServer'],
    # 可能内容里有半角逗号，这个在插入语句中是不被允许的，所以要替换成其他符号
    'repl_to_sub_comma': ';',
}

