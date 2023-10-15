############### 默认参数 #################
# 加载时的解码格式默认值：INPUT_ENCODE = "utf-8" 或者中文环境下常用"gbk"
INPUT_ENCODING = "utf-8"

# 导出时的解码格式默认值：OUTPUT_ENCODE = "utf-8" 或者中文环境下常用"gbk"
OUTPUT_ENCODING = "utf-8"

# 针对BigCSV: 每次执行CHUNKSIZE条数据
# 也针对拆分数据: 每满CHUNKSIZE条数据拆分为子文件
CHUNKSIZE = 10000

# 判断是否读取csv的时候要将双引号"视为分隔符的一部分(如果贴近分隔符的话)，还是视为数据内容进行读取
# 如为真则视为数据内容进行读取，为假则视为分隔符的一部分，默认为假
QUOTE_NONE = False

# 确认是否直接导入（能保持数据类型），还是以字符串的形式导入（能尽量避免数据遗失）
# True: 以字符串类型导入，False: 直接导入
QUOTE_AS_OBJECT = True

