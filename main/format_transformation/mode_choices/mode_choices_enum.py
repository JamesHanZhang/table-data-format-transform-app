from enum import Enum, unique

@unique
class ModeChoices(Enum):
    # 同数据结构数据源处理
    SAME_STRUCT_MODE = 1
    # 不同数据结构同参数表处理
    SAME_PARAMS_MODE = 2
    # 不同数据结构不同参数表处理
    DIFF_PARAMS_MODE = 3