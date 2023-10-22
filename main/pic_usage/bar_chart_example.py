from drawpics import *
from df_import_drivers import DfCreation
from across_process import *
from df_processing import *

# 初始化参数
start_time = start_program()
ip = IntegratedParameters()
ip.init_params()

# 导入表
dc = DfCreation()
df = dc.import_on_extension("03.input_test.csv")

# 修改列类型
change_types = {
    'YEAR': 'object',
    'TRAN_NUMBER': 'int64'
}
df = BasicProcessing.change_column_types(df, change_types)

# 作图
bar = BarChartCreation()
bar.create_multi_bar_charts(df, 'YEAR','TRAN_NUMBER','TABLE_AFFILIATION','各地交易记录','REMARK',show=True,store_flag=True)

# 结束
end_program(start_time)