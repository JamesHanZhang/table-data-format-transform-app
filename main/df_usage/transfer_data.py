from across_process import *
from df_import_drivers import DfCreation
from df_output_drivers import DfOutput
from df_processing import *

# 初始化
ip = IntegratedParameters()
ip.init_params()
dc = DfCreation()
do = DfOutput()

# 导入
df = dc.import_as_df("01.input_test.csv")
print(df)

# 导出
do.output_on_extension(df, "test.md")