from across_process import *
from df_import_drivers import DfCreation
from df_output_drivers import DfOutput
from df_processing import *

# 初始化
ip = IntegratedParameters()
ip.init_params()
dc = DfCreation()
do = DfOutput()

# 直接以generator的形式读取，适合大数据
df_reader = dc.import_on_extension("01.input_test.csv",if_circular=True)
for df in df_reader:
    print(df)

    # 导出
    do.output_on_extension(df, "test.md")