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
chunk_reader = dc.import_on_extension("02.input_test.xlsx", if_circular=True)
count = 0
for df in chunk_reader:
    print(f"time for circular reading: {str(count)}")
    print(df)
    count+=1

    # 导出
    if count == 1:
        overwrite = True
    else:
        overwrite = False
    do.output_on_extension(df, "test.md", overwrite=overwrite)