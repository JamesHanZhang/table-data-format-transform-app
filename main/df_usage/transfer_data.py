from analysis_modules import *

# 初始化
start_time = start_program()
import_params, output_params = IntegrateParams.get_params_from_settings()
dc = DfCreation()
do = DfOutput()

# 直接以generator的形式读取，适合大数据
df = dc.import_on_extension(import_params, "02.input_test.xlsx")
print(df)
do.output_on_extension(df, output_params, "test.csv")

end_program(start_time)