from analysis_modules import *

# 初始化
start_time = start_program()
import_params, output_params = IntegrateParams.get_params_from_settings()
dc = DfCreation()
do = DfOutput()
sql_driver = SqlOutput()

# 直接以generator的形式读取，适合大数据
df = dc.import_on_extension(import_params, "02.input_test.csv")
print(df)
do.output_on_extension(df, output_params, "test.csv")
sql_driver.output_as_sql_control(df, output_params, table_name="temp_table")

end_program(start_time)