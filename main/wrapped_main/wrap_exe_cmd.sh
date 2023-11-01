#! /bin/bash

# 打包csv转excel快速文件
# 注意需要先将default_properties里面的path_properties的参数都空置为""

# 快速将csv转为excel
pyinstaller --onefile --icon=exe_icon.ico csv_to_excel_fast_transfer.py