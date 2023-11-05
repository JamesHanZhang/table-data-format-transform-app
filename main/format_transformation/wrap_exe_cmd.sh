#! /bin/bash

# 激活anaconda环境
conda activate james_environment

# 只有通过外部路径的执行, 才可以保证该脚本不被自动打包仅exe内, 可以作为依赖文件放置在外面
# 因为tabulate在打包环境无法被打包，所以手动标注打包 --hidden-import=tabulate
pyinstaller --name=data-format-transformation-app --onefile --hidden-import=tabulate --icon=rabbit_exe_icon1.ico user_tran_format_app.py