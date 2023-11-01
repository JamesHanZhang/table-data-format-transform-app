# CSV快速转为EXCEL说明书
- author: `JamesHanZhang`
- github: `https://github.com/JamesHanZhang/DATA-ANALYSIS-PROJECT`
- email: `jameshanzhang@foxmail.com`

## 程序使用说明
1. 打开执行程序
2. 输入导入的文件名，例如`input_data.csv`，必须包含后缀名
3. 输入分隔符，即csv文件内各个字段之间如何分开的
4. 输入文件所在的绝对路径
5. 确认无误后，输入`YES`并回车
6. 执行完成后，根据提示回车即可退出程序

## 程序情况
- 如读取的文件中有无法读取的记录，会自动拆分成两个文件保存在文件所在的路径下：
	- 后缀名为`_error_lines.csv`的文件，包含所有无法读取的记录；
	- 后缀名为`_originalcsv(error_deleted).csv`的文件，包含所有可正常读取的记录；
	- 且程序会将所有可读取的记录正常转换为excel文件；
- `.log`文件是程序log记录，用于回查程序执行情况，每次执行程序会自动生成，执行完成后可直接删除；

