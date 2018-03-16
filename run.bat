@echo off
echo 正在运行
py -2 processing_run.py
echo 正在写入数据库
py -2 .\util\file_to_vertica.py
echo 运行完成
echo 请按任意键退出 & pause
exit