@echo off
echo ��������
py -2 processing_run.py
echo ����д�����ݿ�
py -2 .\util\file_to_vertica.py
echo �������
echo �밴������˳� & pause
exit