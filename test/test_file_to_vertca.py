#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : test_file_to_vertica.py
# @Software: vip spider
# @Function:

from util.file_to_vertica import FileToDB
from util.get_engine import GetDBEngine
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('../vip.cfg')
engine = GetDBEngine(config)
engine_vertica=engine.vertica_engine()
ftb = FileToDB(engine_vertica, r'D:\tmp_')
table_names = ['vip_active', 'vip_active_day',
               'vip_active_hour', 'vip_return',
               'vip_goods', 'vip_barCode',
                'vip_region', 'vip_behind_goods']
for table in table_names:
    ftb.files_to_verti(tb_name=table)

