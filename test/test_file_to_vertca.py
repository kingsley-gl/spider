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
ftb = FileToDB(engine_vertica,r'D:\share\vip_data')
ftb.files_to_verti('vip_active')
