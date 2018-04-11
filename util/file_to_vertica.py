#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : file_to_vertica.py
# @Software: vip spider
# @Function:

import sqlalchemy
import os
from .exceptions import DataBaseExecuteError
import time
import datetime
import sys
from util.logger import log
import ConfigParser
import pandas as pd



config = ConfigParser.ConfigParser()
config.read('vip.cfg')


class FileToDB(object):
    '''文件内数据存储到数据库'''
    def __init__(self, db_engine, files_path):
        self.db_engine = db_engine
        self.files_path = files_path
        self._truncate_sql = lambda x: "TRUNCATE TABLE  %s " % x
        # self.debug = log.getLogger('spider_debug')
        self.info = log.getLogger('database')

    def _class_logger(*dargs, **dkwargs):
        '''日志装饰器'''
        def decorator(func):
            def inner(self, *args, **kwargs):
                if dkwargs['level'] == 'debug':
                    log = self.info.debug
                elif dkwargs['level'] == 'info':
                    log = self.info.info
                if kwargs.has_key('tb_name'):
                    log('%s %s' % (dkwargs['msg'], kwargs['tb_name']))
                else:
                    log('%s' % dkwargs['msg'])
                func(self, *args, **kwargs)
                if kwargs.has_key('tb_name'):
                    log('%s %s done' % (dkwargs['msg'], kwargs['tb_name']))
                else:
                    log('%s done' % dkwargs['msg'])
            return inner
        return decorator

    @_class_logger(level='debug', msg='setting local veriable')
    def _set_table(self, tb_name):
        self._tmp1 = tb_name + '_tmp1'
        self.tb = tb_name

    @_class_logger(level='info', msg='files to vertica table')
    def files_to_verti(self, tb_name):
        self._set_table(tb_name)
        try:
            self._tmp1_to_verti()
        except DataBaseExecuteError as e:
            self.info.error(e)

    @_class_logger(level='info', msg='tmp1 to vertica start')
    def _tmp1_to_verti(self):
        delete_sql ="DELETE FROM %s "\
                    "WHERE EXISTS (SELECT 1 FROM %s b "\
                    "WHERE %s.activeCode = b.activeCode )" % (self.tb, self._tmp1, self.tb)

        sql = '''INSERT INTO %s 
              SELECT a.*,SYSDATE() FROM %s a 
              WHERE NOT EXISTS(SELECT 1 FROM %s b WHERE a.activeCode = b.activeCode )'''\
              % (self.tb, self._tmp1, self.tb)
        sqls = [delete_sql, sql]
        try:
            for sql in sqls:
                conn = self.db_engine.connect()
                conn.execute(sql)
        except Exception as e:
            raise DataBaseExecuteError('executing function "%s._tmp1_to_verti" caught %s'
                                       % (self.__class__.__name__, e))
        finally:
            conn.close()

