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
        self._tmp2 = tb_name + '_tmp2'
        self.tb = tb_name
        self.tb_test = tb_name + '_test'

    @_class_logger(level='info', msg='files to vertica table')
    def files_to_verti(self, tb_name):
        self._set_table(tb_name)
        try:
            self._files_to_tmp1()
            self._tmp1_to_tmp2()
            self._tmp2_to_verti()
        except DataBaseExecuteError as e:
            self.info.error(e)

    @_class_logger(level='debug', msg='truncate table')
    def _truncate_table(self, tb_name):
        try:
            with self.db_engine.cursor() as crsr:
                crsr.execute(self._truncate_sql(tb_name))
        except Exception as e:
            raise DataBaseExecuteError('executing function "%s._truncate_table" caught %s'
                                       % (self.__class__.__name__, e))

    @_class_logger(level='info', msg='file to tmp1 start')
    def _files_to_tmp1(self):
        try:
            self._truncate_table(tb_name=self._tmp1)
            file_path_prefix = self.files_path + '\\%s' % (self.tb)
            file_list = set(os.listdir(file_path_prefix))
            for file_name in file_list:
                file_name = os.path.join(file_path_prefix, file_name)
                sql = "COPY %s  FROM LOCAL '%s' DELIMITER ',' AUTO " % (self._tmp1, file_name)
                with self.db_engine.cursor() as crsr:
                    crsr.execute(sql)
        except Exception as e:
            raise DataBaseExecuteError('executing function "%s._files_to_tmp1" caught %s'
                                       % (self.__class__.__name__, e))

    @_class_logger(level='info', msg='tmp1 to tmp2 start')
    def _tmp1_to_tmp2(self):
        try:
            self._truncate_table(tb_name=self._tmp2)
            sql = "INSERT INTO %s " \
                  "SELECT * FROM %s a " \
                  "WHERE EXISTS (SELECT 1 FROM (SELECT activeCode FROM %s GROUP BY activeCode HAVING count(*) = 1) b " \
                  "WHERE a.activeCode= b.activeCode ) ;" \
                  "INSERT INTO %s " \
                  "SELECT DISTINCT *   FROM %s a " \
                  "WHERE NOT EXISTS (SELECT 1 FROM (SELECT activeCode FROM %s GROUP BY activeCode HAVING count(*) = 1) b " \
                  "WHERE a.activeCode= b.activeCode ) ;" % (
                  self._tmp2, self._tmp1, self._tmp1, self._tmp2, self._tmp1, self._tmp1)
            with self.db_engine.cursor() as crsr:
                crsr.execute(sql)
        except Exception as e:
            raise DataBaseExecuteError('executing function "%s._tmp1_to_tmp2" caught %s' % (self.__class__.__name__, e))

    @_class_logger(level='info', msg='tmp2 to vertica start')
    def _tmp2_to_verti(self):
        delete_sql ="DELETE FROM %s "\
                    "WHERE EXISTS (SELECT 1 FROM %s b "\
                    "WHERE %s.activeCode = b.activeCode )" % (self.tb_test, self._tmp2, self.tb_test)

        sql = '''INSERT INTO %s 
              SELECT a.*,SYSDATE() FROM %s a 
              WHERE NOT EXISTS(SELECT 1 FROM %s b WHERE a.activeCode = b.activeCode )'''\
              % (self.tb_test, self._tmp2, self.tb_test)
        sqls = [delete_sql, sql]
        try:
            for sql in sqls:
                with self.db_engine.cursor() as crsr:
                    crsr.execute(sql)
        except Exception as e:
            raise DataBaseExecuteError('executing function "%s._tmp2_to_verti" caught %s'
                                       % (self.__class__.__name__, e))

