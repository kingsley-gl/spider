#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : get_engine.py
# @Software: vip spider
# @Function:


from .exceptions import DataBaseConnectError
import re
import pyodbc




class GetDBEngine(object):
    # pool = [] #链接池
    def __init__(self, config):
        for section in filter(lambda sec: u'DB' in sec,config.sections()):
            for option in config.options(section):
                self.__dict__[section[0:5]+'_'+option] = config.get(section, option)

    def vertica_engine(self):
        try:
            import pyodbc
            return pyodbc.connect('Driver={Vertica}; Database=%s; Servername=%s; UID=%s; PWD=%s; Port =%s'
                                  % (self.VERTI_db, self.VERTI_host, self.VERTI_user, self.VERTI_passwd,
                                     self.VERTI_port))
        except Exception as e:
            raise DataBaseConnectError('executing function "%s.vertica_engine" caught %s'%(self.__class__.__name__, e))

    def orm_vertica_engine(self):
        try:
            import sqlalchemy as sa
            return sa.create_engine('vertica+vertica_python://{user}:{passwd}@{host}:{port}/{db}'
                                    .format(user=self.VERTI_user,
                                            passwd=self.VERTI_passwd,
                                            host=self.VERTI_host,
                                            port=self.VERTI_port,
                                            db=self.VERTI_db))
        except Exception as e:
            raise DataBaseConnectError('executing function "%s.vertica_engine" caught %s'%(self.__class__.__name__, e))

    # def mysql_engine(self):
    #     try:
    #         import sqlalchemy
    #         return
    #     except:
    #         raise DataBaseError('mysql database connect error')


class GetEngine(object):
    def __init__(self, **kwargs):
        self.vertica1_ip = kwargs['host']
        self.vertica1_port = kwargs['port']
        self.vertica1_user = kwargs['user']
        self.vertica1_pw = kwargs['passwd']
        self.vertica1_db = kwargs['db']

    def get_engine(self):
        import pyodbc
        return pyodbc.connect('Driver={Vertica}; Database=%s; Servername=%s; UID=%s; PWD=%s; Port =%s'
                              % (self.vertica1_db, self.vertica1_ip, self.vertica1_user, self.vertica1_pw,
                                 self.vertica1_port))
