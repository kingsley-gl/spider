# -*- coding: utf-8 -*-

import pyodbc
# from sqlalchemy import create_engine




class GetEngine(object):
    def __init__(self, **kwargs):
        self.vertica1_ip = kwargs['host']
        self.vertica1_port = kwargs['port']
        self.vertica1_user = kwargs['user']
        self.vertica1_pw = kwargs['passwd']
        self.vertica1_db = kwargs['db']

    def get_engine(self):
        return pyodbc.connect('Driver={Vertica}; Database=%s; Servername=%s; UID=%s; PWD=%s; Port =%s'
                              % (self.vertica1_db, self.vertica1_ip, self.vertica1_user, self.vertica1_pw,
                                 self.vertica1_port))
