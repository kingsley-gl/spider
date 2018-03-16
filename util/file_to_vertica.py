# -*- coding: utf-8 -*-
import os
import pyodbc
import pandas as pd
import time
import datetime
import sys
import socket
import logging
from GetEngine import *
import ConfigParser
reload(sys)
sys.setdefaultencoding('utf8')
# sys.path.append('../')
def get_now():
    return  time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
config = ConfigParser.ConfigParser()
config.read('vip.cfg')
class vip_to_vertica:
    def __init__(self):
        # self.log.info( os.getcwd()
        # 绝对路径转相对路径
        # self.log.info( os.path.relpath("d:/MyProj/MyFile.txt")
        #..\MyProj\MyFile.txt
        # 相对路径转绝对路径
        # path ='..\\sycm'
        # self.log.info( os.path.abspath(path)
        # sys.path.append(path)
        # from GetEngine import *
        engine = GetEngine(
            host=config.get('VerticaDB_Config', 'host'),
            port=config.get('VerticaDB_Config', 'port'),
            user=config.get('VerticaDB_Config', 'user'),
            passwd=config.get('VerticaDB_Config', 'passwd'),
            db=config.get('VerticaDB_Config', 'db'),
        )
        self.engine_vertica = engine.get_engine()
        if not os.path.exists(r'D:\share\vip_data\log\\'):
            os.makedirs(r'D:\share\vip_data\log\\')
        # 日志文件以及格式
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s %(name)-8s %(levelname)-4s %(message)s',
            datefmt='%Y/%m/%d %H:%M:%S',
            filename=r'D:\share\vip_data\log\BI.VIP.%s.%d.log' % (datetime.date.today().strftime('%Y-%m-%d'), time.time()),
            filemode='w'
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        formatter = logging.Formatter('%(name)-8s: %(levelname)-4s %(message)s')
        console.setFormatter(formatter)
        # console log setting
        logging.getLogger('').addHandler(console)
        self.log = logging.getLogger('BI VIP file to vertica')
        # selenium_logger = logging.getLogger('selenium.webdriver.remote.remote_connection')
        # selenium_logger.setLevel(logging.ERROR)

    def get_folder_address(self):
        """
        入库前，文档存放在 \\192.168.7.138
        """
        # if socket.gethostbyname(socket.getfqdn(socket.gethostname())) == '192.168.7.35':
        return "D:\\share"
        # else:
        #     return "\\\\192.168.7.35\\share"

    def to_vertica_tmp1(self,report_name):
        """
        文本写入数据仓库
        :param report_name:报表名称
        :return:
        """
        file_path = self.get_folder_address() + "\\vip_data\\%s\\"% report_name;
        file_path_deleted = self.get_folder_address() + "\\vip_data\\removed\\";
        file_list = os.listdir(file_path)
        if not os.path.exists(file_path_deleted):
            os.makedirs(file_path_deleted)
        #清空临时表
        table_name = '%s_tmp1'%report_name
        truncate_sql ="TRUNCATE TABLE  %s "%table_name
        vertica_cur = self.engine_vertica.cursor()
        vertica_cur.execute(truncate_sql)
        self.engine_vertica.commit()
        for fl in file_list:
            file_name = os.path.join(file_path,fl)
            remove_name = os.path.join(file_path_deleted,fl)
            # file_name = u'%s%s'%(file_path,fl)
            # remove_name =u'%s%s'%(file_path_deleted,fl)
            sql = ''
            self.log.info( u'{} {}  to {}_tmp1'.format(get_now(),fl,report_name))
            sql = "COPY %s  FROM LOCAL '%s' DELIMITER ',' AUTO "%(table_name,file_name) 
            vertica_cur = self.engine_vertica.cursor()
            vertica_cur.execute(sql)
            self.engine_vertica.commit()
            if vertica_cur.rowcount >0:
                copy_cmd = 'copy /Y "%s" "%s"' % (file_name, remove_name)
                os.popen(copy_cmd)
                os.remove(file_name)
            else :
                self.log.info( '%s not removed'%file_name)
        self.log.info( '%s %s to_vertica_tmp1 OK'%(get_now(),report_name))

    def to_vertica_tmp2(self,report_name):
        """
        数据从临时表1去重，插入临时表2
        :param report_name:bia表名
        :return:
        """
        #tmp1 insert distinct data into tmp2
        #清空临时表
        temp_name = '%s_tmp1'%report_name
        table_name = '%s_tmp2'%report_name
        truncate_sql ="TRUNCATE TABLE  %s "%table_name
        vertica_cur = self.engine_vertica.cursor()
        vertica_cur.execute(truncate_sql)
        self.engine_vertica.commit()
        self.log.info( u'%s %s_tmp1 to %s_tmp2'%(get_now(),report_name,report_name))
        sql ="INSERT INTO %s "\
             "SELECT * FROM %s a "\
             "WHERE EXISTS (SELECT 1 FROM (SELECT activeCode FROM %s GROUP BY activeCode HAVING count(*) = 1) b " \
             "WHERE a.activeCode= b.activeCode ) ;"\
             "INSERT INTO %s "\
             "SELECT DISTINCT *   FROM %s a "\
             "WHERE NOT EXISTS (SELECT 1 FROM (SELECT activeCode FROM %s GROUP BY activeCode HAVING count(*) = 1) b " \
             "WHERE a.activeCode= b.activeCode ) ;"%(table_name,temp_name,temp_name,table_name,temp_name,temp_name)
        # self.log.info( sql)
        vertica_cur = self.engine_vertica.cursor()
        vertica_cur.execute(sql)
        self.engine_vertica.commit()
        self.log.info( '%s %s to_vertica_tmp2 OK'%(get_now(),report_name))

    def to_vertica(self,report_name):
        """
        数据从临时表2插入正式表
        :param report_name:表名
        :return:
        """
        temp_name = '%s_tmp2'%report_name
        table_name = report_name + '_test'
        delete_sql ="DELETE FROM %s "\
                    "WHERE EXISTS (SELECT 1 FROM %s b "\
                    "WHERE %s.activeCode = b.activeCode )"%(table_name,temp_name,table_name)
        sql = '''INSERT INTO %s 
              SELECT a.*,SYSDATE() FROM %s a 
              WHERE NOT EXISTS(SELECT 1 FROM %s b WHERE a.activeCode = b.activeCode )'''%(table_name,temp_name,table_name)
        self.log.info( u'%s %s_tmp2 to %s'%(get_now(),report_name,report_name))
        #删除旧数据
        vertica_cur = self.engine_vertica.cursor()
        vertica_cur.execute(delete_sql)
        self.engine_vertica.commit()
        #插入新数据
        vertica_cur = self.engine_vertica.cursor()
        vertica_cur.execute(sql)
        self.engine_vertica.commit()
        self.log.info( '%s ----- %s to_vertica OK'%(get_now(),report_name))

def main():
    vip = vip_to_vertica()
    # table_names = ['vip_active','vip_active_day','vip_active_hour','vip_return','vip_goods','vip_barCode','vip_region','vip_behind_goods']
    table_names = ['vip_active_day','vip_active_hour','vip_active','vip_goods']
    try:
        for table_name in table_names:
            vip.to_vertica_tmp1(table_name)
            vip.to_vertica_tmp2(table_name)
            vip.to_vertica(table_name)
    except Exception as ex:
        vip.log.info(ex)
if __name__ == '__main__':
    main()