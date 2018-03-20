#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/15
# @Author  : kingsley kwong
# @Site    :
# @File    : vip_spider_download_uvfile.py
# @Software: vip spider
# @Function:

from util.get_engine import GetDBEngine
import pandas as pd
from multiprocessing import Process
from util.csv_processing import SaveAsCSV
from spider.vip_spider_download_salesfile import download_and_process as d_sale
from spider.vip_spider_download_uvfile import download_and_process as d_uv
from util.file_to_vertica import FileToDB
import ConfigParser
import os


config = ConfigParser.ConfigParser()
config.read('vip.cfg')
raw_file_save_path = config.get('Save_Path_Config', 'save_file_path_root')
export_file_path = config.get('Export_Path_Config', 'export_path_root')
engine=GetDBEngine(config)
if __name__ == '__main__':
    print(u'爬虫运行开始')
    engine_vertica = engine.vertica_engine()
    sql_users = r'select * from sycm_user where user_id>=7000 and user_id<8000 and status=0'
    df_users = pd.read_sql(sql_users, engine_vertica)

    print list(df_users.sort_values(by='user_id')['user_id'])
    print u'请选择 user_id'
    user_id = int(raw_input('--> '))
    if user_id in list(df_users.sort_values(by='user_id')['user_id']):
        shop = list(df_users[df_users['user_id']==user_id]['shop_name'])[0]
        login_user = list(df_users[df_users['user_id']==user_id]['login_user'])[0]
        password = list(df_users[df_users['user_id']==user_id]['password'])[0]

    print u'输入需要往前爬取的天数，默认为30天'
    try:
        crawlDays = int(raw_input('--> '))
    except:
        crawlDays = 30
    print u'输入需要爬取的日期，多个时用","分割，例如：2016-07-01，又如：2016-07-01,2019-07-02。如不输入，直接按Enter跳过'
    try:
        crawlDates = raw_input('--> ')
        if len(crawlDates) < 10:
            crawlDates = []
        else:
            crawlDates = crawlDates.split(',')
    except:
        crawlDates = []

    raw_file_save_path = raw_file_save_path+r'{user_id}'.format(user_id=user_id)
    if not os.path.exists(raw_file_save_path):
        os.makedirs(raw_file_save_path)
    p_sale = Process(target=d_sale,kwargs={'user_id':user_id,
                                 'shop':shop,
                                 'login_user':login_user,
                                 'password':password,
                                 'rawFileSavePath':raw_file_save_path,
                                 'csvSaveRootPath':export_file_path,
                                'crawlDays':crawlDays,
                               'crawlDates':crawlDates,
                                })

    p_uv = Process(target=d_uv, kwargs={'user_id': user_id,
                                 'shop': shop,
                                 'login_user': login_user,
                                 'password': password,
                                 'rawFileSavePath': raw_file_save_path,
                                 'csvSaveRootPath': export_file_path,
                                'crawlDays': crawlDays,
                               'crawlDates': crawlDates})
    p_sale.start()
    p_uv.start()
    p_sale.join()
    p_uv.join()

    print('-------data anlysising start--------')
    sac = SaveAsCSV(uv_path=raw_file_save_path+'\\uv',
                    sale_path=raw_file_save_path+'\\sale',
                    export_path=export_file_path) #导出类初始化
    uv_file_list = [file for file in os.listdir(raw_file_save_path+'\\uv')]
    sale_file_list = [file for file in os.listdir(raw_file_save_path+'\\sale')]
    intersection = list(set(uv_file_list).intersection(set(sale_file_list)))  #取交集
    diff = list(set(uv_file_list).difference(set(sale_file_list)))  #取补集
    if not diff:
        for file in diff:
            print('%s 在文件夹里有缺失')
    head = {'shop':shop}
    for file in intersection:   # 读取文件
        fileName = file.decode('gbk')
        if os.path.isfile(raw_file_save_path+'\\uv'+r'\%s'% file) and\
            os.path.isfile(raw_file_save_path+'\\sale'+r'\%s'% file):
            active_code = fileName.split('_')[1]    # 获取档期码
            head.update({u'档期唯一码(档期名称+日期)':active_code})
            head.update({u'售卖时间':active_code[-8:]})
            head.update({u'档期名称':active_code[:-9]})
            sac.save_process(file, **head)
    print('-------data anlysising end--------')

    ftd = FileToDB(db_engine=engine.vertica_engine(), files_path=export_file_path)
    table_names = ['vip_active_day', 'vip_active_hour', 'vip_active', 'vip_goods']
    for table in table_names:
        ftd.files_to_verti(tb_name=table)
