#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/15
# @Author  : kingsley kwong
# @Site    :
# @File    : csv_processing.py
# @Software: vip spider
# @Function:

import pandas as pd
import datetime
import time
import os
import re
import json
from util.logger import log
from util.exceptions import DataBaseExecuteError
import sys
reload(sys)
sys.setdefaultencoding('utf8')
logger = log.getLogger('spider_info')


class ExtractDataM(object):
    def __init__(self, files, percentage_column, chunksize, save_path=None):
        '''

        :param files: files list
        :param percentage_column:
        :param save_path:
        '''
        self.save_file_path = save_path #csv to save
        self.files = files
        self.files_xls = map(pd.ExcelFile, self.files)
        self.column_dict = {}
        self.percentage_column = percentage_column #percentage column list
        self._result = None
        self.table = None   #table name
        self.chunksize = chunksize

    def drag_datas_from_header(self, headers, on):
        '''
        提取数据
        :param headers:
        :param sale_sheet_name:
        :param uv_sheet_name:
        :return:
        '''
        # dfs = [pd.read_excel(file, encoding='utf-8') for file in self.files]
        dfs = map(pd.read_excel, self.files_xls)
        dfs = map(lambda x: x.dropna(axis=0), dfs)

        for p_header in self.percentage_column:  # 百分比数据处理
            for df in dfs:
                if p_header in df.columns:
                    if len(df[p_header]) > 0:
                        df[p_header] = [str(element).replace('%', '') if '%' in str(element)
                                             else str(element) for element in list(df[p_header])]
                        df[p_header] = [float(element)/100 if re.match('\d+', str(element)) is not None
                                             else 0.0 for element in list(df[p_header])]
        df_cols = []
        for df in dfs:
            df_col = [u'条形码']
            for header in headers:
                if header in df.columns:
                    df_col.append(header)   # add column from header
            df_col = set(df_col)
            df_cols.append(df_col)

        target_dfs = []
        for df, cols in zip(dfs, df_cols):
            temp = df.reindex(columns=cols)  # getting header data
            temp = temp.sort_values(by=[u'条形码'])
            target_dfs.append(temp)

        for i in range(len(target_dfs) - 1):
            if not i:
                result_df = pd.merge(target_dfs[i], target_dfs[i+1],
                                     suffixes=('', '_y'), on=on)  # merge different dataframe
            else:
                result_df = pd.merge(result_df, target_dfs[i+1],
                                     suffixes=('', '_y'), on=on)

        for key in self.column_dict.keys():
            result_df[key] = self.column_dict[key]  # adding columns
        result_df = result_df.reindex(columns=headers)  # generate target dateframe
        return result_df

    def write_to_csv(self, result):
        '''
        导入csv
        :param result: drag_datas_from_header 得到的pandas表

        :return:
        '''
        path = self.save_file_path + r'\%s' % self.table + r'\%s_%s.%d' % (
        self.table, datetime.date.today().strftime('%Y-%m-%d'), time.time()) + '.csv'
        logger.info('start to writer table "%s" to file %s' % (self.table, path))
        if not os.path.exists(self.save_file_path + ur'\%s' % self.table):
            os.makedirs(self.save_file_path + ur'\%s' % self.table)
        result.to_csv(path_or_buf=path, header=False, index=False, encoding='utf-8')

    def write_to_db(self, result, tb_name, con, header):
        '''
        导入csv
        :param result: drag_datas_from_header 得到的pandas表

        :return:
        '''
        result.columns = header
        result = result.dropna(axis=1, how='all')
        print('write {tb_name} in database'.format(tb_name=tb_name))
        result.to_sql(name=tb_name, con=con, if_exists='append', index=False,
                      chunksize=int(self.chunksize))

    def add_column(self, key, value):
        '''
        添加列
        :param key:
        :param value:
        :return:
        '''
        self.column_dict[key] = value


class SaveAsCSVM(object):

    def __init__(self, files_paths, engine, tb_frame_json, export_path, chunksize):
        self.files_paths = files_paths
        with open(tb_frame_json) as fp:
            j = json.load(fp)
            self.tables = j['tables']
            self.percentage_columns = j['percentageColumns']
        self.export_path = export_path
        self.engine = engine
        self.chunksize = chunksize
        for table in self.tables:
            # table.keys()
            map(lambda key: self.connect.connect().execute('TRUNCATE TABLE huimei.{table}'.format(table=key+'_tmp1')),
                table.keys())

    def __getattr__(self, item):
        if item in 'connect':
            try:
                return self.engine.orm_vertica_engine()
            except DataBaseExecuteError:
                return None
            super(object, self).__getattr__()

    def save_process(self, file_name, **kwargs):
        files_list = map(lambda x: '\\'.join((x, file_name)), self.files_paths)

        e = ExtractDataM(files=files_list,
                         save_path=self.export_path,
                         percentage_column=self.percentage_columns,
                         chunksize=self.chunksize)
        for key in kwargs.keys():
            e.add_column(key, kwargs[key])
        for table in self.tables:
            for key in table.keys():
                e.table = key
                ret = e.drag_datas_from_header(headers=table[key][0], on=u'条形码')
                # e.write_to_csv(ret)
                e.write_to_db(result=ret, tb_name=key + '_tmp1', con=self.connect, header=table[key][1])


