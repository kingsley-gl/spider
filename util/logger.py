#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : logger.py
# @Software: vip spider
# @Function:

import logging.config
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('vip.cfg')


LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'debug': {
            'format': '[LOGNAME-%(name)s][TIME-%(asctime)s] %(levelname)s: %(message)s'
        },
        'info': {
            'format': '[%(asctime)s][%(filename)s] %(levelname)s %(message)s'
        },
    },
    'handlers': {
        'null': {
            'level':'DEBUG',
            'class':'logging.NullHandler',
            'formatter':'debug',
        },
        'console_debug':{
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'debug',
            'stream': 'ext://sys.stdout',
        },
        'console_info': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'info',
            'stream': 'ext://sys.stdout',
        },
        'file_sale': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'formatter': 'info',
            'filename': config.get('SPIDER_LOG', 'sale_log_file_path'),
        },
        'file_uv': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'formatter': 'info',
            'filename': config.get('SPIDER_LOG', 'uv_log_file_path'),
        },
        'file_database': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'formatter': 'info',
            'filename': config.get('SPIDER_LOG', 'database_log_file_path'),
        }
    },
    'loggers': {
        'spider_sales': {
            'handlers':['console_debug', 'file_sale'],
            'propagate': True,
            'level':'INFO',
        },
        'spider_uv': {
            'handlers': ['console_debug', 'file_uv'],
            'propagate': True,
            'level': 'INFO',
        },
        'database': {
            'handlers': ['console_debug', 'file_database'],
            'propagate': True,
            'level': 'INFO',
        }

    }
}

logging.config.dictConfig(LOGGING)
log = logging