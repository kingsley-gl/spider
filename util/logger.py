#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/19
# @Author  : kingsley kwong
# @Site    :
# @File    : logger.py
# @Software: vip spider
# @Function:

import logging.config
import datetime,time


LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'debug': {
            'format': '[LOGNAME-%(name)s][TIME-%(asctime)s][FILE-%(filename)s][PID-%(process)d] %(levelname)s: %(message)s'
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
            'level':'DEBUG',
            'class':'logging.StreamHandler',
            'formatter': 'debug',
            'stream':'ext://sys.stdout',
        },
        'console_info':{
            'level':'INFO',
            'class':'logging.StreamHandler',
            'formatter':'info',
            'stream':'ext://sys.stdout',
        },
        'file':{
            'level':'INFO',
            'class':'logging.FileHandler',
            'formatter':'info',
            'filename':'e:\\vip_spider\\vip_spider.log',
        }

    },
    'loggers': {
        'spider_debug': {
            'handlers':['console_debug',],
            'propagate': True,
            'level':'DEBUG',
        },
        'spider_info':{
            'handlers': ['file','console_info'],
            'propagate': True,
            'level': 'INFO',
        }


    }
}

logging.config.dictConfig(LOGGING)
log = logging