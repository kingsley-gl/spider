#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/21
# @Author  : kingsley kwong
# @Site    :
# @File    : base_state.py
# @Software: vip spider
# @Function:
from util.exceptions import NoElementError
import inspect
from selenium.common.exceptions import NoSuchElementException
from util.logger import log
import time


class State(object):
    success_state = None
    fail_state = None
    logger_name = 'testing'

    def _operate_logger(*dargs, **dkwargs):
        def _decorate(func):
            def _inner(self, *args, **kwargs):
                logger = log.getLogger(self.logger_name)
                if dkwargs['level'] in 'debug':
                    logger.debug('State-%s %s(%s).%s()'%(self.__name__, kwargs['locate_way'], kwargs['xpath'], kwargs['operator']))
                else:
                    logger.info('')
                try:
                    return func(self, *args, **kwargs)
                except NoElementError as e:
                    logger.error(e)
                    raise e
            return _inner
        return _decorate

    @_operate_logger(level='debug')
    def browser_operation(self, driver, locate_way, xpath, operator, key=None):
        try:
            position = getattr(driver, locate_way)
            locate = position(xpath)
            operate = getattr(locate, operator)
            if inspect.ismethod(operate):
                if key is not None:
                    return operate(key)
                else:
                    return operate()
            else:
                return operate
        except NoSuchElementException:
            raise NoElementError('could not execute "%s(%s).%s(%s)" '%(locate_way, xpath, operator, key))

    def do(self, driver):
        pass

    def work(self, driver):
        try:
            return self.do(driver=driver)
        except Exception as e:
            logger = log.getLogger(self.logger_name)
            logger.error(e)
            print('redo')
            time.sleep(3.0)
            return self.fail_state


class WorkState(object):
    def __init__(self, driver, default_state):
        self.driver = driver
        self.default_state = default_state  # 初始默认状态
        self.current_state = self.default_state  # 当前状态

    def run(self):
        while self.current_state:
            self.current_state = self.current_state.work(driver=self.driver)

