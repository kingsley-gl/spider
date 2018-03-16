#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time    : 2018/03/15
# @Author  : kingsley kwong
# @Site    :
# @File    : vip_spider_download_salesfile.py
# @Software: vip spider
# @Function:

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time,datetime
from util.delete_dup_file import delete_duplicate_files
import os
TIMING = 2.0

dangqi_period_date = []

def download_and_process(user_id, shop, login_user,
                         password, rawFileSavePath,
                         csvSaveRootPath,crawlDays,crawlDates):

    rawFileSavePath = rawFileSavePath + '\\sale'
    if not os.path.exists(rawFileSavePath):
        os.makedirs(rawFileSavePath)
    profile = webdriver.FirefoxProfile()
    profile.set_preference("browser.download.folderList", 2)
    profile.set_preference("browser.download.dir", rawFileSavePath)
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/octet-stream, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    profile.set_preference("browser.download.useDownloadDir", True)
    profile.update_preferences()
    b = webdriver.Firefox(firefox_profile = profile)
    b.set_page_load_timeout(30)

    b.get(r'https://vis.vip.com/login.php')
    while True:
        # 登录
        try:
            b.find_element_by_xpath(r'//*[@id="userName"]').send_keys(login_user)
            b.find_element_by_xpath((r'//*[@id="passWord"]')).send_keys(password)
            b.find_element_by_xpath(r'//*[@id="checkWord"]').send_keys('')
            break
        except:pass
    while True:
        try:
            b.find_element_by_xpath(r'//li[@data-v-0944691c]').click()   # 点击：魔方罗盘
            time.sleep(TIMING)
            break
        except Exception as e:
            time.sleep(3)
    root_handle = b.current_window_handle
    b.switch_to.window(root_handle)
    b.close()
    for handle in b.window_handles:
        if handle != root_handle:
            b.switch_to.window(handle)
            break
    root_url = b.current_url
    # 记录所有档期
    time.sleep(3)
    b.find_element_by_xpath(r'//button[@aria-label="Close"]').click()  # 点击：新功能导航页面关闭
    while True:
        try:
            time.sleep(TIMING)
            b.find_element_by_xpath("//div[@id='compass-app-body']/div/div/div/ul/li[4]/div").click()  # 点击：左栏第一级 商品
            time.sleep(TIMING)
            b.find_element_by_xpath("//div[@id='compass-app-body']/div/div/div/ul/li[4]/ul/li").click()  # 点击：左栏第二级 商品详情
            time.sleep(TIMING)
            b.find_element_by_xpath(
                "//div[@id='compass-app-body']/div[2]/div/div[2]/div/label[2]/span/span").click()   #点击：档期圆点
            time.sleep(TIMING)
            b.find_element_by_xpath(
                "//div[@id='compass-app-body']/div[2]/div/div[3]/div/div/div/div/div[2]/div/div/label/span/span").click()   #点击：全部方框
            time.sleep(TIMING)
            b.find_element_by_xpath(
                "//div[@id='compass-app-body']/div[2]/div/div[3]/div/div/div/div/div[3]/div/div/div/label/span/span").click()   #点击：条形码方框
            time.sleep(TIMING)

            break
        except Exception as e:
            print 'Exception is %s'%e
            b.refresh()
            b.get(root_url)
            time.sleep(5)

    i = 1
    brands = list()
    b.find_element_by_xpath("//input[@type='text']").click()    #点击：品牌下拉
    time.sleep(2)
    while True:
        try:
            brands.append(b.find_element_by_xpath("//div[4]/div/div/ul/li["+str(i)+"]/span").text)  #获取所有品牌
            i += 1
        except Exception as e:
            print('ID-%d brand Exception %s' % (i, e))
            b.find_element_by_xpath("//input[@type='text']").click()  # 点击：品牌下拉
            break


    def sleep_decorator(*dargs,**dkwargs):
        def _wraper(func):
            def _inner_wrapper(*args,**kwargs):
                func(*args,**kwargs)
                time.sleep(dkwargs['time'])
            return _inner_wrapper
        return _wraper

    @sleep_decorator(time=TIMING)
    def send_dangqi_keys(dangqi):
        '''
        发送档期值，下拉点击
        :param dangqi:
        :return:
        '''
        if dangqi:
            b.find_element_by_xpath("(//input[@type='text'])[2]").click()  # 点击：档期下拉
            time.sleep(TIMING)
            print(dangqi)
            b.find_element_by_xpath("(//input[@type='text'])[2]").send_keys(dangqi)  # 档期值输入
            time.sleep(TIMING)
            try:
                b.find_element_by_xpath("(//input[@type='text'])[2]").send_keys(Keys.DOWN)  # 键盘操作：下
                time.sleep(TIMING)
                b.find_element_by_xpath("(//input[@type='text'])[2]").send_keys(Keys.ENTER)  # 键盘操作：回车
                time.sleep(TIMING)
                dangqi_period_date.append({dangqi:b.find_element_by_xpath(
                    "//div[@id='compass-app-body']/div[2]/div/div[2]/div/div/div[2]/span[2]/span[2]").text})
                time.sleep(TIMING)
                b.find_element_by_xpath("(//button[@type='button'])[2]").click()
                time.sleep(TIMING)
            except Exception as e:
                print('档期-%s Exception is %s'%(dangqi,e))
                pass

    def crawler_days(crawl_days,crawl_dates):
        '''
        计算爬取日期
        :param crawl_days: 爬取天数
        :param crawl_dates: 爬取日期
        :return: 所有的爬取的日期
        '''
        today = datetime.date.today()
        str2date = lambda x:datetime.datetime.strptime(x, "%Y-%m-%d").date()
        date2str = lambda x:x.strftime("%Y-%m-%d")
        if crawl_days != 0:
            date_of_days = [today - datetime.timedelta(days=days) for days in range(1,crawl_days+1)]
            return set(map(date2str,date_of_days) + crawl_dates)
        else:
            return crawl_dates

    crawlDates = crawler_days(crawlDays, crawlDates)
    print('craw dates:',crawlDates)
    all_of_dangqi = set()
    for i,brand in enumerate(brands):
        # print('brand %s'%brand)
        try:
            b.find_element_by_xpath("//input[@type='text']").click()
            b.find_element_by_xpath("//input[@type='text']").send_keys(brand)
            time.sleep(TIMING)
            if i != 0:
                b.find_element_by_xpath("//input[@type='text']").send_keys(Keys.DOWN)   #键盘操作：下
                b.find_element_by_xpath("//input[@type='text']").send_keys(Keys.ENTER)  #键盘操作：回车
                time.sleep(TIMING)
        except:pass

        j = 1
        b.find_element_by_xpath("(//input[@type='text'])[2]").click()  # 点击：档期下拉
        time.sleep(TIMING)
        options = list()    #档期选项初始化，清空选项
        while True:
            try:
                options.append(b.find_element_by_xpath("//div[5]/div/div/ul/li[" + str(j) + "]/span").text)
                j += 1
            except Exception as e:
                print('ID-%d dangqi Exception %s'%(j,e))
                b.find_element_by_xpath("(//input[@type='text'])[2]").click()  # 点击：档期下拉
                break


        if crawlDates:  #指定日期
            for crawlDate in crawlDates:    #按日期爬取
                crawlDate = crawlDate.replace('-','')
                date_of_dangqi = set(map(lambda x:x if crawlDate in x else None,options))   #指定日期档期
                # print(date_of_dangqi)
                all_of_dangqi.update(date_of_dangqi)
                map(send_dangqi_keys,date_of_dangqi)
        else:
            raise Exception("请指定一个可用的爬取日期")
            break

    print('sales download end')

    delete_duplicate_files(rawFileSavePath) # 删除重复下载文件
    b.close()
    # for targetOption in all_of_dangqi:
    #     if targetOption:
    #         while True:
    #             try:
    #                 process_andSaveAs_csv(shop, targetOption, rawFileSavePath, csvSaveRootPath)
    #                 # writeDataBase()
    #                 # 当日任务已完成，更新状态
    #                 dateTaskFile = rawFileSavePath + r'\{}_task.data'.format(datetime.date.today().strftime('%Y-%m-%d'))
    #                 f = open(dateTaskFile,'rb')
    #                 taskDict = pickle.load(f)
    #                 f.close()
    #                 taskDict[targetOption] = 'y'
    #                 f = open(dateTaskFile, 'wb')
    #                 pickle.dump(taskDict, f)
    #                 f.close()
    #                 break
    #             except Exception, e:
    #                 print e
    #                 traceback.print_exc()
    #                 time.sleep(20)


