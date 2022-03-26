#!/usr/bin/env python 
# -*- coding:utf-8 -*-
# @Time : 2022/3/25 1:13
# @Author : Haozheng Li (Liam)
# @Email : hxl1119@case.edu

import time
import logging


def log(func):
    def inner(*args, **kwargs):
        begin_time = time.time()
        logging.debug(' Begin func: {}'.format(func.__name__))
        result = func(*args, **kwargs)
        logging.debug(' End func: {}, time consuming: {}s'.format(func.__name__, time.time()-begin_time))
        return result
    return inner
