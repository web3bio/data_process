#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-12 19:13:50
LastEditors: Zella Zhong
LastEditTime: 2024-09-26 21:11:24
FilePath: /data_process/src/utils/timeutils.py
Description: 
'''
import time
from datetime import datetime

def unix_string_to_datetime(value):
    '''
    description: parse unix_string to datetime format "%Y-%m-%d %H:%M:%S"
    '''
    unix_i64 = int(value)
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(unix_i64))


def iso8601_string_to_datetime(value):
    '''
    description: %Y-%m-%dT%H:%M:%S.%fZ string to datetime format "%Y-%m-%d %H:%M:%S"
    '''
    dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    formatted_date = dt.strftime("%Y-%m-%d %H:%M:%S")
    return formatted_date


def get_unix_milliconds():
    # Get the current time in seconds since the Epoch
    current_time_seconds = time.time()
    # Convert to milliconds by multiplying by 1e6 and converting to an integer
    milliconds = int(current_time_seconds * 1e6)
    return milliconds