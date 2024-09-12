#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-12 19:05:02
LastEditors: Zella Zhong
LastEditTime: 2024-09-12 19:07:43
FilePath: /data_process/src/data_process.py
Description: 
'''
import os
import time
import logging

from dotenv import load_dotenv
load_dotenv()

from apscheduler.schedulers.background import BlockingScheduler, BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import setting
import setting.filelogger as logger

if __name__ == "__main__":
    config = setting.load_settings(env=os.getenv("ENVIRONMENT"))
    if not os.path.exists(config["server"]["log_path"]):
        os.makedirs(config["server"]["log_path"])

    logger.InitLogger(config)
    scheduler = None
    try:
        scheduler = BackgroundScheduler()
        scheduler.start()

        while True:
            time.sleep(60)
            logging.info("just sleep for nothing")

    except (KeyboardInterrupt, SystemExit) as ex:
        scheduler.shutdown()
        logging.exception(ex)
        print('Exit The Job!')
