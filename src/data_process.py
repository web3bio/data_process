#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-12 19:05:02
LastEditors: Zella Zhong
LastEditTime: 2024-10-17 18:31:37
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

from jobs.farcaster_process_job import FarcasterProcess
from jobs.lens_process_job import LensProcess
from jobs.ens_process_job import ENSProcess
from jobs.clusters_process_job import ClustersProcess
from jobs.basenames_process_job import BasenamesProcess

from jobs.ens_graphdb_job import EnsGraphDB
from jobs.lens_graphdb_job import LensGraphDB
from jobs.farcaster_graphdb_job import FarcasterGraphDB
from jobs.clusters_graphdb_job import ClustersGraphDB


def farcaster_process_job():
    logging.info("Starting farcaster_process_job...")
    FarcasterProcess().process_pipeline()

def farcaster_extras_job():
    logging.info("Starting farcaster_extras_job...")
    FarcasterProcess().process_extras()

def lens_process_job():
    logging.info("Starting lens_process_job...")
    LensProcess().process_pipeline()

def lens_extras_job():
    logging.info("Starting farcaster_extras_job...")
    LensProcess().process_extras()

def ensname_process_job():
    logging.info("Starting ensname_process_job...")
    ENSProcess().process_pipeline()

def clusters_process_job():
    logging.info("Starting clusters_process_job job...")
    ClustersProcess().process_pipeline()

def basenames_process_job():
    logging.info("Starting basenames_process_job job...")
    BasenamesProcess().online_dump()

def ensname_graphdb_job():
    logging.info("Starting ensname_graphdb_job...")
    EnsGraphDB().dumps_to_graphdb()

def lens_graphdb_job():
    logging.info("Starting lens_graphdb_job...")
    LensGraphDB().dumps_to_graphdb()

def farcaster_graphdb_job():
    logging.info("Starting farcaster_graphdb_job...")
    FarcasterGraphDB().dumps_to_graphdb()

def clusters_graphdb_job():
    logging.info("Starting clusters_graphdb_job...")
    ClustersGraphDB().dumps_to_graphdb()


if __name__ == "__main__":
    config = setting.load_settings(env=os.getenv("ENVIRONMENT"))
    if not os.path.exists(config["server"]["log_path"]):
        os.makedirs(config["server"]["log_path"])

    logger.InitLogger(config)
    scheduler = None
    try:
        scheduler = BackgroundScheduler()

        # ENS Job Start
        ensname_process_job_trigger = CronTrigger(
            year="*", month="*", day="*", hour="1", minute="0", second="0"
        )
        scheduler.add_job(
            ensname_process_job,
            trigger=ensname_process_job_trigger,
            id='ensname_process_job'
        )
        # ENS Job End

        # Farcaster Job Start
        farcaster_process_job_trigger = CronTrigger(
            year="*", month="*", day="*", hour="15", minute="0", second="0"
        )
        scheduler.add_job(
            farcaster_process_job,
            trigger=farcaster_process_job_trigger,
            id='farcaster_process_job'
        )
        farcaster_extras_job_trigger = CronTrigger(
            year="*", month="*", day="*", hour="15", minute="30", second="0"
        )
        scheduler.add_job(
            farcaster_extras_job,
            trigger=farcaster_extras_job_trigger,
            id='farcaster_extras_job'
        )
        # Farcaster Job End

        # Clusters Job Start
        clusters_process_job_trigger = CronTrigger(
            year="*", month="*", day="*", hour="18", minute="0", second="0"
        )
        scheduler.add_job(
            clusters_process_job,
            trigger=clusters_process_job_trigger,
            id='clusters_process_job'
        )
        # Clusters Job End

        # Lens Job Start
        lens_process_job_trigger = CronTrigger(
            year="*", month="*", day="*", hour="6", minute="0", second="0"
        )
        scheduler.add_job(
            lens_process_job,
            trigger=lens_process_job_trigger,
            id='lens_process_job'
        )
        lens_extras_job_trigger = CronTrigger(
            year="*", month="*", day="*", hour="6", minute="30", second="0"
        )
        scheduler.add_job(
            lens_extras_job,
            trigger=lens_extras_job_trigger,
            id='lens_extras_job'
        )
        # Lens Job End

        scheduler.start()

        # testing job
        # fetch some history data from 2024-09-19 - 2024-10-01
        basenames_process_job()
        # clusters_process_job()
        # clusters_graphdb_job()
        # farcaster_graphdb_job()
        # clusters_graphdb_job()
        # ensname_graphdb_job()
        # lens_graphdb_job()
        while True:
            time.sleep(60)
            logging.info("just sleep for nothing")

    except (KeyboardInterrupt, SystemExit) as ex:
        scheduler.shutdown()
        logging.exception(ex)
        print('Exit The Job!')
