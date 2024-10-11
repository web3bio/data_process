#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-10-11 12:06:32
LastEditors: Zella Zhong
LastEditTime: 2024-10-11 23:03:26
FilePath: /data_process/src/jobs/clusters_process_job.py
Description: 
'''
import os
import sys
sys.path.append("/".join(os.path.abspath(__file__).split("/")[:-2]))

import io
import ssl
import csv
import copy
import math
import time
import uuid
import json
import hashlib
import logging
import binascii
import psycopg2
import requests
import traceback
import base64
import gzip
import pandas as pd

from datetime import datetime
from urllib.parse import quote
from urllib.parse import unquote
from operator import itemgetter
from psycopg2.extras import execute_values, execute_batch
from multiformats import CID
from eth_utils import keccak, to_bytes, encode_hex, is_address, to_normalized_address

from requests import Session
from requests_ratelimiter import LimiterAdapter

import setting


PAGE_LIMIT = 1000

def address_type_to_network(address_type):
    network = ""
    if address_type == "aptos":
        network = "aptos"
    elif address_type == "evm":
        network = "ethereum"
    elif address_type == "solana":
        network = "solana"
    elif address_type == "dogecoin":
        network = "doge"
    elif address_type == "near":
        network = "near"
    elif address_type == "stacks":
        network = "stacks"
    elif address_type == "tron":
        network = "tron"
    elif address_type == "ton":
        network = "ton"
    elif address_type == "ripple-classic":
        network = "xrpc"
    elif address_type.find("bitcoin") != -1:
        network = "bitcoin"
    elif address_type.find("cosmos") != -1:
        network = "cosmos"
    elif address_type.find("litecoin") != -1:
        network = "litecoin"
    else:
        network = "unknown"

    return network

class ClustersProcess(object):
    def __init__(self):
        self.job_name = "clusters_process_job"
        self.job_type = "cron"

    def update_extras_job_status(self, job_status, check_point=0):
        extras_job_name = "clusters_extras_job"
        extras_job_name_type = "cron"
        job_status_type = 0
        if job_status == "start":
            job_status_type = 0
        if job_status == "running":
            job_status_type = 1
        elif job_status == "end":
            job_status_type = 2
        elif job_status == "fail":
            job_status_type = -1
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        sql_statement = """
        INSERT INTO public.job_status (
        job_name, job_type, check_point, job_status_type, job_status, update_time
        ) VALUES %s
        """
        conn = psycopg2.connect(setting.PG_DSN["write"])
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            values = [(extras_job_name, extras_job_name_type, check_point, job_status_type, job_status, update_time)]
            cursor.execute(sql_statement, values)
        except Exception as ex:
            logging.error("Caught exception during insert")
            raise ex
        finally:
            cursor.close()
            conn.close()
    
    def update_job_status(self, job_status, check_point=0):
        job_status_type = 0
        if job_status == "start":
            job_status_type = 0
        if job_status == "running":
            job_status_type = 1
        elif job_status == "end":
            job_status_type = 2
        elif job_status == "fail":
            job_status_type = -1
        update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        sql_statement = """
        INSERT INTO public.job_status (
        job_name, job_type, check_point, job_status_type, job_status, update_time
        ) VALUES %s
        """
        conn = psycopg2.connect(setting.PG_DSN["write"])
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            values = [(self.job_name, self.job_type, check_point, job_status_type, job_status, update_time)]
            cursor.execute(sql_statement, values)
        except Exception as ex:
            logging.error("Caught exception during insert")
            raise ex
        finally:
            cursor.close()
            conn.close()

    def get_latest_timestamp(self):
        '''
        description: get_lastest_timestamp in clusters_profile
        '''
        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        read_cursor = read_conn.cursor()
        sql_query = "SELECT MAX(update_time) AS max_update_time FROM public.clusters_profile"
        read_cursor.execute(sql_query)
        result = read_cursor.fetchone()
        read_cursor.close()
        read_conn.close()
        if result:
            max_update_time = result[0]
            if max_update_time is None:
                logging.info("Cluster latest_timestampe: 0")
                return 0
            latest_timestamp = int(max_update_time.timestamp())
            logging.info("Cluster latest_timestamp: {}".format(latest_timestamp))
            return latest_timestamp
        else:
            logging.info("Cluster latest_timestampe: 0")
            return 0

    def insert_events(self, upsert_data):
        '''
        description: fetch clusters indexes data save into clusters_profile database
        '''
        sql_statement = """INSERT INTO clusters_profile (
            cluster_id,
            bytes32_address,
            network,
            address,
            address_type,
            is_verified,
            cluster_name,
            name,
            avatar,
            display_name,
            description,
            texts,
            registration_time,
            create_time,
            update_time,
            delete_time
        ) VALUES %s
        ON CONFLICT (cluster_id, address, address_type, cluster_name)
        DO UPDATE SET
            network = EXCLUDED.network,
            update_time = EXCLUDED.update_time;
        """
        if upsert_data:
            write_conn = psycopg2.connect(setting.PG_DSN["write"])
            write_conn.autocommit = True
            cursor = write_conn.cursor()
            try:
                execute_values(cursor, sql_statement, upsert_data)
                logging.info("Clusters Batch insert completed {} records.".format(len(upsert_data)))
            except Exception as ex:
                logging.error("Clusters Caught exception during insert in {}".format(json.dumps(upsert_data)))
                raise ex
            finally:
                cursor.close()
                write_conn.close()
        else:
            logging.debug("No valid Clusters upsert_data to process.")

    def update_events(self, update_data):
        '''
        description: updateWallet clusters events update into database
        '''
        update_sql = """
            UPDATE clusters_profile
            SET
                network = %(network)s,
                name = %(name)s,
                is_verified = %(is_verified)s,
                update_time = %(update_time)s
            WHERE 
                cluster_id = %(cluster_id)s AND address = %(address)s AND address_type = %(address_type)s
        """
        if update_data:
            write_conn = psycopg2.connect(setting.PG_DSN["write"])
            write_conn.autocommit = True
            cursor = write_conn.cursor()
            try:
                execute_batch(cursor, update_sql, update_data)
                logging.info("Clusters Batch update completed for {} records.".format(len(update_data)))
            except Exception as ex:
                logging.error("Clusters Caught exception during update in {}".format(json.dumps(update_data)))
                raise ex
            finally:
                cursor.close()
                write_conn.close()
        else:
            logging.debug("No valid Clusters update_data to process.")

    def delete_events(self, delete_data):
        '''
        description: removeWallet clusters events update into database
        '''
        delete_sql = """
            UPDATE clusters_profile
            SET
                is_verified = %(is_verified)s,
                update_time = %(update_time)s,
                delete_time = %(delete_time)s
            WHERE 
                cluster_id = %(cluster_id)s AND address = %(address)s AND address_type = %(address_type)s
        """
        if delete_data:
            write_conn = psycopg2.connect(setting.PG_DSN["write"])
            write_conn.autocommit = True
            cursor = write_conn.cursor()
            try:
                execute_batch(cursor, delete_sql, delete_data)
                logging.info("Clusters Batch delete completed for {} records.".format(len(delete_data)))
            except Exception as ex:
                logging.error("Clusters Caught exception during delete in {}".format(json.dumps(delete_data)))
                raise ex
            finally:
                cursor.close()
                write_conn.close()
        else:
            logging.debug("No valid Clusters delete_data to process.")

    def process_clusters_profile(self):
        clusters_profile_dirs = os.path.join(setting.Settings["datapath"], "clusters_process")
        if not os.path.exists(clusters_profile_dirs):
            os.makedirs(clusters_profile_dirs)

        clusters_profile_path = os.path.join(clusters_profile_dirs, "clusters_profile.csv")
        start = time.time()
        logging.info("processing clusters_profile start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        url = "{}/v0.1/events".format(setting.CLUSTERS["api"])
        fromTimestamp = 0
        all_count = 0
        batch_count = 0
        max_batch_limit = 65536

        session = Session()
        adapter = LimiterAdapter(per_minute=100, per_second=2)
        session.mount(setting.CLUSTERS["api"], adapter)

        result_columns = [
            "cluster_id", "bytes32_address", "network", "address", "address_type",
            "is_verified", "cluster_name", "name", "avatar", "display_name",
            "description", "texts", "registration_time", "create_time", "update_time", "delete_time"
        ]

        update_wallet_columns = [
            "cluster_id", "bytes32_address", "network", "address", "address_type", "name", "is_verified", "update_time"
        ]

        remove_wallet_columns = [
            "cluster_id", "bytes32_address", "address", "address_type", "is_verified", "update_time", "delete_time"
        ]

        clustername_df = pd.DataFrame(columns=result_columns)
        update_wallet_df = pd.DataFrame(columns=update_wallet_columns)
        remove_wallet_df = pd.DataFrame(columns=remove_wallet_columns)

        for i in range(max_batch_limit):
            try:
                new_url = ""
                if fromTimestamp != 0:
                    new_url = "{}?fromTimestamp={}&limit={}".format(url, fromTimestamp, PAGE_LIMIT)
                else:
                    new_url = url

                if i % 1000:
                    self.update_extras_job_status("running")
                response = session.get(new_url, timeout=120)
                if response.status_code != 200:
                    logging.warn("clusters api response failed, batch_time={} {} {}".format(i, response.status_code, response.reason))
                    continue

                resp = None
                raw_text = response.text
                if raw_text == "null":
                    resp = None
                else:
                    resp = json.loads(response.text)

                if resp is None:
                    break

                total = 0
                upsert_data = []
                upsert_data_dict = {}
                update_wallet = []
                remove_wallet = []
                if "items" in resp:
                    total = len(resp["items"])
                    for item in resp["items"]:
                        event_type = item["eventType"]
                        timestamp = item["timestamp"]
                        fromTimestamp = timestamp  # fromTimestamp = lastTimestamp
                        if event_type == "register":
                            # {
                            #     "eventType": "register",
                            #     "clusterId": 127285,
                            #     "bytes32Address": "0x00000000000000000000000049ec5b29ff322e5c35983d7bcc9aa91468b65f7a",
                            #     "address": "0x49ec5b29ff322e5c35983d7bcc9aa91468b65f7a",
                            #     "addressType": "evm",
                            #     "data": {
                            #         "name": "isaacnewton",
                            #         "weiAmount": 10000000000000000
                            #     },
                            #     "timestamp": 1721821775
                            # }
                            cluster_id = item.get("clusterId", 0)
                            if cluster_id == 0:
                                continue
                            address = item.get("address", None)
                            address_type = item.get("addressType", None)
                            if address_type is None:
                                continue
                            else:
                                if address_type == "":
                                    continue
                            if address is None:
                                continue
                            else:
                                if address == "":
                                    continue

                            network = address_type_to_network(address_type)
                            bytes32_address = item.get("bytes32Address", None)
                            if bytes32_address is None:
                                continue
                            else:
                                if bytes32_address == "":
                                    continue

                            cluster_name = None
                            if "data" in item:
                                if "name" in item["data"]:
                                    cluster_name = item["data"]["name"]
                            if cluster_name is None:
                                continue
                            else:
                                if cluster_name == "":
                                    continue

                            # default fields
                            name = ""
                            is_verified = False
                            avatar = None
                            display_name = None
                            description = None
                            texts = json.dumps({})
                            registration_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))
                            create_time = registration_time
                            update_time = registration_time
                            delete_time = None

                            upsert_data.append({
                                "cluster_id": cluster_id,
                                "bytes32_address": bytes32_address,
                                "network": network,
                                "address": address,
                                "address_type": address_type,
                                "is_verified": is_verified,
                                "cluster_name": cluster_name,
                                "name": name,
                                "avatar": avatar,
                                "display_name": display_name,
                                "description": description,
                                "texts": texts,
                                "registration_time": registration_time,
                                "create_time": create_time,
                                "update_time": update_time,
                                "delete_time": delete_time
                            })
                            # unique_id = "{}-{}-{}-{}".format(cluster_id, address, address_type, cluster_name)
                            # row = (
                            #     cluster_id,
                            #     bytes32_address,
                            #     network,
                            #     address,
                            #     address_type,
                            #     is_verified,
                            #     cluster_name,
                            #     name,
                            #     avatar,
                            #     display_name,
                            #     description,
                            #     texts,
                            #     registration_time,
                            #     create_time,
                            #     update_time,
                            #     delete_time
                            # )
                            # upsert_data_dict[unique_id] = row

                        elif event_type == "updateWallet":
                            # {
                            #     "eventType": "updateWallet",
                            #     "clusterId": 127285,
                            #     "bytes32Address": "0x00000000000000000000000049ec5b29ff322e5c35983d7bcc9aa91468b65f7a",
                            #     "address": "0x49ec5b29ff322e5c35983d7bcc9aa91468b65f7a",
                            #     "addressType": "evm",
                            #     "data": {
                            #         "name": "main",
                            #         "isVerified": 1
                            #     },
                            #     "timestamp": 1721821781
                            # }
                            cluster_id = item.get("clusterId", 0)
                            if cluster_id == 0:
                                continue
                            address = item.get("address", None)
                            address_type = item.get("addressType", None)
                            if address_type is None:
                                continue
                            else:
                                if address_type == "":
                                    continue
                            if address is None:
                                continue
                            else:
                                if address == "":
                                    continue

                            network = address_type_to_network(address_type)
                            bytes32_address = item.get("bytes32Address", None)
                            if bytes32_address is None:
                                continue
                            else:
                                if bytes32_address == "":
                                    continue

                            is_verified = False
                            name = None
                            if "data" in item:
                                name = item["data"].get("name", None)
                                is_verified_int = item["data"].get("isVerified", 0)
                                if is_verified_int == 1:
                                    is_verified = True
                            if name is None:
                                continue
                            else:
                                if name == "":
                                    continue

                            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))
                            update_wallet.append({
                                "cluster_id": cluster_id,
                                "bytes32_address": bytes32_address,
                                "network": network,
                                "address": address,
                                "address_type": address_type,
                                "name": name,
                                "is_verified": is_verified,
                                "update_time": update_time,
                            })

                        elif event_type == "removeWallet":
                            # data = None
                            # {
                            #     "eventType": "removeWallet",
                            #     "clusterId": 50854,
                            #     "bytes32Address": "0x165b9d2567f38fd96a69695e9ebfb283570de77e15e054a4499d8a42abcfbefc",
                            #     "address": "2WGzbosx5SbSPbj4om81SnVfmtz3stjdPbp8z9yyy1As",
                            #     "addressType": "solana",
                            #     "data": null,
                            #     "timestamp": 1721872544
                            # }
                            cluster_id = item.get("clusterId", 0)
                            if cluster_id == 0:
                                continue
                            address = item.get("address", None)
                            address_type = item.get("addressType", None)
                            if address_type is None:
                                continue
                            else:
                                if address_type == "":
                                    continue
                            if address is None:
                                continue
                            else:
                                if address == "":
                                    continue
                            network = address_type_to_network(address_type)
                            bytes32_address = item.get("bytes32Address", None)
                            if bytes32_address is None:
                                continue
                            else:
                                if bytes32_address == "":
                                    continue

                            update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(timestamp)))
                            delete_time = update_time
                            remove_wallet.append({
                                "cluster_id": cluster_id,
                                "bytes32_address": bytes32_address,
                                "network": network,
                                "address": address,
                                "address_type": address_type,
                                "is_verified": False,
                                "update_time": update_time,
                                "delete_time": delete_time,
                            })

                batch_count += 1
                all_count += total

                if upsert_data:
                    temp_clustername_df = pd.DataFrame(upsert_data, columns=result_columns)
                    clustername_df = pd.concat([clustername_df, temp_clustername_df], ignore_index=True)

                if update_wallet:
                    temp_update_wallet_df = pd.DataFrame(update_wallet, columns=update_wallet_columns)
                    update_wallet_df = pd.concat([update_wallet_df, temp_update_wallet_df], ignore_index=True)

                if remove_wallet:
                    temp_remove_wallet_df = pd.DataFrame(remove_wallet, columns=remove_wallet_columns)
                    remove_wallet_df = pd.concat([remove_wallet_df, temp_remove_wallet_df], ignore_index=True)

                # upsert_data = upsert_data_dict.values()
                # for k, v in upsert_data_dict.items():
                #     if k.startswith("127287"):
                #         print(k, v)
                # self.insert_events(upsert_data)
                # self.update_events(update_wallet)
                # self.delete_events(remove_wallet)

                logging.info("Fetching Clusters event batch_count: %d, all_count=%d, fromTimestamp=%d", batch_count, all_count, fromTimestamp)

                if "nextPage" in resp:
                    if resp["nextPage"] == "" and batch_count > 0:
                        break
                if total < PAGE_LIMIT:
                    break

            except Exception as ex:
                error_msg = traceback.format_exc()
                logging.error("Fetch clusters: Exception occurs error! {}".format(error_msg))
                logging.exception(ex)
                batch_count += 1
                continue

        clustername_df = clustername_df.drop_duplicates(subset=['cluster_id', 'address', 'address_type', 'cluster_name'], keep='first')
        merged_df = pd.merge(
            update_wallet_df,
            clustername_df[['cluster_id', 'cluster_name', 'registration_time', 'create_time']],
            on=['cluster_id'],
            how='left',
            suffixes=('', '_clustername')
        )

        merged_df = pd.merge(
            merged_df,
            remove_wallet_df[['cluster_id', 'address', 'address_type', 'is_verified', 'update_time', 'delete_time']],
            on=['cluster_id', 'address', 'address_type'],
            how='left',
            suffixes=('', '_remove')
        )

        merged_df['is_verified'] = merged_df.apply(
            lambda row: row['is_verified_remove'] if pd.notnull(row['delete_time']) else row['is_verified'],
            axis=1
        )

        merged_df['update_time'] = merged_df.apply(
            lambda row: row['update_time_remove'] if pd.notnull(row['delete_time']) else row['update_time'],
            axis=1
        )

        merged_df = merged_df[merged_df["cluster_name"].notnull()]
        merged_df = merged_df.drop_duplicates(subset=['cluster_id', 'address', 'address_type', 'cluster_name'], keep='last')
        merged_df = merged_df.sort_values(by='cluster_id')
        merged_df = merged_df[[
            "cluster_id", "bytes32_address", "network", "address", "address_type", "is_verified", \
            "cluster_name", "name", "registration_time", "create_time", "update_time", "delete_time"]]
        merged_df.to_csv(clusters_profile_path, index=False, quoting=csv.QUOTE_ALL)

        end = time.time()
        ts_delta = end - start
        logging.info("processing clusters_profile end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing clusters_profile cost: %d", ts_delta)

    def save_clusters_profile(self, dump_batch_size=10000):
        clusters_profile_dirs = os.path.join(setting.Settings["datapath"], "clusters_process")
        if not os.path.exists(clusters_profile_dirs):
            raise FileNotFoundError(f"No data directory {clusters_profile_dirs}")

        clusters_profile_path = os.path.join(clusters_profile_dirs, "clusters_profile.csv")
        if not os.path.exists(clusters_profile_path):
            raise FileNotFoundError(f"No data path {clusters_profile_path}")
        start = time.time()
        logging.info("saving clusters_profile start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        insert_clusters_profile = """
        INSERT INTO clusters_profile (
            cluster_id,
            bytes32_address,
            network,
            address,
            address_type,
            is_verified,
            cluster_name,
            name,
            registration_time,
            create_time,
            update_time,
            delete_time
        ) VALUES %s
        ON CONFLICT (cluster_id, address, address_type, cluster_name)
        DO UPDATE SET
            bytes32_address = EXCLUDED.bytes32_address,
            network = EXCLUDED.network,
            is_verified = EXCLUDED.is_verified,
            name = EXCLUDED.name,
            registration_time = EXCLUDED.registration_time,
            create_time = EXCLUDED.create_time,
            update_time = EXCLUDED.update_time,
            delete_time = EXCLUDED.delete_time;
        """

        write_conn = psycopg2.connect(setting.PG_DSN["write"])
        write_conn.autocommit = True
        cursor = write_conn.cursor()

        cnt = 0
        batch = []
        batch_times = 0
        try:
            # Loading `clusters_profile`
            with open(clusters_profile_path, 'r', encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)  # Skip the header
                logging.info("[%s] header: %s", clusters_profile_path, header)
                batch = []
                for row in csv_reader:
                    cnt += 1
                    # convert empty strings to None except for 'clusters_id'
                    row = [None if (col == "" and idx != 0) else col for idx, col in enumerate(row)]
                    if row[6] is None:
                        # index: 6 is `cluster_name``
                        continue
                    batch.append(row)
                    if len(batch) >= dump_batch_size:
                        # bulk insert
                        batch_times += 1
                        execute_values(
                            cursor, insert_clusters_profile, batch, template=None, page_size=dump_batch_size
                        )
                        logging.info("Upserted[cluster_profile] batch with size [%d], batch_times %d", len(batch), batch_times)
                        batch = []

                # remaining
                if batch:
                    batch_times += 1
                    execute_values(
                        cursor, insert_clusters_profile, batch, template=None, page_size=len(batch)
                    )
                    logging.info("Upserted[cluster_profile] batch with size [%d], batch_times %d", len(batch), batch_times)
            os.rename(clusters_profile_path, clusters_profile_path + ".finished")

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            write_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("saving clusters_profile end at: %s", \
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("saving clusters_profile cost: %d", ts_delta)

    def process_pipeline(self):
        try:
            # self.update_job_status("start")
            self.process_clusters_profile()
            self.save_clusters_profile()
            # self.update_job_status("running")
            # self.update_job_status("end")

        except Exception as ex:
            logging.exception(ex)
            # self.update_job_status("fail")

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    print(os.getenv("ENVIRONMENT"))
    import setting.filelogger as logger
    config = setting.load_settings(env=os.getenv("ENVIRONMENT"))
    logger.InitLogger(config)

    # ClustersProcess().get_latest_timestamp()

    ClustersProcess().process_pipeline()