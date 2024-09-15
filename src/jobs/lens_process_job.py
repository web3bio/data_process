#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-13 17:53:04
LastEditors: Zella Zhong
LastEditTime: 2024-09-14 23:23:47
FilePath: /data_process/src/jobs/lens_process_job.py
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
from utils.timeutils import iso8601_string_to_datetime


QUERY_PROFILES_BY_IDS = """
query ProfileQuerrry {
  profiles(request: {where: { profileIds: [%s]}}) {
    items {
      id
      createdAt
      metadata {
        displayName
        bio
        attributes {
          key
          value
        }
        picture {
          ... on ImageSet {
            optimized {
              uri
            }
          }
        }
        coverPicture {
          optimized {
            uri
          }
        }
      }
      txHash
      stats {
        id
        followers
        following
      }
    }
  }
}
"""


def process_profile_config(config_json_list):
    config_dict = {}
    if config_json_list is None:
        return json.dumps(config_dict)

    for item in config_json_list:
        key = item['key']
        value = item['value']
        kk = quote(key, 'utf-8')  # convert string to url-encoded
        vv = quote(value, 'utf-8')  # convert string to url-encoded
        config_dict[kk] = vv

    return json.dumps(config_dict)


class LensProcess(object):
    def __init__(self):
        self.job_name = "lens_process_job"
        self.job_type = "cron"

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

    def update_extras_job_status(self, job_status, check_point=0):
        extras_job_name = "lens_extras_job"
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

    def process_lens_profile(self):
        lens_process_dirs = os.path.join(setting.Settings["datapath"], "lens_process")
        if not os.path.exists(lens_process_dirs):
            os.makedirs(lens_process_dirs)

        lens_profile_path = os.path.join(lens_process_dirs, "lens_profile.csv")
        start = time.time()
        logging.info("processing lens_profile start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        cursor = read_conn.cursor()

        try:
            lens_profile_handles_v2 = "lens_profile_handles_v2"
            lens_profile_handles_v2_columns = [
                "lens_token_id",
                "profile_id",
                "handles_id",
                "handles_name",
                "namespace",
                "metadata",
                "display_name",
                "status",
                "picture",
                "cover_picture",
                "profile_config",
                "lens_address",
                "created_at",
                "handles_token_id"
            ]
            select_sql = "SELECT %s FROM %s" % (",".join(lens_profile_handles_v2_columns), lens_profile_handles_v2)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            orignal_df = pd.DataFrame(rows, columns=lens_profile_handles_v2_columns)

            # profile_id: orignal_df.lens_token_id
            # profile_id_hex: orignal_df.profile_id
            # name: orignal_df.handles_name + ".lens"
            # namespace orignal_df.namespace
            # label_name orignal_df.handles_name
            # full_handle orignal_df.namespace + "/"+ orignal_df.handles_name
            # is_primary if orignal_df.status=0: is_primary=True, else orignal_df.status=1: is_primary=False
            # handle_node_id orignal_df.handles_id
            # handle_token_id orignal_df.handles_token_id
            # avatar orignal_df.avatar
            # display_name orignal_df.display_name
            # cover_picture orignal_df.cover_picture
            # network polygon
            # address orignal_df.lens_address
            # create_time orignal_df.created_at format 'yyyy-mm-dd HH:MM:SS'
            # update_time now
            # texts need call process_profile_config()
            # orignal_df.profile_config = [{"key": "isBeta", "value": "true"}, {"key": "location", "value": "London"}, {"key": "website", "value": "www.dylandrake.co.uk"}, {"key": "twitter", "value": "@dylndrake"}, 
            # {"key": "hasPrideLogo", "value": ""}, {"key": "statusEmoji", "value": ""}, {"key": "statusMessage", "value": "we satoshi"}, {"key": "app", "value": "Lenster"}]
            # need process to {"isBeta": "true", "location": "London", "website": "www.dylandrake.co.uk", "twitter": "%40dylndrake", "hasPrideLogo": "", "statusEmoji": "", "statusMessage": "we satoshi", "app": "Lenster"}

            result_df = pd.DataFrame()
            result_df['profile_id'] = orignal_df['lens_token_id'].astype('int64')
            result_df['profile_id_hex'] = orignal_df['profile_id']
            result_df['name'] = orignal_df['handles_name'] + ".lens"
            result_df['handle_name'] = orignal_df['handles_name']
            result_df['namespace'] = orignal_df['namespace']
            result_df['label_name'] = orignal_df['handles_name']
            result_df['is_primary'] = orignal_df['status'].apply(lambda x: True if x == 0 else False)
            result_df['handle_node_id'] = orignal_df['handles_id']
            result_df['handle_token_id'] = orignal_df['handles_token_id']
            result_df['avatar'] = orignal_df['picture']
            result_df['display_name'] = orignal_df['display_name']
            result_df['cover_picture'] = orignal_df['cover_picture']
            result_df['network'] = 'polygon'
            result_df['address'] = orignal_df['lens_address']
            result_df['texts'] = orignal_df['profile_config'].apply(process_profile_config)
            result_df['create_time'] = pd.to_datetime(orignal_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            result_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # saving profile
            result_df = result_df.sort_values(by='profile_id')
            result_df.to_csv(lens_profile_path, index=False, quoting=csv.QUOTE_ALL)

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("processing lens_profile end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing lens_profile cost: %d", ts_delta)

    def save_lens_profile(self, dump_batch_size=10000):
        lens_process_dirs = os.path.join(setting.Settings["datapath"], "lens_process")
        if not os.path.exists(lens_process_dirs):
            raise FileNotFoundError(f"No data directory {lens_process_dirs}")

        lens_profile_path = os.path.join(lens_process_dirs, "lens_profile.csv")
        if not os.path.exists(lens_profile_path):
            raise FileNotFoundError(f"No data path {lens_profile_path}")
        start = time.time()
        logging.info("saving lens_profile start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        insert_lens_profile = """
            INSERT INTO lensv2_profile (
                profile_id, profile_id_hex, name, handle_name, namespace,
                label_name, is_primary, handle_node_id, handle_token_id,
                avatar, display_name, cover_picture, network, address,
                texts, create_time, update_time
            ) VALUES %s
            ON CONFLICT (profile_id) DO UPDATE SET
                name = EXCLUDED.name,
                handle_name = EXCLUDED.handle_name,
                namespace = EXCLUDED.namespace,
                label_name = EXCLUDED.label_name,
                is_primary = EXCLUDED.is_primary,
                handle_node_id = EXCLUDED.handle_node_id,
                handle_token_id = EXCLUDED.handle_token_id,
                avatar = EXCLUDED.avatar,
                display_name = EXCLUDED.display_name,
                cover_picture = EXCLUDED.cover_picture,
                network = EXCLUDED.network,
                address = EXCLUDED.address,
                texts = EXCLUDED.texts,
                create_time = EXCLUDED.create_time,
                update_time = EXCLUDED.update_time;
        """

        write_conn = psycopg2.connect(setting.PG_DSN["write"])
        write_conn.autocommit = True
        cursor = write_conn.cursor()

        cnt = 0
        batch = []
        batch_times = 0
        try:
            # Loading `len_profile`
            with open(lens_profile_path, 'r', encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)  # Skip the header
                logging.info("[%s] header: %s", lens_profile_path, header)
                batch = []
                for row in csv_reader:
                    cnt += 1
                    # convert empty strings to None except for 'profile_id'
                    row = [None if (col == "" and idx != 0) else col for idx, col in enumerate(row)]
                    batch.append(row)
                    if len(batch) >= dump_batch_size:
                        # bulk insert
                        batch_times += 1
                        execute_values(
                            cursor, insert_lens_profile, batch, template=None, page_size=dump_batch_size
                        )
                        logging.info("Upserted[lensv2_profile] batch with size [%d], batch_times %d", len(batch), batch_times)
                        batch = []

                # remaining
                if batch:
                    batch_times += 1
                    execute_values(
                        cursor, insert_lens_profile, batch, template=None, page_size=len(batch)
                    )
                    logging.info("Upserted[lensv2_profile] batch with size [%d], batch_times %d", len(batch), batch_times)
            os.rename(lens_profile_path, lens_profile_path + ".finished")

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            write_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("saving lens_profile end at: %s", \
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("saving lens_profile cost: %d", ts_delta)

    def save_lens_social(self, rows):
        insert_lens_social_sql = """
            INSERT INTO lensv2_social (
                profile_id, follower, following, update_time
            ) VALUES %s
            ON CONFLICT (profile_id) DO UPDATE SET
                follower = EXCLUDED.follower,
                following = EXCLUDED.following,
                update_time = EXCLUDED.update_time;
        """
        write_conn = psycopg2.connect(setting.PG_DSN["write"])
        write_conn.autocommit = True
        cursor = write_conn.cursor()
        try:
            execute_values(cursor, insert_lens_social_sql, rows)
            logging.info("Batch upsert[lens_social] completed for {} records.".format(len(rows)))
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            write_conn.close()

    def update_lens_metadata(self, update_data):
        update_sql = """
            UPDATE lensv2_profile
            SET
                avatar = %(avatar)s,
                display_name = %(display_name)s,
                description = %(description)s,
                cover_picture = %(cover_picture)s,
                tx_hash = %(tx_hash)s,
                texts = %(texts)s,
                registration_time = %(registration_time)s,
                update_time = %(update_time)s
            WHERE 
                profile_id = %(profile_id)s;
        """
        if update_data:
            write_conn = psycopg2.connect(setting.PG_DSN["write"])
            write_conn.autocommit = True
            cursor = write_conn.cursor()
            try:
                execute_batch(cursor, update_sql, update_data)
                logging.info("Batch update completed for {} records.".format(len(update_data)))
            except Exception as ex:
                logging.exception(ex)
                raise ex
            finally:
                cursor.close()
                write_conn.close()
        else:
            logging.debug("No valid update_data to process.")

    def process_lens_extras(self):
        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        cursor = read_conn.cursor()
        profile_ids_list = []
        try:
            lensv2_table = "lensv2_profile"
            lensv2_columns = ["profile_id_hex"]
            select_sql = "SELECT %s FROM %s" % (",".join(lensv2_columns), lensv2_table)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            for row in rows:
                profile_ids_list.append(row[0])
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()

        session = Session()
        adapter = LimiterAdapter(per_minute=100, per_second=2)
        session.mount(setting.LENS["api"], adapter)
        headers = {
            "accept": "application/json",
            "user-agent": "spectaql",     # Add user agent header to avoid firewall
        }
        per_count = 50
        batch_times = math.ceil(len(profile_ids_list)/per_count)
        for i in range(batch_times):
            update_social = []
            update_data = []
            batch_ids = profile_ids_list[i*per_count: (i+1)*per_count]
            batch_update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            try:
                if batch_times % 1000:
                    self.update_extras_job_status("running")
                query_vars = ", ".join(["\"" + id + "\"" for id in batch_ids])
                payload = QUERY_PROFILES_BY_IDS % query_vars
                response = session.post(url=setting.LENS["api"], json={"query": payload}, headers=headers, timeout=30)
                if response.status_code != 200:
                    logging.warn("lens api response failed, batch_time={} {} {}".format(i, response.status_code, response.reason))
                    continue
                data = json.loads(response.text)
                if "data" in data:
                    if "profiles" in data["data"]:
                        if "items" in data["data"]["profiles"]:
                            for item in data["data"]["profiles"]["items"]:
                                profile_id = item.get("id", "")
                                if profile_id == "":
                                    continue
                                update_item = {
                                    "profile_id": profile_id,
                                    "avatar": None,
                                    "display_name": None,
                                    "description": None,
                                    "cover_picture": None,
                                    "tx_hash": None,
                                    "texts": json.dumps({}),
                                    "registration_time": None,
                                    "update_time": batch_update_time,
                                }
                                created_at = item.get("createdAt", None)
                                if created_at is not None:
                                    update_item["registration_time"] = iso8601_string_to_datetime(created_at)

                                metadata = item.get("metadata", None)
                                if metadata is not None:
                                    update_item["display_name"] = metadata.get("displayName", None)
                                    update_item["description"]  = metadata.get("bio", None)
                                    attributes = metadata.get("attributes", None)
                                    update_item["texts"] = process_profile_config(attributes)

                                    picture = metadata.get("picture", None)
                                    if picture is not None:
                                        optimized = picture.get("optimized", None)
                                        if optimized is not None:
                                            update_item["avatar"] = optimized.get("uri", None)

                                    cover_picture = metadata.get("coverPicture", None)
                                    if cover_picture is not None:
                                        optimized = cover_picture.get("optimized", None)
                                        if optimized is not None:
                                            update_item["cover_picture"] = optimized.get("uri", None)

                                update_item["tx_hash"] = item.get("txHash", None)
                                update_data.append(update_item)
                                stats = item.get("stats", None)
                                if stats is not None:
                                    if "followers" in stats and "following" in stats:
                                        update_social.append(
                                            (profile_id, stats["followers"], stats["following"], batch_update_time))
                        self.update_lens_metadata(update_data)
                        self.save_lens_social(update_social)
            except psycopg2.DatabaseError as db_err:
                logging.exception(db_err)
                raise db_err
            except Exception as ex:
                logging.exception(ex)
                logging.error(batch_ids)
                continue

    def process_extras(self):
        try:
            self.update_extras_job_status("start")
            self.process_lens_extras()
            self.update_extras_job_status("end")
        except Exception as ex:
            logging.exception(ex)
            self.update_extras_job_status("fail")

    def process_pipeline(self):
        try:
            self.update_job_status("start")
            self.process_lens_profile()

            self.update_job_status("running")
            self.save_lens_profile()
            self.update_job_status("end")

        except Exception as ex:
            logging.exception(ex)
            self.update_job_status("fail")

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    print(os.getenv("ENVIRONMENT"))
    import setting.filelogger as logger
    config = setting.load_settings(env=os.getenv("ENVIRONMENT"))
    logger.InitLogger(config)

    # LensProcess().process_lens_extras()
    # LensProcess().process_lens_profile()
    # LensProcess().save_lens_profile()