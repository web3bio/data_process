#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-13 17:52:55
LastEditors: Zella Zhong
LastEditTime: 2024-09-29 16:34:25
FilePath: /data_process/src/jobs/farcaster_process_job.py
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
from utils.address import bytea_to_hex_address, hexstr_to_eth_checksum_address, hexstr_to_solana_address

def determine_network(hex_data):
    if is_address(hex_data):
        return "ethereum", to_normalized_address(hex_data)
    else:
        if len(hex_data) == 66:
            return "solana", hexstr_to_solana_address(hex_data)
    
    return "unknown", hex_data

def extract_address_and_network(claim):
    '''type(claim) is dict'''
    address = claim.get("address")
    return determine_network(address)


class FarcasterProcess(object):
    def __init__(self):
        self.job_name = "farcaster_process_job"
        self.job_type = "cron"

    def update_extras_job_status(self, job_status, check_point=0):
        extras_job_name = "farcaster_extras_job"
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

    def process_farcaster_profile(self):
        farcaster_process_dirs = os.path.join(setting.Settings["datapath"], "farcaster_process")
        if not os.path.exists(farcaster_process_dirs):
            os.makedirs(farcaster_process_dirs)

        farcaster_profile_path = os.path.join(farcaster_process_dirs, "farcaster_profile.csv")
        farcaster_verified_address_path = os.path.join(farcaster_process_dirs, "farcaster_verified_address.csv")
        start = time.time()
        logging.info("processing farcaster_profile start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        # conn.autocommit = True
        cursor = read_conn.cursor()

        try:
            fids_table = "farcaster_fids"
            fids_columns = ["fid", "created_at", "custody_address", "registered_at"]

            fids_sql = "SELECT %s FROM %s" % (",".join(fids_columns), fids_table)
            cursor.execute(fids_sql)
            rows = cursor.fetchall()
            fids_df = pd.DataFrame(rows, columns=fids_columns)

            fnames_table = "farcaster_fnames"
            fnames_columns = ["fid", "fname", "custody_address", "deleted_at"]
            fnames_sql = "SELECT %s FROM %s" % (",".join(fnames_columns), fnames_table)
            cursor.execute(fnames_sql)
            rows = cursor.fetchall()
            fnames_df = pd.DataFrame(rows, columns=fnames_columns)

            userdata_table = "farcaster_user_data"
            userdata_columns = ["fid", "type", "value", "deleted_at"]
            userdata_sql = "SELECT %s FROM %s" % (",".join(userdata_columns), userdata_table)
            cursor.execute(userdata_sql)
            rows = cursor.fetchall()
            userdata_df = pd.DataFrame(rows, columns=userdata_columns)

            # userdata_df need to be flattened to fill avatar, display_name, description
            # fid field from fids_df.fid
            # fname fname field from fnames_df.fname (fids_df join fnames_df on fid)
            # label_name label_name same as fname
            # avatar (fids_df join userdata_df on fid) and when userdata_df.type=1 value is avatar
            # display_name (fids_df join userdata_df on fid) and when userdata_df.type=2 value is display_name
            # description (fids_df join userdata_df on fid) and when userdata_df.type=3 value is description
            # fname (fids_df join userdata_df on fid) and when userdata_df.type=6 value is fname(primary)
            # custody_address field from fids_df.custody_address
            # network, address from verifications_df.claim and call determine_network
            # create_time field from fids_df.registered_at
            # update_time now

            # if deleted_at is not null, fname set to null
            result_df = pd.merge(fids_df, fnames_df[['fid', 'fname', 'deleted_at']], on='fid', how='left')
            result_df['fname'] = result_df.apply(lambda row: None if pd.notnull(row['deleted_at']) else row['fname'], axis=1)
            result_df = result_df[result_df['fname'].notnull()] # NOTICE: remove fname is null

            # group by 'fid' and aggregate 'fname' into a list (ignoring None values)
            alias_df = fnames_df.groupby('fid').agg({
                'fname': lambda x: json.dumps([name for name in x if name is not None])
            }).reset_index()
            result_df = pd.merge(result_df, alias_df, on='fid', how='left', suffixes=('', '_alias'))
            result_df = result_df.rename(columns={'fname_alias': 'alias'})

            avatar_df = userdata_df[(userdata_df['type'] == 1) & (userdata_df['deleted_at'].isnull())].rename(columns={'value': 'avatar'})
            display_name_df = userdata_df[(userdata_df['type'] == 2) & (userdata_df['deleted_at'].isnull())].rename(columns={'value': 'display_name'})
            description_df = userdata_df[(userdata_df['type'] == 3) & (userdata_df['deleted_at'].isnull())].rename(columns={'value': 'description'})
            primary_fname_df = userdata_df[(userdata_df['type'] == 6) & (userdata_df['deleted_at'].isnull())].rename(columns={'value': 'primary_fname'})

            result_df = pd.merge(result_df, display_name_df[['fid', 'display_name', 'deleted_at']], on='fid', how='left', suffixes=('', '_display_name'))
            result_df['display_name'] = result_df.apply(lambda row: None if pd.notnull(row['deleted_at_display_name']) else row['display_name'], axis=1)
            result_df = result_df.drop(columns=['deleted_at_display_name'])

            result_df = pd.merge(result_df, avatar_df[['fid', 'avatar', 'deleted_at']], on='fid', how='left', suffixes=('', '_avatar'))
            result_df['avatar'] = result_df.apply(lambda row: None if pd.notnull(row['deleted_at_avatar']) else row['avatar'] if row['avatar'] else None, axis=1)
            result_df = result_df.drop(columns=['deleted_at_avatar'])

            result_df = pd.merge(result_df, description_df[['fid', 'description', 'deleted_at']], on='fid', how='left', suffixes=('', '_description'))
            result_df['description'] = result_df.apply(lambda row: None if pd.notnull(row['deleted_at_description']) else row['description'] if row['description'] else None, axis=1)
            result_df = result_df.drop(columns=['deleted_at_description'])

            # Filter primary fname for fname
            result_df = pd.merge(result_df, primary_fname_df[['fid', 'primary_fname', 'deleted_at']], on='fid', how='left', suffixes=('', '_primary_fname'))
            result_df['primary_fname'] = result_df.apply(lambda row: None if pd.notnull(row['deleted_at_primary_fname']) else row['primary_fname'] if row['primary_fname'] else None, axis=1)
            result_df = result_df.drop(columns=['deleted_at_primary_fname'])

            # replace primary fname
            result_df = result_df[result_df['primary_fname'].notnull()]
            result_df = result_df.drop(columns=['fname'])
            result_df = result_df.rename(columns={'primary_fname': 'fname'})
            result_df = result_df.drop_duplicates(subset=['fid'], keep='first')

            result_df['label_name'] = result_df['fname'].apply(lambda x: x.split(".")[0] if pd.notnull(x) else None)
            result_df['custody_address'] = result_df['custody_address'].apply(lambda x: bytea_to_hex_address(x) if x else None)
            result_df['update_time'] = datetime.now()

            result_df['create_time'] = pd.to_datetime(result_df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            result_df['registration_time'] = pd.to_datetime(result_df['registered_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
            result_df['update_time'] = pd.to_datetime(result_df['update_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            result_df['delete_time'] = result_df['deleted_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)

            result_df = result_df.drop(columns=['created_at', 'registered_at', 'deleted_at'])

            verifications_table = "farcaster_verifications"
            verifications_columns = ["fid", "claim", "timestamp", "deleted_at"]
            verifications_sql = "SELECT %s FROM %s" % (",".join(verifications_columns), verifications_table)
            cursor.execute(verifications_sql)
            rows = cursor.fetchall()
            verifications_df = pd.DataFrame(rows, columns=verifications_columns)

            # process the claim::jsonb fields
            verifications_df[['network', 'address']] = verifications_df['claim'].apply(lambda claim: pd.Series(extract_address_and_network(claim)))
            verifications_df['create_time'] = pd.to_datetime(verifications_df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
            verifications_df['delete_time'] = verifications_df['deleted_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
            verifications_df['update_time'] = datetime.now()
            verifications_df['update_time'] = pd.to_datetime(verifications_df['update_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

            # select useful columns
            verifications_df = verifications_df[['fid', 'network', 'address', 'timestamp', 'create_time', 'update_time', 'delete_time']]
            # dedup lines
            verifications_df = verifications_df.drop_duplicates(subset=['fid', 'network', 'address'], keep='first')

            # keep the first occurrence of data with timestamp
            profile_address_df = verifications_df[['fid', 'network', 'address', 'timestamp', 'delete_time']].copy()
            profile_address_df = verifications_df.sort_values(by=['fid', 'timestamp'])
            profile_address_df = profile_address_df[profile_address_df['delete_time'].isnull()]
            profile_address_df = profile_address_df.drop_duplicates(subset=['fid'], keep='first')
            profile_address_df = profile_address_df.drop(columns=['timestamp'])

            # signer_address_df join result_df: fill network, address
            result_df = pd.merge(result_df, profile_address_df[['fid', 'network', 'address']], on='fid', how='left')

            result_df = result_df.sort_values(by='fid')
            # export the result_df to a CSV file
            result_df = result_df[['fid', 'fname', 'label_name', 'alias', 'avatar', 'display_name',
                        'description', 'custody_address', 'network', 'address', 
                        'registration_time', 'create_time', 'update_time', 'delete_time']]
            result_df.to_csv(farcaster_profile_path, index=False, quoting=csv.QUOTE_ALL)

            verifications_df = pd.merge(verifications_df, primary_fname_df[['fid', 'primary_fname']], on='fid', how='left')
            verifications_df = verifications_df.rename(columns={'primary_fname': 'fname'})
            verifications_df = verifications_df[['fid', 'fname', 'network', 'address', 'create_time', 'update_time', 'delete_time']]
            verifications_df = verifications_df.sort_values(by='fid')
            verifications_df.to_csv(farcaster_verified_address_path, index=False, quoting=csv.QUOTE_ALL)

            # TODO: add delete operation
            delete_verified_df = verifications_df[verifications_df['delete_time'].notnull()]
            delete_fname_df = fnames_df[fnames_df['deleted_at'].notnull()]
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("processing farcaster_profile end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing farcaster_profile cost: %d", ts_delta)

    def save_farcaster_verified_address(self, dump_batch_size=10000):
        farcaster_process_dirs = os.path.join(setting.Settings["datapath"], "farcaster_process")
        if not os.path.exists(farcaster_process_dirs):
            raise FileNotFoundError(f"No data directory {farcaster_process_dirs}")

        farcaster_verified_address_path = os.path.join(farcaster_process_dirs, "farcaster_verified_address.csv")
        if not os.path.exists(farcaster_verified_address_path):
            raise FileNotFoundError(f"No data path {farcaster_verified_address_path}")

        start = time.time()
        logging.info("saving farcaster_verified_address start at: %s", \
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        insert_verified_address = """
            INSERT INTO farcaster_verified_address (
                fid, fname, network, address, create_time, update_time, delete_time
            ) VALUES %s
            ON CONFLICT (fid, address) DO UPDATE SET
                fname = EXCLUDED.fname,
                network = EXCLUDED.network,
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
            # Loading `farcaster_verified_address`
            with open(farcaster_verified_address_path, 'r', encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)  # Skip the header
                logging.info("[%s] header: %s", farcaster_verified_address_path, header)
                batch = []
                for row in csv_reader:
                    cnt += 1
                    # convert empty strings to None except for 'fid'
                    row = [None if (col == "" and idx != 0) else col for idx, col in enumerate(row)]
                    batch.append(row)
                    if len(batch) >= dump_batch_size:
                        # bulk insert
                        batch_times += 1
                        execute_values(
                            cursor, insert_verified_address, batch, template=None, page_size=dump_batch_size
                        )
                        logging.info("Upserted[farcaster_verified_address] batch with size [%d], batch_times %d", len(batch), batch_times)
                        batch = []
                # remaining
                if batch:
                    batch_times += 1
                    execute_values(
                        cursor, insert_verified_address, batch, template=None, page_size=len(batch)
                    )
                    logging.info("Upserted[farcaster_verified_address] batch with size [%d], batch_times %d", len(batch), batch_times)
            os.rename(farcaster_verified_address_path, farcaster_verified_address_path + ".finished")
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            write_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("saving farcaster_verified_address end at: %s", \
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("saving farcaster_verified_address cost: %d", ts_delta)

    def save_farcaster_profile(self, dump_batch_size=10000):
        farcaster_process_dirs = os.path.join(setting.Settings["datapath"], "farcaster_process")
        if not os.path.exists(farcaster_process_dirs):
            raise FileNotFoundError(f"No data directory {farcaster_process_dirs}")

        farcaster_profile_path = os.path.join(farcaster_process_dirs, "farcaster_profile.csv")
        if not os.path.exists(farcaster_profile_path):
            raise FileNotFoundError(f"No data path {farcaster_profile_path}")

        start = time.time()
        logging.info("saving farcaster_profile start at: %s", \
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        insert_farcaster_profile = """
            INSERT INTO farcaster_profile (
                fid, fname, label_name, alias,
                avatar, display_name, description,
                custody_address, network, address,
                registration_time, create_time, update_time, delete_time
            ) VALUES %s
            ON CONFLICT (fid) DO UPDATE SET
                fname = EXCLUDED.fname,
                label_name = EXCLUDED.label_name,
                alias = EXCLUDED.alias,
                avatar = EXCLUDED.avatar,
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                custody_address = EXCLUDED.custody_address,
                network = EXCLUDED.network,
                address = EXCLUDED.address,
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
            # Loading `farcaster_profile`
            with open(farcaster_profile_path, 'r', encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)  # Skip the header
                logging.info("[%s] header: %s", farcaster_profile_path, header)
                batch = []
                for row in csv_reader:
                    cnt += 1
                    # convert empty strings to None except for 'fid'
                    row = [None if (col == "" and idx != 0) else col for idx, col in enumerate(row)]
                    batch.append(row)
                    if len(batch) >= dump_batch_size:
                        # bulk insert
                        batch_times += 1
                        execute_values(
                            cursor, insert_farcaster_profile, batch, template=None, page_size=dump_batch_size
                        )
                        logging.info("Upserted[farcaster_profile] batch with size [%d], batch_times %d", len(batch), batch_times)
                        batch = []
                # remaining
                if batch:
                    batch_times += 1
                    execute_values(
                        cursor, insert_farcaster_profile, batch, template=None, page_size=len(batch)
                    )
                    logging.info("Upserted[farcaster_profile] batch with size [%d], batch_times %d", len(batch), batch_times)
            os.rename(farcaster_profile_path, farcaster_profile_path + ".finished")
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            write_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("saving farcaster_profile end at: %s", \
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("saving farcaster_profile cost: %d", ts_delta)

    def save_farcaster_social(self, rows):
        insert_farcaster_social_sql = """
            INSERT INTO farcaster_social (
                fid, is_power, follower, following, update_time
            ) VALUES %s
            ON CONFLICT (fid) DO UPDATE SET
                is_power = EXCLUDED.is_power,
                follower = EXCLUDED.follower,
                following = EXCLUDED.following,
                update_time = EXCLUDED.update_time;
        """
        write_conn = psycopg2.connect(setting.PG_DSN["write"])
        write_conn.autocommit = True
        cursor = write_conn.cursor()
        try:
            execute_values(cursor, insert_farcaster_social_sql, rows)
            logging.info("Batch upsert[farcaster_social] completed for {} records.".format(len(rows)))
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            write_conn.close()

    def process_farcaster_extras(self):
        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        cursor = read_conn.cursor()
        fids_set = set()
        try:
            fids_table = "farcaster_fids"
            fids_columns = ["fid"]
            fids_sql = "SELECT %s FROM %s" % (",".join(fids_columns), fids_table)
            cursor.execute(fids_sql)
            rows = cursor.fetchall()
            for row in rows:
                fids_set.add(row[0])
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()

        fids_list = list(fids_set)
        # fids_list = range(0, 500)
        session = Session()
        adapter = LimiterAdapter(per_minute=100, per_second=2)
        session.mount(setting.NEYNAR["api"], adapter)
        headers = {
            "accept": "application/json",
            "api_key": setting.NEYNAR["api_key"],
        }
        per_count = 100
        batch_times = math.ceil(len(fids_list)/per_count)
        for i in range(batch_times):
            update_social = []
            batch_ids = fids_list[i*per_count: (i+1)*per_count]
            limit_fids = ",".join([str(x) for x in batch_ids])
            batch_update_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
            try:
                if batch_times % 1000:
                    self.update_extras_job_status("running")
                uri_bulk_fids = "{}/v2/farcaster/user/bulk?fids={}".format(setting.NEYNAR["api"], limit_fids)
                response = session.get(uri_bulk_fids, headers=headers)
                if response.status_code != 200:
                    logging.warn("farcaster api response failed, batch_time={} {} {}".format(i, response.status_code, response.reason))
                    continue
                data = json.loads(response.text)
                if "users" in data:
                    for user in data["users"]:
                        user_fid = user.get("fid", 0)
                        if user_fid == 0:
                            continue
                        follower = user.get("follower_count", 0)
                        following = user.get("following_count", 0)
                        is_power = user.get("power_badge", False)
                        update_social.append(
                            (user_fid, is_power, follower, following, batch_update_time)
                        )
                    if update_social:
                        logging.info("the last fid=%d", update_social[-1][0])
                        self.save_farcaster_social(update_social)
                    logging.info("update_social batch_time=%d, all_batch=%d", i, batch_times)
            except psycopg2.DatabaseError as db_err:
                logging.exception(db_err)
                raise db_err
            except Exception as ex:
                logging.exception(ex)
                continue
    
    def process_extras(self):
        try:
            self.update_extras_job_status("start")
            self.process_farcaster_extras()
            self.update_extras_job_status("end")
        except Exception as ex:
            logging.exception(ex)
            self.update_extras_job_status("fail")

    def process_pipeline(self):
        try:
            self.update_job_status("start")
            self.process_farcaster_profile()

            self.update_job_status("running")
            self.save_farcaster_profile()
            self.save_farcaster_verified_address()
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

    # FarcasterProcess().process_farcaster_profile()
    # FarcasterProcess().save_farcaster_profile()
    FarcasterProcess().save_farcaster_verified_address()
    # FarcasterProcess().process_farcaster_extras()