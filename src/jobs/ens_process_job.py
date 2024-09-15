#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-12 19:11:40
LastEditors: Zella Zhong
LastEditTime: 2024-09-15 08:48:47
FilePath: /data_process/src/jobs/ens_process_job.py
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

from utils.domain import compute_namehash
from utils.timeutils import unix_string_to_datetime
from utils.coin_type import decode_cointype_address
from utils.contenthash import decode_contenthash


def process_compute_namehash(ens_name):
    if ens_name is None:
        return None, None, None, None
    parent_node, label, erc721_token_id, erc1155_token_id, node = compute_namehash(ens_name)
    return parent_node, label, erc721_token_id, erc1155_token_id

# def process_resolved_records(row):
#     if pd.isnull(row['resolved_records']):
#         # If resolved_records is missing, initialize it with {"60": resolved_address}
#         if row['resolved_address'] is not None:
#             if row['resolved_address'] != "":
#                 coin_type_name, decode_address = decode_cointype_address("60", row['resolved_address'])
#                 return json.dumps({coin_type_name: decode_address})
#     else:
#         # resolved_records is a dict (JSONB), update the "60" field with resolved_address
#         resolved_records = row['resolved_records']
#         new_resolved_records = {}
#         for coin_type, raw_hex in resolved_records.items():
#             coin_type_name, decode_address = decode_cointype_address(coin_type, raw_hex)
#             new_resolved_records[coin_type_name] = decode_address
#             if coin_type == "60":
#                 if row['resolved_address'] is not None:
#                     if row['resolved_address'] != "":
#                         coin_type_name, decode_address = decode_cointype_address("60", row['resolved_address'])
#                         new_resolved_records[coin_type_name] = decode_address
#         return json.dumps(new_resolved_records)
#     return json.dumps({})

def process_resolved_records(row):
    if pd.isnull(row['resolved_records']):
        if row['resolved_address'] is not None and row['resolved_address'] != "":
            coin_type_name, decode_address = decode_cointype_address("60", row['resolved_address'])
            if decode_address:
                return json.dumps({coin_type_name: decode_address})
        return json.dumps({})

    resolved_records = row['resolved_records']
    new_resolved_records = {}
    for coin_type, raw_hex in resolved_records.items():
        coin_type_name, decode_address = decode_cointype_address(coin_type, raw_hex)
        if decode_address:
            new_resolved_records[coin_type_name] = decode_address

    if row['resolved_address'] is not None and row['resolved_address'] != "":
        coin_type_name, decode_address = decode_cointype_address("60", row['resolved_address'])
        if decode_address:
            new_resolved_records[coin_type_name] = decode_address
    
    return json.dumps(new_resolved_records)

def process_contenthash(row):
    if row is None:
        return None
    try:
        decoded = decode_contenthash(row)
        return decoded
    except Exception:
        return row # return raw value

def aggregate_texts(group):
    return json.dumps({quote(row['text_key'], 'utf-8'): quote(row['text_value'], 'utf-8') for _, row in group.iterrows()})

# ETH_NODE The node hash of "eth"
ETH_NODE = "0x93cdeb708b7545dc668eb9280176169d1c33cfd8ed6f04690a0bcc88a93fc4ae"

# ADDR_REVERSE_NODE The node hash of "addr.reverse"
ADDR_REVERSE_NODE = "0x91d1777781884d03a6757a803996e38de2a42967fb37eeaca72729271025a9e2"


class ENSProcess(object):
    def __init__(self):
        self.job_name = "ens_process_job"
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
        sql_statement = f"""
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

    def ens_domain_contenthash(self, batch_size=100000):
        table_name = "ens_domain_contenthash"
        ens_metadata_dirs = os.path.join(setting.Settings["datapath"], "ens_metadata")
        if not os.path.exists(ens_metadata_dirs):
            os.makedirs(ens_metadata_dirs)

        start = time.time()
        logging.info("loading offline ENS metadata(%s) start at: %s", \
                      table_name,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        conn = psycopg2.connect(setting.PG_DSN["read"])
        conn.autocommit = True
        cursor = conn.cursor()

        ens_domain_contenthash_path = os.path.join(ens_metadata_dirs, f"{table_name}.csv")
        try:
            with open(ens_domain_contenthash_path, 'w', encoding="utf-8") as data_fw:
                offset = 0
                first_batch = True
                while True:
                    # Use batch-based query with LIMIT and OFFSET
                    copy_sql = f"""
                        COPY (
                            SELECT namenode, contenthash, update_time
                            FROM public.{table_name}
                            LIMIT {batch_size} OFFSET {offset}
                        ) TO STDOUT WITH CSV HEADER QUOTE '"'
                    """
                    try:
                        if first_batch:
                            # Include the header for the first batch
                            cursor.copy_expert(copy_sql, data_fw)
                            first_batch = False
                        else:
                            # Skip the header for subsequent batches
                            copy_sql_no_header = copy_sql.replace("HEADER", "")
                            cursor.copy_expert(copy_sql_no_header, data_fw)

                    except psycopg2.OperationalError as ex:
                        if ex.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                            error_msg = traceback.format_exc()
                            logging.error("Serialization failure, skipping batch starting at offset\
                                          %d, %s", offset, error_msg)
                            logging.exception(ex)
                            break
                        else:
                            raise ex

                    logging.info("successfully written(%s) rowcount=[%d],offset=[%d]",
                                 table_name, cursor.rowcount, offset)
                    if cursor.rowcount < batch_size:
                        break  # No more rows to fetch, exit the loop

                    offset += batch_size

            logging.info("successfully written to %s", ens_domain_contenthash_path)

        except Exception as ex:
            # error_msg = traceback.format_exc()
            # logging.error(error_msg)
            logging.exception(ex)
            raise ex

        end = time.time()
        ts_delta = end - start
        logging.info("loading offline ENS metadata(%s) end at: %s", \
                      table_name,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("loading offline ENS metadata(%s) cost: %d", table_name, ts_delta)
        cursor.close()
        conn.close()

    def ens_domain_text(self, batch_size=100000):
        table_name = "ens_domain_text"
        ens_metadata_dirs = os.path.join(setting.Settings["datapath"], "ens_metadata")
        if not os.path.exists(ens_metadata_dirs):
            os.makedirs(ens_metadata_dirs)

        start = time.time()
        logging.info("loading offline ENS metadata(%s) start at: %s", \
                      table_name,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        conn = psycopg2.connect(setting.PG_DSN["read"])
        conn.autocommit = True
        cursor = conn.cursor()

        ens_domain_text_path = os.path.join(ens_metadata_dirs, f"{table_name}.csv")
        try:
            with open(ens_domain_text_path, 'w', encoding="utf-8") as data_fw:
                offset = 0
                first_batch = True
                while True:
                    # Use batch-based query with LIMIT and OFFSET
                    copy_sql = f"""
                        COPY (
                            SELECT node_id, key_hash, text_key, text_value, created_at, updated_at
                            FROM public.{table_name}
                            LIMIT {batch_size} OFFSET {offset}
                        ) TO STDOUT WITH CSV HEADER QUOTE '"'
                    """
                    try:
                        if first_batch:
                            # Include the header for the first batch
                            cursor.copy_expert(copy_sql, data_fw)
                            first_batch = False
                        else:
                            # Skip the header for subsequent batches
                            copy_sql_no_header = copy_sql.replace("HEADER", "")
                            cursor.copy_expert(copy_sql_no_header, data_fw)

                    except psycopg2.OperationalError as ex:
                        if ex.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                            error_msg = traceback.format_exc()
                            logging.error("Serialization failure, skipping batch starting at offset\
                                          %d, %s", offset, error_msg)
                            logging.exception(ex)
                            break
                        else:
                            raise ex

                    logging.info("successfully written(%s) rowcount=[%d],offset=[%d]",
                                 table_name, cursor.rowcount, offset)
                    if cursor.rowcount < batch_size:
                        break  # No more rows to fetch, exit the loop

                    offset += batch_size

            logging.info("successfully written to %s", ens_domain_text_path)

        except Exception as ex:
            # error_msg = traceback.format_exc()
            # logging.error(error_msg)
            logging.exception(ex)
            raise ex

        end = time.time()
        ts_delta = end - start
        logging.info("loading offline ENS metadata(%s) end at: %s", \
                      table_name,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("loading offline ENS metadata(%s) cost: %d", table_name, ts_delta)
        cursor.close()
        conn.close()

    def ens_wrapped_domain(self, batch_size=100000):
        table_name = "ens_wrapped_domain"
        ens_metadata_dirs = os.path.join(setting.Settings["datapath"], "ens_metadata")
        if not os.path.exists(ens_metadata_dirs):
            os.makedirs(ens_metadata_dirs)

        start = time.time()
        logging.info("loading offline ENS metadata(%s) start at: %s", \
                      table_name,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        conn = psycopg2.connect(setting.PG_DSN["read"])
        conn.autocommit = True
        cursor = conn.cursor()

        ens_wrapped_domain_path = os.path.join(ens_metadata_dirs, f"{table_name}.csv")
        try:
            with open(ens_wrapped_domain_path, 'w', encoding="utf-8") as data_fw:
                offset = 0
                first_batch = True
                while True:
                    # Use batch-based query with LIMIT and OFFSET
                    copy_sql = f"""
                        COPY (
                            SELECT wrapped_id,domain,expiry_date,fuses,owner,name,created_at,updated_at
                            FROM public.{table_name} 
                            LIMIT {batch_size} OFFSET {offset}
                        ) TO STDOUT WITH CSV HEADER QUOTE '"'
                    """
                    try:
                        if first_batch:
                            # Include the header for the first batch
                            cursor.copy_expert(copy_sql, data_fw)
                            first_batch = False
                        else:
                            # Skip the header for subsequent batches
                            copy_sql_no_header = copy_sql.replace("HEADER", "")
                            cursor.copy_expert(copy_sql_no_header, data_fw)

                    except psycopg2.OperationalError as ex:
                        if ex.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                            error_msg = traceback.format_exc()
                            logging.error("Serialization failure, skipping batch starting at offset\
                                          %d, %s", offset, error_msg)
                            logging.exception(ex)
                            break
                        else:
                            raise ex

                    logging.info("successfully written(%s) rowcount=[%d],offset=[%d]",
                                 table_name, cursor.rowcount, offset)
                    if cursor.rowcount < batch_size:
                        break  # No more rows to fetch, exit the loop

                    offset += batch_size

            logging.info("successfully written to %s", ens_wrapped_domain_path)

        except Exception as ex:
            # error_msg = traceback.format_exc()
            # logging.error(error_msg)
            logging.exception(ex)
            raise ex

        end = time.time()
        ts_delta = end - start
        logging.info("loading offline ENS metadata(%s) end at: %s", \
                      table_name,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("loading offline ENS metadata(%s) cost: %d", table_name, ts_delta)
        cursor.close()
        conn.close()

    def ens_domain(self, batch_size=100000):
        table_name = "ens_domain"
        ens_metadata_dirs = os.path.join(setting.Settings["datapath"], "ens_metadata")
        if not os.path.exists(ens_metadata_dirs):
            os.makedirs(ens_metadata_dirs)

        start = time.time()
        logging.info("loading offline ENS metadata(%s) start at: %s", \
                      table_name,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        conn = psycopg2.connect(setting.PG_DSN["read"])
        conn.autocommit = True
        cursor = conn.cursor()

        ens_domain_path = os.path.join(ens_metadata_dirs, f"{table_name}.csv")
        try:
            with open(ens_domain_path, 'w', encoding="utf-8") as data_fw:
                offset = 0
                first_batch = True
                while True:
                    # Use batch-based query with LIMIT and OFFSET
                    copy_sql = f"""
                        COPY (
                            SELECT 
                                node_id, name, label_name, label_hash, 
                                resolved_address, resolver, owner, is_migrated,
                                registrant, wrapped_owner, registered_height, registered_hash,
                                expiry_date, updated_at, created_at, primary_address 
                            FROM public.{table_name} 
                            LIMIT {batch_size} OFFSET {offset}
                        ) TO STDOUT WITH CSV HEADER QUOTE '"'
                    """
                    try:
                        if first_batch:
                            # Include the header for the first batch
                            cursor.copy_expert(copy_sql, data_fw)
                            first_batch = False
                        else:
                            # Skip the header for subsequent batches
                            copy_sql_no_header = copy_sql.replace("HEADER", "")
                            cursor.copy_expert(copy_sql_no_header, data_fw)

                    except psycopg2.OperationalError as ex:
                        if ex.pgcode == psycopg2.errorcodes.SERIALIZATION_FAILURE:
                            error_msg = traceback.format_exc()
                            logging.error("Serialization failure, skipping batch starting at offset\
                                          %d, %s", offset, error_msg)
                            logging.exception(ex)
                            break
                        else:
                            raise ex

                    logging.info("successfully written(%s) rowcount=[%d],offset=[%d]",
                                 table_name, cursor.rowcount, offset)
                    if cursor.rowcount < batch_size:
                        break  # No more rows to fetch, exit the loop

                    offset += batch_size

            logging.info("successfully written to %s", ens_domain_path)

        except Exception as ex:
            # error_msg = traceback.format_exc()
            # logging.error(error_msg)
            logging.exception(ex)
            raise ex

        end = time.time()
        ts_delta = end - start
        logging.info("loading offline ENS metadata(%s) end at: %s", \
                      table_name,
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("loading offline ENS metadata(%s) cost: %d", table_name, ts_delta)
        cursor.close()
        conn.close()

    def dumps_ensname(self, cursor, rows):
        '''
        description: fetch clusters indexes data save into ensname database
        '''
        sql_statement = """
        INSERT INTO public.ensname (
            namenode,
            name,
            label_name,
            label,
            erc721_token_id,
            erc1155_token_id,
            parent_node,
            registration_time,
            registered_height,
            registered_hash,
            expire_time,
            is_migrated,
            is_wrapped,
            wrapped_owner,
            fuses,
            grace_period_ends,
            owner,
            resolver,
            resolved_address,
            reverse_address,
            is_primary,
            update_time
        ) VALUES %s
        ON CONFLICT (namenode)
        DO UPDATE SET
            name = EXCLUDED.name,
            label_name = EXCLUDED.label_name,
            label = EXCLUDED.label,
            erc721_token_id = EXCLUDED.erc721_token_id,
            erc1155_token_id = EXCLUDED.erc1155_token_id,
            parent_node = EXCLUDED.parent_node,
            registration_time = LEAST(ensname.registration_time, EXCLUDED.registration_time),
            registered_height = EXCLUDED.registered_height,
            registered_hash = EXCLUDED.registered_hash,
            expire_time = GREATEST(ensname.expire_time, EXCLUDED.expire_time),
            is_migrated = EXCLUDED.is_migrated,
            is_wrapped = EXCLUDED.is_wrapped,
            wrapped_owner = EXCLUDED.wrapped_owner,
            owner = EXCLUDED.owner,
            resolver = EXCLUDED.resolver,
            resolved_address = EXCLUDED.resolved_address,
            reverse_address = EXCLUDED.reverse_address,
            is_primary = EXCLUDED.is_primary,
            update_time = EXCLUDED.update_time;
        """
        upsert_data = []
        for row in rows:
            upsert_data.append(
                (
                    row["namenode"],
                    row.get("name", None),
                    row.get("label_name", None),
                    row.get("label", None),
                    row.get("erc721_token_id", None),
                    row.get("erc1155_token_id", None),
                    row.get("parent_node", None),
                    row.get("registration_time", None),
                    row.get("registered_height", 0),
                    row.get("registered_hash", None),
                    row.get("expire_time", None),
                    row.get("is_migrated", False),
                    row.get("is_wrapped", False),
                    row.get("wrapped_owner", None),
                    row.get("fuses", 0),
                    row.get("grace_period_ends", None),
                    row.get("owner", None),
                    row.get("resolver", None),
                    row.get("resolved_address", None),
                    row.get("reverse_address", None),
                    row.get("is_primary", False),
                    row.get("update_time", None)
                )
            )
        if upsert_data:
            try:
                execute_values(cursor, sql_statement, upsert_data)
                logging.info("Batch insert completed %d records.", len(upsert_data))
            except Exception as ex:
                logging.error("Caught exception during insert in %s", json.dumps(upsert_data))
                raise ex
        else:
            logging.debug("No valid upsert_data to process.")

    def save_ensname_metadata(self):
        ens_metadata_dirs = os.path.join(setting.Settings["datapath"], "ens_metadata")
        if not os.path.exists(ens_metadata_dirs):
            raise FileNotFoundError(f"No metadata directory {ens_metadata_dirs}")

        upsert_data_dict = {}
        has_header = True
        cnt = 0
        ens_domain_path = os.path.join(ens_metadata_dirs, "ens_domain.csv")
        if not os.path.exists(ens_domain_path):
            raise FileNotFoundError(f"No metadata directory {ens_domain_path}")

        try:
            with open(ens_domain_path, 'r', encoding="utf-8") as csvfile:
                # node_id, name, label_name, label_hash, 
                # resolved_address, resolver, owner, is_migrated,
                # registrant, wrapped_owner, registered_height, registered_hash,
                # expiry_date, updated_at, created_at, primary_address 
                reader = csv.reader(csvfile)
                for item in reader:
                    if cnt == 0 and has_header:
                        cnt += 1
                        has_header = False
                        continue

                    cnt += 1
                    upsert_record = {}
                    namenode = item[0]
                    ens_name = item[1]
                    if ens_name == "" or ens_name == '""':
                        continue

                    ens_name = ens_name.rstrip(".")
                    parent_node, label, erc721_token_id, erc1155_token_id, node = compute_namehash(ens_name)

                    upsert_record["namenode"] = namenode
                    upsert_record["name"] = ens_name
                    upsert_record["label_name"] = ens_name.split(".")[0]
                    upsert_record["label"] = label
                    upsert_record["erc721_token_id"] = erc721_token_id
                    upsert_record["erc1155_token_id"] = erc1155_token_id
                    upsert_record["parent_node"] = parent_node

                    resolved_address = item[4]
                    if resolved_address != "" and resolved_address != '""':
                        upsert_record["resolved_address"] = resolved_address
                    else:
                        upsert_record["resolved_address"] = None

                    resolver = item[5]
                    if resolver != "" and resolver != '""':
                        upsert_record["resolver"] = resolver
                    else:
                        upsert_record["resolver"] = None

                    owner = item[6]
                    if owner != "" and owner != '""':
                        upsert_record["owner"] = owner
                    else:
                        upsert_record["owner"] = None

                    is_migrated = item[7]
                    if is_migrated == "0":
                        upsert_record["is_migrated"] = False
                    elif is_migrated == "1":
                        upsert_record["is_migrated"] = True

                    wrapped_owner = item[9]
                    if wrapped_owner != "" and wrapped_owner != '""':
                        upsert_record["wrapped_owner"] = wrapped_owner
                        upsert_record["is_wrapped"] = True
                    else:
                        upsert_record["wrapped_owner"] = None
                        upsert_record["is_wrapped"] = False

                    registered_height = item[10]
                    registered_hash = item[11]
                    if registered_height != "0" and registered_hash != "" and registered_hash != '""':
                        upsert_record["registered_height"] = int(registered_height)
                        upsert_record["registered_hash"] = registered_hash
                    else:
                        upsert_record["registered_height"] = 0
                        upsert_record["registered_hash"] = None

                    expiry_date = item[12]
                    if expiry_date != "" and expiry_date != "0" and expiry_date != '""':
                        expire_time = unix_string_to_datetime(expiry_date)
                        upsert_record["expire_time"] = expire_time
                    else:
                        upsert_record["expire_time"] = None

                    updated_at = item[13]
                    if updated_at != "" and updated_at != "0" and updated_at != '""':
                        update_time = unix_string_to_datetime(updated_at)
                        upsert_record["update_time"] = update_time
                    else:
                        upsert_record["update_time"] = unix_string_to_datetime(int(time.time()))

                    created_at = item[14]
                    if created_at != "" and created_at != "0" and created_at != '""':
                        registration_time = unix_string_to_datetime(created_at)
                        upsert_record["registration_time"] = None
                        upsert_record["created_at"] = int(created_at)
                    else:
                        upsert_record["registration_time"] = None
                        upsert_record["created_at"] = 0
                    

                    primary_address = item[15]
                    if primary_address != "" and primary_address != '""':
                        upsert_record["reverse_address"] = primary_address
                        upsert_record["is_primary"] = True
                    else:
                        upsert_record["reverse_address"] = None
                        upsert_record["is_primary"] = False

                    upsert_data_dict[namenode] = upsert_record
        except Exception as ex:
            logging.exception(ex)
            raise ex

        has_header = True
        cnt = 0
        ens_wrapped_domain_path = os.path.join(ens_metadata_dirs, "ens_wrapped_domain.csv")
        if not os.path.exists(ens_wrapped_domain_path):
            raise FileNotFoundError(f"No metadata directory {ens_wrapped_domain_path}")

        try:
            with open(ens_wrapped_domain_path, 'r', encoding="utf-8") as csvfile:
                # wrapped_id	domain	expiry_date	fuses	owner	name	created_at	updated_at
                reader = csv.reader(csvfile)
                for item in reader:
                    if cnt == 0 and has_header:
                        cnt += 1
                        has_header = False
                        continue

                    cnt += 1
                    wrapped_id = item[0]
                    fuses = item[3]
                    owner = item[4]
                    updated_at = item[7]
                    if wrapped_id in upsert_data_dict:
                        upsert_data_dict[wrapped_id]["fuses"] = int(fuses)
                        upsert_data_dict[wrapped_id]["wrapped_owner"] = owner
                        upsert_data_dict[wrapped_id]["is_wrapped"] = True
                        if updated_at != "" or updated_at != "0":
                            update_time = unix_string_to_datetime(updated_at)
                            upsert_data_dict[wrapped_id]["update_time"] = update_time
                        else:
                            upsert_data_dict[wrapped_id]["update_time"] = unix_string_to_datetime(int(time.time()))
        except Exception as ex:
            logging.exception(ex)
            raise ex

        conn = psycopg2.connect(setting.PG_DSN["write"])
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            upsert_data = upsert_data_dict.values()
            upsert_data = sorted(upsert_data, key=itemgetter("registered_height"))
            per_count = 10000
            batch_count = math.ceil(len(upsert_data) / per_count)
            for i in range(batch_count):
                batch_upsert_data = upsert_data[i*per_count: (i+1)*per_count]
                self.dumps_ensname(cursor, batch_upsert_data)

            os.rename(ens_domain_path, ens_domain_path + ".finished")
            os.rename(ens_wrapped_domain_path, ens_wrapped_domain_path + ".finished")
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            conn.close()

    def save_ens_contenthash(self):
        ens_metadata_dirs = os.path.join(setting.Settings["datapath"], "ens_metadata")
        if not os.path.exists(ens_metadata_dirs):
            raise FileNotFoundError(f"No metadata directory {ens_metadata_dirs}")

        update_data = []
        has_header = True
        cnt = 0
        ens_domain_contenthash_path = os.path.join(ens_metadata_dirs, "ens_domain_contenthash.csv")
        if not os.path.exists(ens_domain_contenthash_path):
            raise FileNotFoundError(f"No metadata directory {ens_domain_contenthash_path}")

        with open(ens_domain_contenthash_path, newline='', encoding="utf-8") as csvfile:
            # namenode,contenthash,update_time
            reader = csv.reader(csvfile)
            for row in reader:
                if cnt == 0 and has_header:
                    cnt += 1
                    has_header = False
                    continue

                namenode = row[0]
                contenthash = row[1]
                update_time = row[2]
                update_data.append({
                    "namenode": namenode,
                    "contenthash": contenthash,
                    "update_time": update_time
                })

        update_sql = """
            UPDATE public.ensname
            SET
                contenthash = %(contenthash)s,
                update_time = %(update_time)s
            WHERE 
                namenode = %(namenode)s
        """
        conn = psycopg2.connect(setting.PG_DSN["write"])
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            per_count = 2000
            batch_count = math.ceil(len(update_data) / per_count)
            for i in range(batch_count):
                batch_update_data = update_data[i*per_count: (i+1)*per_count]
                if batch_update_data:
                    try:
                        execute_batch(cursor, update_sql, update_data)
                        logging.info("Batch update completed for %d records.", len(batch_update_data))
                    except Exception as ex:
                        logging.error("Caught exception during update in %s", json.dumps(batch_update_data))
                        raise ex
                else:
                    logging.debug("No valid update_data to process.")

            os.rename(ens_domain_contenthash_path, ens_domain_contenthash_path + ".finished")
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            conn.close()

    def save_ensname_texts(self):
        ens_metadata_dirs = os.path.join(setting.Settings["datapath"], "ens_metadata")
        if not os.path.exists(ens_metadata_dirs):
            raise FileNotFoundError(f"No metadata directory {ens_metadata_dirs}")

        upsert_data_dict = {}
        has_header = True
        cnt = 0
        ens_domain_text_path = os.path.join(ens_metadata_dirs, "ens_domain_text.csv")
        if not os.path.exists(ens_domain_text_path):
            raise FileNotFoundError(f"No metadata directory {ens_domain_text_path}")

        with open(ens_domain_text_path, newline='', encoding="utf-8") as csvfile:
            # node_id,key_hash,text_key,text_value,created_at,updated_at
            reader = csv.reader(csvfile)
            for row in reader:
                if cnt == 0 and has_header:
                    cnt += 1
                    has_header = False
                    continue

                namenode = row[0]
                if namenode not in upsert_data_dict:
                    upsert_data_dict[namenode] = {"namenode": namenode, "texts": {}}

                text_key = row[2]
                text_value = row[3]
                upsert_data_dict[namenode]["texts"][text_key] = text_value

        update_data = []
        for namenode, record in upsert_data_dict.items():
            texts = record.get("texts", {})
            kv_fields = []
            for k, v in texts.items():
                vv = quote(v, 'utf-8')  # convert string to url-encoded
                kk = quote(k, 'utf-8')  # convert string to url-encoded
                kv_fields.append("'" + kk + "'")
                kv_fields.append("'" + vv + "'")
            jsonb_data = ",".join(kv_fields)
            if texts != {}:
                update_data.append({
                    "namenode": namenode,
                    "jsonb_data": jsonb_data
                })

        conn = psycopg2.connect(setting.PG_DSN["write"])
        conn.autocommit = True
        cursor = conn.cursor()
        try:
            per_count = 2000
            batch_count = math.ceil(len(update_data) / per_count)
            for i in range(batch_count):
                batch_update_data = update_data[i*per_count: (i+1)*per_count]

                namenode_ids = []
                sql_parts = []
                # Start building the update SQL
                update_sql = "UPDATE public.ensname SET "

                # Build the texts field update logic
                texts_case = "texts = CASE "

                for value in batch_update_data:
                    namenode = value.get("namenode")
                    namenode_ids.append(namenode)
                    jsonb_data = value.get("jsonb_data", None)
                    texts_case += f" WHEN namenode = '{namenode}' THEN texts || jsonb_build_object({jsonb_data})"

                # Add the ELSE part
                texts_case += " ELSE texts END"

                # Add the cases to the main SQL
                sql_parts.append(texts_case)
                update_sql += ",".join(sql_parts)

                # Add the WHERE clause for the namenodes
                namenode_ids = "', '".join(namenode_ids)
                update_sql += f" WHERE namenode IN ('{namenode_ids}')"
                try:
                    cursor.execute(update_sql)
                    logging.info(f"Batch update completed for {len(batch_update_data)} records.")
                except Exception as ex:
                    logging.error(f"Caught exception during update: {ex}")
                    raise ex

            os.rename(ens_domain_text_path, ens_domain_text_path + ".finished")
        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            conn.close()

    def process_ensname(self):
        ens_process_dirs = os.path.join(setting.Settings["datapath"], "ensname_process")
        if not os.path.exists(ens_process_dirs):
            os.makedirs(ens_process_dirs)

        ensname_path = os.path.join(ens_process_dirs, "ensname.csv")

        start = time.time()
        logging.info("processing ensname start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))


        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        cursor = read_conn.cursor()

        try:
            ens_domain = "ens_domain"
            columns = ["node_id", "name", "label_name", "label_hash", "show_address", "resolved_address", "resolver", "owner", "is_migrated", "registrant", "wrapped_owner", "expiry_date", "registered_height", "registered_hash", "updated_at", "created_at", "primary_address"]
            select_sql = "SELECT %s FROM %s" % (",".join(columns), ens_domain)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            ens_domain_df = pd.DataFrame(rows, columns=columns)

            ens_wrapped_domain = "ens_wrapped_domain"
            columns = ["wrapped_id", "domain", "expiry_date", "fuses", "owner", "name", "created_at", "updated_at"]
            select_sql = "SELECT %s FROM %s" % (",".join(columns), ens_wrapped_domain)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            ens_wrapped_domain_df = pd.DataFrame(rows, columns=columns)

            ens_domain_content_hash = "ens_domain_content_hash"
            columns = ["node_id", "content_hash", "text_value", "updated_at", "created_at"]
            select_sql = "SELECT %s FROM %s" % (",".join(columns), ens_domain_content_hash)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            ens_domain_content_hash_df = pd.DataFrame(rows, columns=columns)

            ens_domain_text = "ens_domain_text"
            columns = ["node_id", "key_hash", "text_key", "text_value", "created_at", "updated_at"]
            select_sql = "SELECT %s FROM %s" % (",".join(columns), ens_domain_text)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            ens_domain_text_df = pd.DataFrame(rows, columns=columns)
            
            ens_resolved_records = "ens_resolved_records"
            columns = ["namenode", "resolved_records", "update_time"]
            select_sql = "SELECT %s FROM %s" % (",".join(columns), ens_resolved_records)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            ens_resolved_records_df = pd.DataFrame(rows, columns=columns)

            result_df = pd.DataFrame()
            result_df['namenode'] = ens_domain_df['node_id']
            result_df['name'] = ens_domain_df['name'].apply(lambda x: x.rstrip(".") if pd.notnull(x) and x != "" else None)
            result_df['label_name'] = result_df['name'].apply(lambda x: x.split(".")[0] if pd.notnull(x) and x != "" else None)
            result_df[['parent_node', 'label', 'erc721_token_id', 'erc1155_token_id']] = result_df['name'].apply(lambda x: pd.Series(process_compute_namehash(x)))
            result_df['registered_height'] = ens_domain_df['registered_height']
            result_df['registered_hash'] = ens_domain_df['registered_hash']
            result_df['expire_time'] = ens_domain_df['expiry_date'].apply(lambda x: unix_string_to_datetime(x) if x > 0 else None)
            result_df['is_migrated'] = ens_domain_df['is_migrated'].apply(lambda x: True if x == 1 else False)
            result_df['owner'] = ens_domain_df['owner'].apply(lambda x: x if pd.notnull(x) and x != "" else None)
            result_df['resolver'] = ens_domain_df['resolver'].apply(lambda x: x if pd.notnull(x) and x != "" else None)
            result_df['resolved_address'] = ens_domain_df['resolved_address'].apply(lambda x: x if pd.notnull(x) and x != "" else None)
            result_df['reverse_address'] = ens_domain_df['primary_address'].apply(lambda x: x if pd.notnull(x) and x != "" else None)
            result_df['is_primary'] = result_df['reverse_address'].apply(lambda x: True if pd.notnull(x) and x != "" else False)
            result_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # wrapped_domain
            result_df = pd.merge(result_df,
                     ens_wrapped_domain_df[['wrapped_id', 'owner', 'fuses']],
                     left_on='namenode',
                     right_on='wrapped_id',
                     how='left',
                     suffixes=('', '_wrapped'))
            result_df['is_wrapped'] = result_df['wrapped_id'].apply(lambda x: True if pd.notnull(x) else False)
            result_df['fuses'] = result_df['fuses'].fillna(0).astype(int)
            result_df['wrapped_owner'] = result_df['owner_wrapped']
            result_df = result_df.drop(columns=['wrapped_id', 'owner_wrapped']) # Drop unnecessary columns after the join

            # contenthash
            result_df = pd.merge(result_df,
                     ens_domain_content_hash_df[['node_id', 'content_hash']],
                     left_on='namenode',
                     right_on='node_id',
                     how='left',
                     suffixes=('', '_contenthash'))
            result_df = result_df.drop(columns=['node_id'])
            # result_df['contenthash'] = result_df['content_hash'].fillna('')
            result_df['contenthash'] = result_df['content_hash'].apply(process_contenthash)

            # resolved_records
            result_df = pd.merge(result_df,
                     ens_resolved_records_df[['namenode', 'resolved_records']],
                     left_on='namenode',
                     right_on='namenode',
                     how='left')
            result_df['resolved_records'] = result_df.apply(process_resolved_records, axis=1)

            # texts
            grouped = ens_domain_text_df.groupby('node_id')
            processed_texts = grouped.apply(aggregate_texts).reset_index(name='texts')
            result_df = pd.merge(result_df, processed_texts, left_on='namenode', right_on='node_id', how='left')
            result_df['texts'] = result_df['texts'].fillna({})
            result_df = result_df.drop(columns=['node_id'])

            
            result_df = result_df.sort_values(by='registered_height')
            # export the result_df to a CSV file
            result_df = result_df[['namenode', 'name', 'label_name', 'label',\
                                   'erc721_token_id', 'erc1155_token_id', 'parent_node',\
                                    'registered_height', 'registered_hash',\
                                    'expire_time', 'is_migrated', 'is_wrapped', 'wrapped_owner', 'fuses',\
                                    'owner', 'resolver', 'resolved_address', 'reverse_address', 'is_primary',\
                                    'contenthash', 'update_time', 'texts', 'resolved_records']]
            result_df.to_csv(ensname_path, index=False, quoting=csv.QUOTE_ALL)

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("processing ensname end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing ensname cost: %d", ts_delta)

    def save_ensname(self, dump_batch_size=10000):
        ens_process_dirs = os.path.join(setting.Settings["datapath"], "ensname_process")
        if not os.path.exists(ens_process_dirs):
            raise FileNotFoundError(f"No ensname directory {ens_process_dirs}")

        ensname_path = os.path.join(ens_process_dirs, "ensname.csv")
        if not os.path.exists(ensname_path):
            raise FileNotFoundError(f"No data path {ensname_path}")

        start = time.time()
        logging.info("saving ensname start at: %s", \
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        insert_ensname = """
        INSERT INTO ensname (
            namenode,
            name,
            label_name,
            label,
            erc721_token_id,
            erc1155_token_id,
            parent_node,
            registered_height,
            registered_hash,
            expire_time,
            is_migrated,
            is_wrapped,
            wrapped_owner,
            fuses,
            owner,
            resolver,
            resolved_address,
            reverse_address,
            is_primary,
            contenthash,
            update_time,
            texts,
            resolved_records
        ) VALUES %s
        ON CONFLICT (namenode)
        DO UPDATE SET
            name = EXCLUDED.name,
            label_name = EXCLUDED.label_name,
            label = EXCLUDED.label,
            erc721_token_id = EXCLUDED.erc721_token_id,
            erc1155_token_id = EXCLUDED.erc1155_token_id,
            parent_node = EXCLUDED.parent_node,
            registered_height = EXCLUDED.registered_height,
            registered_hash = EXCLUDED.registered_hash,
            expire_time = EXCLUDED.expire_time,
            is_migrated = EXCLUDED.is_migrated,
            is_wrapped = EXCLUDED.is_wrapped,
            wrapped_owner = EXCLUDED.wrapped_owner,
            owner = EXCLUDED.owner,
            resolver = EXCLUDED.resolver,
            resolved_address = EXCLUDED.resolved_address,
            reverse_address = EXCLUDED.reverse_address,
            is_primary = EXCLUDED.is_primary,
            contenthash = EXCLUDED.contenthash,
            update_time = EXCLUDED.update_time,
            texts = EXCLUDED.texts,
            resolved_records = EXCLUDED.resolved_records;
        """

        write_conn = psycopg2.connect(setting.PG_DSN["write"])
        write_conn.autocommit = True
        cursor = write_conn.cursor()

        cnt = 0
        batch = []
        batch_times = 0

        try:
            # Loading `ensname`
            with open(ensname_path, 'r', encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)  # Skip the header
                logging.info("[%s] header: %s", ensname_path, header)
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
                            cursor, insert_ensname, batch, template=None, page_size=dump_batch_size
                        )
                        logging.info("Upserted[ensname] batch with size [%d], batch_times %d", len(batch), batch_times)
                        batch = []

                # remaining
                if batch:
                    batch_times += 1
                    execute_values(
                        cursor, insert_ensname, batch, template=None, page_size=len(batch)
                    )
                    logging.info("Upserted[ensname] batch with size [%d], batch_times %d", len(batch), batch_times)
            os.rename(ensname_path, ensname_path + ".finished")

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            write_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("saving ensname end at: %s", \
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("saving ensname cost: %d", ts_delta)


    def process_pipeline(self):
        try:
            self.update_job_status("start")
            self.update_job_status("running")
            self.process_ensname()
            self.save_ensname()
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

    ENSProcess().save_ensname()