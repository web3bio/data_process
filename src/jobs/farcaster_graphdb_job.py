#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-27 00:12:32
LastEditors: Zella Zhong
LastEditTime: 2024-10-16 21:37:07
FilePath: /data_process/src/jobs/farcaster_graphdb_job.py
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

import setting
from utils.timeutils import get_unix_milliconds

def generate_new_graph_id(row):
    graph_id_address = row['graph_id_address']
    updated_nanosecond_address = row['updated_nanosecond_address']
    if pd.isna(graph_id_address):
        new_graph_id = str(uuid.uuid4())
        current_time_ns = int(get_unix_milliconds())
        return new_graph_id, current_time_ns, False
    else:
        return graph_id_address, int(updated_nanosecond_address), True


def generate_isolate_graph_id(row):
    graph_id_farcaster = row['graph_id_farcaster']
    updated_nanosecond_farcaster = row['updated_nanosecond_farcaster']
    if pd.isna(graph_id_farcaster):
        new_graph_id = str(uuid.uuid4())
        current_time_ns = int(get_unix_milliconds())
        return new_graph_id, current_time_ns, False
    else:
        return graph_id_farcaster, int(updated_nanosecond_farcaster), True

def assign_graph_id(group):
    '''
    description:
        # group by farcaster_unique_id
        # merged_df = ['fname', 'network', 'address', 'farcaster_unique_id', 'address_unique_id',
        #  'unique_id_farcaster', 'graph_id_farcaster', 'updated_nanosecond_farcaster',
        #  'new_address_graph_id', 'new_address_updated_nanosecond', 'is_exist']
    '''
    sorted_address_pairs = group[['new_address_graph_id', 'new_address_updated_nanosecond']].sort_values(by='new_address_updated_nanosecond')

    # Select the smallest new_address pair as the default
    final_graph_id = sorted_address_pairs['new_address_graph_id'].iloc[0]
    final_updated_nanosecond = sorted_address_pairs['new_address_updated_nanosecond'].iloc[0]

    # Check if graph_id_farcaster exists and compare it with the smallest address pair
    if pd.notna(group['graph_id_farcaster']).any():
        # If graph_id_farcaster exists, we check the corresponding updated_nanosecond_farcaster
        farcaster_graph_id = group['graph_id_farcaster'].dropna().iloc[0]
        farcaster_updated_ns = group['updated_nanosecond_farcaster'].dropna().iloc[0]

        # If the farcaster updated_nanosecond is smaller, replace both graph_id and updated_nanosecond
        if farcaster_updated_ns < final_updated_nanosecond:
            final_graph_id = farcaster_graph_id
            final_updated_nanosecond = farcaster_updated_ns

    # Step 4: Assign the final graph_id and updated_nanosecond to the entire group
    group['final_graph_id'] = final_graph_id
    group['final_updated_nanosecond'] = int(final_updated_nanosecond)

    return group


class FarcasterGraphDB(object):
    def __init__(self):
        self.job_name = "farcaster_graphdb_job"
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

    def process_farcaster_identity_graph(self):
        farcaster_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/farcaster")
        if not os.path.exists(farcaster_process_dirs):
            os.makedirs(farcaster_process_dirs)

        start = time.time()
        logging.info("processing farcaster_identity_graph start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        # allocation
        allocation_path = os.path.join(farcaster_process_dirs, "graph_id.csv")
        # vertices
        identities_path = os.path.join(farcaster_process_dirs, "Identities.csv")
        identities_graph_path = os.path.join(farcaster_process_dirs, "IdentitiesGraph.csv")
        # edges
        part_of_identities_graph_path = os.path.join(farcaster_process_dirs, "PartOfIdentitiesGraph.csv")
        hold_path = os.path.join(farcaster_process_dirs, "Hold.csv")

        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        cursor = read_conn.cursor()
        try:
            # Loading farcaster fname
            farcaster_profile = "farcaster_profile"
            columns = ['fname']
            select_sql = "SELECT %s FROM %s WHERE delete_time is null" % (",".join(columns), farcaster_profile)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            farcaster_fname_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table farcaster_profile[fname] row_count: %d" % farcaster_fname_df.shape[0])

            # Loading farcaster verifications
            farcaster_verified_address = "farcaster_verified_address"
            columns = ['fname', 'network', 'address']
            select_sql = "SELECT %s FROM %s WHERE delete_time is null" % (",".join(columns), farcaster_verified_address)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            verification_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table farcaster_verified_address row_count: %d" % verification_df.shape[0])

            address_df = verification_df[['network', 'address']].copy()
            address_df['primary_id'] = address_df['network'] + "," + address_df['address']
            address_df['platform'] = address_df['network']
            address_df['identity'] = address_df['address']
            address_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            address_df = address_df.drop_duplicates(subset=['platform', 'identity'], keep='first')
            address_df = address_df[['primary_id', 'platform', 'identity', 'update_time']]

            fname_df = farcaster_fname_df[['fname']].copy()
            fname_df['primary_id'] = farcaster_fname_df['fname'].apply(lambda x: f"farcaster,{x}")
            fname_df['platform'] = 'farcaster'
            fname_df['identity'] = fname_df['fname']
            fname_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            fname_df = fname_df.dropna(subset=['identity'])
            fname_df = fname_df.drop_duplicates(subset=['identity'], keep='first')
            fname_df = fname_df[['primary_id', 'platform', 'identity', 'update_time']]

            # Identities.csv
            identities_df = pd.concat([fname_df, address_df])
            identities_df.to_csv(identities_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", identities_path, identities_df.shape[0])

            # Hold.csv
            hold_grouped = verification_df.groupby(['fname', 'network', 'address'], as_index=False).first()
            hold_grouped = hold_grouped[hold_grouped['address'] != '0x0000000000000000000000000000000000000000']
            hold_grouped['from'] = hold_grouped.apply(lambda x: f"{x['network']},{x['address']}", axis=1)
            hold_grouped['to'] = hold_grouped.apply(lambda x: f"farcaster,{x['fname']}", axis=1)
            hold_grouped['source'] = "farcaster"
            hold_grouped['level'] = 5
            hold_df = hold_grouped[['from', 'to', 'source', 'level']]
            hold_df.to_csv(hold_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", hold_path, hold_df.shape[0])

            # Loading graph_id allocation table
            graph_id_table = "graph_id"
            columns = ['unique_id', 'graph_id', 'updated_nanosecond']
            select_sql = "SELECT %s FROM %s WHERE platform in ('ethereum', 'solana', 'farcaster')" % (",".join(columns), graph_id_table)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            allocation_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table graph_id row_count: %d", allocation_df.shape[0])

            # Create unique ids for farcaster and address
            # ['fname', 'network', 'address', 'farcaster_unique_id', 'address_unique_id']
            verification_df['farcaster_unique_id'] = verification_df.apply(lambda x: f"farcaster,{x['fname']}", axis=1)
            verification_df['address_unique_id'] = verification_df.apply(lambda x: f"{x['network']},{x['address']}", axis=1)

            address_unique_df = verification_df[['network', 'address']].copy()
            address_unique_df['address_unique_id'] = address_unique_df['network'] + "," + address_unique_df['address']
            address_unique_df = address_unique_df.drop_duplicates(subset=['network', 'address'], keep='first')
            address_unique_df = pd.merge(address_unique_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                                    left_on='address_unique_id', right_on='unique_id', how='left', suffixes=('', '_address'))
            address_unique_df = address_unique_df.rename(columns={
                'unique_id': 'unique_id_address',
                'graph_id': 'graph_id_address',
                'updated_nanosecond': 'updated_nanosecond_address'
            })
            # Prepare graph_id for dedup address
            address_unique_df[[
                'new_address_graph_id',
                'new_address_updated_nanosecond',
                'is_exist'
            ]] = address_unique_df.apply(generate_new_graph_id, axis=1, result_type="expand")
            # address_unique_df columns:
            # ['network', 'address', 'address_unique_id', 'unique_id_address',
            # 'graph_id_address', 'updated_nanosecond_address',
            # 'new_address_graph_id', 'new_address_updated_nanosecond', 'is_exist']
            logging.debug("Successfully prepare graph_id for dedup address_unique_df row_count: %d", address_unique_df.shape[0])


            # Isolate facaster fname
            isolate_fname_df = farcaster_fname_df[['fname']].copy()
            isolate_fname_df['farcaster_unique_id'] = isolate_fname_df['fname'].apply(lambda x: f"farcaster,{x}")
            isolate_fname_df = pd.merge(isolate_fname_df, verification_df,
                                        left_on='farcaster_unique_id', right_on='farcaster_unique_id', how='left', suffixes=('', '_verification'))
            isolate_fname_df = isolate_fname_df[isolate_fname_df['address'].isna()] # filter isolated farcaster
            isolate_fname_df = isolate_fname_df[['fname', 'farcaster_unique_id']]
            isolate_fname_df = pd.merge(isolate_fname_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                                        left_on='farcaster_unique_id', right_on='unique_id', how='left', suffixes=('', '_farcaster'))
            isolate_fname_df = isolate_fname_df.rename(columns={
                'unique_id': 'unique_id_farcaster',
                'graph_id': 'graph_id_farcaster',
                'updated_nanosecond': 'updated_nanosecond_farcaster'
            })
            # Prepare graph_id for isolate farcaster identity
            isolate_fname_df[[
                'final_graph_id',
                'final_updated_nanosecond',
                'is_exist'
            ]] = isolate_fname_df.apply(generate_isolate_graph_id, axis=1, result_type="expand")
            logging.debug("Successfully prepare dedup isolate_fname_df row_count: %d", isolate_fname_df.shape[0])

            # Merge verification_df with allocation_df on both unique_ids to check if they already have a graph_id
            merged_df = pd.merge(verification_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                                left_on='farcaster_unique_id', right_on='unique_id', how='left', suffixes=('', '_farcaster'))
            merged_df = merged_df.rename(columns={
                'unique_id': 'unique_id_farcaster',
                'graph_id': 'graph_id_farcaster',
                'updated_nanosecond': 'updated_nanosecond_farcaster'
            })

            merged_df = pd.merge(merged_df, address_unique_df[['address_unique_id', 'new_address_graph_id', 'new_address_updated_nanosecond', 'is_exist']],
                                left_on='address_unique_id', right_on='address_unique_id', how='left', suffixes=('', '_address'))
            logging.debug("Successfully merge verification_df and allocation_df to final_df row_count: %d", merged_df.shape[0])

            # merged_df columns:
            # ['fname', 'network', 'address', 'farcaster_unique_id', 'address_unique_id',
            #  'unique_id_farcaster', 'graph_id_farcaster', 'updated_nanosecond_farcaster',
            #  'new_address_graph_id', 'new_address_updated_nanosecond', 'is_exist']

            logging.debug("Start assign_graph_id...")
            # Apply assign_graph_id to each group (grouped by fname)
            # final_df = merged_df.set_index('farcaster_unique_id').groupby(level='farcaster_unique_id').apply(assign_graph_id).reset_index(drop=True)
            final_df = merged_df.groupby('farcaster_unique_id').apply(assign_graph_id).reset_index(drop=True)
            # final_df columns:
            # ['fname', 'network', 'address', 'farcaster_unique_id', 'address_unique_id',
            #  'unique_id_farcaster', 'graph_id_farcaster', 'updated_nanosecond_farcaster',
            #  'new_address_graph_id', 'new_address_updated_nanosecond', 'is_exist',
            #  'final_graph_id', 'final_updated_nanosecond']
            logging.debug("End assign_graph_id...")
            logging.debug("Successfully genrate final_df row_count: %d", final_df.shape[0])

            isolate_ig_df = isolate_fname_df[['final_graph_id', 'final_updated_nanosecond']].copy()
            # rename the columns
            isolate_ig_df = isolate_ig_df.rename(columns={
                'final_graph_id': 'primary_id',
                'final_updated_nanosecond': 'updated_nanosecond'
            })

            common_ig_df = final_df[['final_graph_id', 'final_updated_nanosecond']]
            # rename the columns
            common_ig_df = common_ig_df.rename(columns={
                'final_graph_id': 'primary_id',
                'final_updated_nanosecond': 'updated_nanosecond'
            })
            identities_graph_df = pd.concat([isolate_ig_df, common_ig_df])
            identities_graph_df = identities_graph_df.drop_duplicates(subset=['primary_id'], keep='first')
            identities_graph_df['updated_nanosecond'] = identities_graph_df['updated_nanosecond'].astype('int64')
            identities_graph_df.to_csv(identities_graph_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", identities_graph_path, identities_graph_df.shape[0])

            partof_address = final_df[['address_unique_id', 'final_graph_id']].copy()
            # rename the columns
            partof_address = partof_address.rename(columns={
                'address_unique_id': 'from',
                'final_graph_id': 'to'
            })

            partof_fname = final_df[['farcaster_unique_id', 'final_graph_id']].copy()
            # rename the columns
            partof_fname = partof_fname.rename(columns={
                'farcaster_unique_id': 'from',
                'final_graph_id': 'to'
            })

            part_of_isolated_fname = isolate_fname_df[['farcaster_unique_id', 'final_graph_id']].copy()
            # rename the columns
            part_of_isolated_fname = part_of_isolated_fname.rename(columns={
                'farcaster_unique_id': 'from',
                'final_graph_id': 'to'
            })

            part_of_identities_graph_df = pd.concat([partof_address, partof_fname, part_of_isolated_fname])
            part_of_identities_graph_df = part_of_identities_graph_df.drop_duplicates(subset=['from', 'to'], keep='last')
            part_of_identities_graph_df.to_csv(part_of_identities_graph_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", part_of_identities_graph_path, part_of_identities_graph_df.shape[0])

            address_part = final_df[['address_unique_id', 'final_graph_id', 'network', 'address', 'final_updated_nanosecond']].copy()
            address_part = address_part.drop_duplicates(subset=['address_unique_id'], keep='first')
            address_part = address_part.rename(columns={
                'address_unique_id': 'unique_id',
                'final_graph_id': 'graph_id',
                'network': 'platform',
                'address': 'identity',
                'final_updated_nanosecond': 'updated_nanosecond'
            })
            address_part = address_part[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]

            fname_part = final_df[['farcaster_unique_id', 'final_graph_id', 'fname', 'final_updated_nanosecond']].copy()
            fname_part = fname_part.drop_duplicates(subset=['farcaster_unique_id'], keep='first')
            fname_part['platform'] = "farcaster"
            fname_part = fname_part.rename(columns={
                'farcaster_unique_id': 'unique_id',
                'final_graph_id': 'graph_id',
                'fname': 'identity',
                'final_updated_nanosecond': 'updated_nanosecond'
            })
            fname_part = fname_part[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]

            isolated_fname_part = isolate_fname_df[['farcaster_unique_id', 'final_graph_id', 'fname', 'final_updated_nanosecond']].copy()
            isolated_fname_part = isolated_fname_part.drop_duplicates(subset=['farcaster_unique_id'], keep='first')
            isolated_fname_part['platform'] = "farcaster"
            isolated_fname_part = isolated_fname_part.rename(columns={
                'farcaster_unique_id': 'unique_id',
                'final_graph_id': 'graph_id',
                'fname': 'identity',
                'final_updated_nanosecond': 'updated_nanosecond'
            })
            isolated_fname_part = isolated_fname_part[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]

            final_graph_id_df = pd.concat([address_part, fname_part, isolated_fname_part], ignore_index=True)
            final_graph_id_df = final_graph_id_df[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]
            final_graph_id_df['updated_nanosecond'] = final_graph_id_df['updated_nanosecond'].astype('int64')
            final_graph_id_df = final_graph_id_df.drop_duplicates(subset=['unique_id'], keep='first')
            final_graph_id_df.to_csv(allocation_path, index=False, quoting=csv.QUOTE_ALL)
            logging.debug("Successfully save %s row_count: %d", allocation_path, final_graph_id_df.shape[0])

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("processing farcaster_identity_graph end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing farcaster_identity_graph cost: %d", ts_delta)

    def save_graph_id(self, dump_batch_size=10000):
        farcaster_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/farcaster")
        if not os.path.exists(farcaster_process_dirs):
            raise FileNotFoundError(f"No directory {farcaster_process_dirs}")


        # allocation
        allocation_path = os.path.join(farcaster_process_dirs, "graph_id.csv")
        if not os.path.exists(allocation_path):
            raise FileNotFoundError(f"No data path {allocation_path}")

        start = time.time()
        logging.info("saving graph_id allocation start at: %s", \
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        insert_graph_id = """
        INSERT INTO graph_id (
            unique_id,
            graph_id,
            platform,
            identity,
            updated_nanosecond
        ) VALUES %s
        ON CONFLICT (unique_id)
        DO UPDATE SET
            graph_id = EXCLUDED.graph_id,
            platform = EXCLUDED.platform,
            identity = EXCLUDED.identity,
            updated_nanosecond = EXCLUDED.updated_nanosecond;
        """
        write_conn = psycopg2.connect(setting.PG_DSN["write"])
        write_conn.autocommit = True
        cursor = write_conn.cursor()

        cnt = 0
        batch = []
        batch_times = 0
        try:
            # Loading `graph_id.csv` for allocation
            with open(allocation_path, 'r', encoding="utf-8") as csvfile:
                csv_reader = csv.reader(csvfile)
                header = next(csv_reader)  # Skip the header
                logging.info("[%s] header: %s", allocation_path, header)
                batch = []
                for row in csv_reader:
                    cnt += 1
                    batch.append(row)
                    if len(batch) >= dump_batch_size:
                        # bulk insert
                        batch_times += 1
                        execute_values(
                            cursor, insert_graph_id, batch, template=None, page_size=dump_batch_size
                        )
                        logging.info("Upserted[graph_id] batch with size [%d], batch_times %d", len(batch), batch_times)
                        batch = []

                # remaining
                if batch:
                    batch_times += 1
                    execute_values(
                        cursor, insert_graph_id, batch, template=None, page_size=len(batch)
                    )
                    logging.info("Upserted[graph_id] batch with size [%d], batch_times %d", len(batch), batch_times)
            os.rename(allocation_path, allocation_path + ".finished")

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            write_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("saving graph_id allocation end at: %s", \
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("saving graph_id allocation cost: %d", ts_delta)

    def check_status(self, job_id):
        '''
        description: Check Job Status
        {
            "error": false,
            "message": "",
            "results": [{
                "overall": {
                    "duration": 1205,
                    "size": 1252574,
                    "progress": 1,
                    "startTime": 1727371282760,
                    "averageSpeed": 13249,
                    "id": "SocialGraph.Load_Test.file.m1.1727371282757",
                    "endTime": 1727371284150,
                    "currentSpeed": 13249,
                    "status": "FINISHED",
                    "statistics": {
                        "fileLevel": {
                            "validLine": 15965
                        },
                        "objectLevel": {
                            "vertex": [{
                                "validObject": 15965,
                                "typeName": "Identities"
                            }]
                        }
                    }
                },
                "workers": [{
                    "tasks": [{
                        "filename": "/home/tigergraph/shared_data/import_graphs/ensname/Identities.csv"
                    }]
                }]
            }]
        }
        '''
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + setting.TIGERGRAPH_SETTINGS["social_graph_token"]
        }
        # # GET 'http://hostname:restpp/gsql/v1/loading-jobs/status/{{job_id}}?graph=SocialGraph'
        status_job_url = "http://{}:{}/gsql/v1/loading-jobs/status/{}?graph={}".format(
            setting.TIGERGRAPH_SETTINGS["host"],
            setting.TIGERGRAPH_SETTINGS["restpp"],
            job_id,
            setting.TIGERGRAPH_SETTINGS["social_graph_name"])
        response = requests.get(
            url=status_job_url,
            headers=headers,
            timeout=60
        )
        raw_text = response.text
        res = json.loads(raw_text)
        if "error" in res:
            if res["error"] is True:
                error_msg = "graphdb_server check job status[{}] failed: url={}, error={}".format(
                    status_job_url, job_id, res)
                logging.error(error_msg)
                raise Exception(error_msg)

            job_status = None
            if len(res["results"]) > 0:
                overall = res["results"][0].get("overall", None)
                if overall is not None:
                    job_status = overall.get("status", None)

            return job_status

    def get_loading_job_status(self, job_id):
        max_times = 40
        sleep_second = 15
        status = None
        cnt = 0
        try:
            while status != "FINISHED" and status != "FAILED" and cnt < max_times:
                status = self.check_status(job_id)
                cnt += 1
                logging.debug("%s %s", job_id, status)
                if status is not None:
                    if status == "FINISHED" or status == "FAILED":
                        break
                else:
                    logging.error("check_status return None, job_id=%s", job_id)
                time.sleep(sleep_second)

                if status == "FAILED":
                    raise Exception("job_id=[%s] check_status[%s]", job_id, status)
                if cnt >= max_times:
                    raise Exception("job_id=[%s] check_status timeout(%d)", job_id, sleep_second * max_times)
            
            return status
        except Exception as ex:
            raise ex

    def run_loading_job(self):
        # POST 'http://hostname:restpp/gsql/v1/loading-jobs/run?graph=SocialGraph'
        # -d '[{"name":"Job_Name","sys.data_root":"/tmp","dataSources":[]}]'
        farcaster_loading_job_name = "Load_Farcaster"
        start = time.time()
        logging.info("run loading job[%s] start at: %s", \
                farcaster_loading_job_name,
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        job_run_url = "http://{}:{}/gsql/v1/loading-jobs/run?graph={}".format(
            setting.TIGERGRAPH_SETTINGS["host"],
            setting.TIGERGRAPH_SETTINGS["restpp"],
            setting.TIGERGRAPH_SETTINGS["social_graph_name"])
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + setting.TIGERGRAPH_SETTINGS["social_graph_token"]
        }
        payload = [{
            "name": farcaster_loading_job_name,
            "sys.data_root": setting.TIGERGRAPH_SETTINGS["graph_data_root"],
            "dataSources": []
        }]
        response = requests.post(url=job_run_url, data=json.dumps(payload), headers=headers, timeout=60)
        if response.status_code != 200:
            error_msg = "graphdb_server run loading job[{}] failed: url={}, {} {}".format(
                job_run_url, farcaster_loading_job_name, response.status_code, response.reason)
            logging.warn(error_msg)
            raise Exception(error_msg)

        raw_text = response.text
        res = json.loads(raw_text)
        # {
        #     "jobIds": ["jobId"],
        #     "messages": "Successfully ran loading job(s): [Load_Ens].",
        #     "error": false,
        #     "message": ""
        # }
        if "error" in res:
            if res["error"] is True:
                error_msg = "graphdb_server run loading job[{}] failed: url={}, error={}".format(
                    job_run_url, farcaster_loading_job_name, res)
                logging.error(error_msg)
                raise Exception(error_msg)
            else:
                job_ids = res.get("jobIds")
                if len(job_ids) == 0:
                    error_msg = "graphdb_server run loading job[{}] failed: url={}, job_ids={} job_ids is empty".format(
                        job_run_url, farcaster_loading_job_name, job_ids)
                    logging.error(error_msg)
                    raise Exception(error_msg)
                else:
                    job_id = job_ids[0]
                    logging.info("Successfully run loading job(s): [{}]".format(job_id))
                    
                    # Check Job Status
                    status = self.get_loading_job_status(job_id)
                    logging.info("run loading job(s): [{}] status[{}]".format(job_id, status))

        end = time.time()
        ts_delta = end - start
        logging.info("run loading job[%s] end at: %s", \
                    farcaster_loading_job_name,
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("run loading job[%s] cost: %d", farcaster_loading_job_name, ts_delta)

    def dumps_to_graphdb(self):
        try:
            self.update_job_status("start")
            self.update_job_status("running")
            self.process_farcaster_identity_graph()
            self.save_graph_id()
            self.run_loading_job()
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

    FarcasterGraphDB().process_farcaster_identity_graph()
    # FarcasterGraphDB().save_graph_id()
    # FarcasterGraphDB().run_loading_job()
