#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-26 16:48:23
LastEditors: Zella Zhong
LastEditTime: 2024-09-29 02:50:12
FilePath: /data_process/src/jobs/ens_graphdb_job.py
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


def combine_logic(row):
    ens_graph_id = row['graph_id_ens']
    ens_updated_ns = row['updated_nanosecond_ens']
    eth_graph_id = row['graph_id_ethereum']
    eth_updated_ns = row['updated_nanosecond_ethereum']

    if pd.notna(ens_graph_id) and pd.notna(eth_graph_id):
        # Case 1: Both exist
        if ens_graph_id == eth_graph_id:
            return ens_graph_id, int(ens_updated_ns), eth_graph_id, int(eth_updated_ns), "both_exist_and_same"
        else:
            return eth_graph_id, int(eth_updated_ns), eth_graph_id, int(eth_updated_ns), "both_exist_but_use_ethereum_graph_id"

    elif pd.notna(ens_graph_id) and pd.isna(eth_graph_id):
        # Case 2: ens_unique_id exists but ethereum_unique_id does not exist
        new_graph_id = str(uuid.uuid4())
        current_time_ns = int(get_unix_milliconds())
        return new_graph_id, current_time_ns, new_graph_id, current_time_ns, "only_ens_exist_use_new_graph_id"

    elif pd.isna(ens_graph_id) and pd.notna(eth_graph_id):
        # Case 3: ethereum_unique_id exists but ens_unique_id does not exist
        return eth_graph_id, int(eth_updated_ns), eth_graph_id, int(eth_updated_ns), "only_ethereum_exist_use_ethereum_graph_id"

    else:
        # Case 4: Neither exist
        new_graph_id = str(uuid.uuid4())
        current_time_ns = int(get_unix_milliconds())
        return new_graph_id, current_time_ns, new_graph_id, current_time_ns, "both_missing"


class EnsGraphDB(object):
    def __init__(self):
        self.job_name = "ens_graphdb_job"
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

    def process_ensname_temp_reverse(self):
        graphdb_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/temp_reverse")
        if not os.path.exists(graphdb_process_dirs):
            os.makedirs(graphdb_process_dirs)

        # vertices
        etheruem_path = os.path.join(graphdb_process_dirs, "ethereum.Identities.csv")
        name_path = os.path.join(graphdb_process_dirs, "ensname.Identities.csv")
        identities_graph_path = os.path.join(graphdb_process_dirs, "IdentitiesGraph.csv")
        # edges
        part_of_identities_graph_path = os.path.join(graphdb_process_dirs, "PartOfIdentitiesGraph.csv")
        reverse_resolve_path = os.path.join(graphdb_process_dirs, "Reverse_Resolve.csv")

        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        cursor = read_conn.cursor()

        try:
            temp_reverse = "temp_reverse"
            columns = ['graph_id', 'updated_nanosecond', 'name', 'reverse_address', 'expire_time']
            select_sql = "SELECT %s FROM %s" % (",".join(columns), temp_reverse)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            temp_reverse_df = pd.DataFrame(rows, columns=columns)

            temp_reverse_df['expire_time'] = pd.to_datetime(temp_reverse_df['expire_time'], errors='coerce')

            name_df = temp_reverse_df[['name', 'expire_time']].copy()
            name_df['primary_id'] = name_df['name'].apply(lambda x: f"ens,{x}")
            name_df['platform'] = 'ens'
            name_df.loc[:, 'identity'] = name_df['name']
            name_df.loc[:, 'display_name'] = name_df['name']
            name_df['reverse'] = True
            name_df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            name_df['expired_at'] = name_df['expire_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            name_df['uuid'] = name_df.apply(lambda _: str(uuid.uuid4()), axis=1)
            name_df = name_df[['primary_id', 'uuid', 'platform', 'identity', 'display_name', 'updated_at', 'reverse', 'expired_at']]
            name_df.to_csv(name_path, sep='\t', index=False)

            ethereum_df = temp_reverse_df[['reverse_address', 'name']].copy()
            ethereum_df['primary_id'] = ethereum_df['reverse_address'].apply(lambda x: f"ethereum,{x}")
            ethereum_df['platform'] = 'ethereum'
            ethereum_df.loc[:, 'identity'] = ethereum_df['reverse_address']
            ethereum_df.loc[:, 'display_name'] = ethereum_df['name']
            ethereum_df['reverse'] = True
            ethereum_df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ethereum_df['uuid'] = ethereum_df.apply(lambda _: str(uuid.uuid4()), axis=1)
            ethereum_df = ethereum_df[['primary_id', 'uuid', 'platform', 'identity', 'display_name', 'reverse', 'updated_at']]
            ethereum_df.to_csv(etheruem_path, sep='\t', index=False)

            identities_graph_df = temp_reverse_df[['graph_id', 'updated_nanosecond']].copy()
            identities_graph_df = identities_graph_df.drop_duplicates(subset=['graph_id'], keep='last')
            identities_graph_df.to_csv(identities_graph_path, sep='\t', index=False)

            partof_ethereum = temp_reverse_df[['reverse_address', 'graph_id']].copy()
            partof_ethereum['ethereum_unique_id'] = partof_ethereum['reverse_address'].apply(lambda x: f"ethereum,{x}")
            partof_ethereum = partof_ethereum[['ethereum_unique_id', 'graph_id']]
            partof_ethereum = partof_ethereum.drop_duplicates(subset=['ethereum_unique_id', 'graph_id'], keep='last')
            # rename the columns
            partof_ethereum = partof_ethereum.rename(columns={
                'ethereum_unique_id': 'from',
                'graph_id': 'to'
            })

            partof_ensname = temp_reverse_df[['name', 'graph_id']].copy()
            partof_ensname['ens_unique_id'] = partof_ensname['name'].apply(lambda x: f"ens,{x}")
            partof_ensname = partof_ensname[['ens_unique_id', 'graph_id']]
            partof_ensname = partof_ensname.drop_duplicates(subset=['ens_unique_id', 'graph_id'], keep='last')
            # rename the columns
            partof_ensname = partof_ensname.rename(columns={
                'ens_unique_id': 'from',
                'graph_id': 'to'
            })

            part_of_identities_graph_df = pd.concat([partof_ensname, partof_ethereum])
            part_of_identities_graph_df.to_csv(part_of_identities_graph_path, sep='\t', index=False)

            # Reverse.csv
            reverse_grouped = temp_reverse_df[['name', 'reverse_address']].copy()
            reverse_grouped['from'] = reverse_grouped.apply(lambda x: f"ethereum,{x['reverse_address']}", axis=1)
            reverse_grouped['to'] = reverse_grouped.apply(lambda x: f"ens,{x['name']}", axis=1)
            reverse_grouped['source'] = "the_graph"
            reverse_grouped['system'] = "ens"
            reverse_grouped['uuid'] = reverse_grouped.apply(lambda _: str(uuid.uuid4()), axis=1)
            reverse_grouped['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            reverse_grouped['fetcher'] = "relation_service"
            reverse_resolve_df = reverse_grouped[['from', 'to', 'source', 'system', 'name', 'uuid', 'updated_at', 'fetcher']]
            reverse_resolve_df.to_csv(reverse_resolve_path, sep='\t', index=False)

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()

    def process_ensname_identity_graph(self):
        graphdb_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/ensname")
        if not os.path.exists(graphdb_process_dirs):
            os.makedirs(graphdb_process_dirs)

        start = time.time()
        logging.info("processing ensname_identity_graph start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        # allocation
        allocation_path = os.path.join(graphdb_process_dirs, "graph_id.csv")
        # vertices
        identities_path = os.path.join(graphdb_process_dirs, "Identities.csv")
        identities_graph_path = os.path.join(graphdb_process_dirs, "IdentitiesGraph.csv")
        # edges
        part_of_identities_graph_path = os.path.join(graphdb_process_dirs, "PartOfIdentitiesGraph.csv")
        # proof_path = os.path.join(graphdb_process_dirs, "Proof_Forward.csv")
        hold_path = os.path.join(graphdb_process_dirs, "Hold.csv")
        resolve_path = os.path.join(graphdb_process_dirs, "Resolve.csv")
        reverse_resolve_path = os.path.join(graphdb_process_dirs, "Reverse_Resolve.csv")

        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        cursor = read_conn.cursor()
        try:
            ensname = "ensname"
            columns = ['name', 'is_wrapped', 'wrapped_owner', 'owner', 'resolved_address', 'reverse_address']
            select_sql = "SELECT %s FROM %s WHERE name is not null" % (",".join(columns), ensname)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            ensnames_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table ensname row_count: %d" % ensnames_df.shape[0])

            # Check if 'is_wrapped' is True, then replace 'owner' with 'wrapped_owner'
            ensnames_df.loc[ensnames_df['is_wrapped'] == True, 'owner'] = ensnames_df['wrapped_owner']
            ensnames_df = ensnames_df[['name', 'owner', 'resolved_address', 'reverse_address']]

            # Generate vertices and edges loading jobs for graphdb
            # Identities.csv
            ethereum_df = ensnames_df.melt(id_vars=['name'], var_name='field', value_name='identity')
            ethereum_df = ethereum_df.dropna(subset=['identity'])
            ethereum_df['primary_id'] = ethereum_df.apply(lambda x: f"ethereum,{x['identity']}", axis=1)
            ethereum_df['platform'] = 'ethereum'
            ethereum_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ethereum_df = ethereum_df[['primary_id', 'platform', 'identity', 'update_time']]
            ethereum_df = ethereum_df.drop_duplicates(subset=['identity'], keep='last')

            name_df = ensnames_df[['name']].copy()
            name_df['primary_id'] = name_df['name'].apply(lambda x: f"ens,{x}")
            name_df['platform'] = 'ens'
            name_df['identity'] = name_df['name']
            name_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            name_df = name_df[['primary_id', 'platform', 'identity', 'update_time']]

            identities_df = pd.concat([name_df, ethereum_df])
            identities_df.to_csv(identities_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", identities_path, identities_df.shape[0])

            # Hold.csv
            hold_df = ensnames_df[ensnames_df['owner'].notna()]
            hold_grouped = hold_df.groupby(['name', 'owner'], as_index=False).first()
            hold_grouped['from'] = hold_grouped.apply(lambda x: f"ethereum,{x['owner']}", axis=1)
            hold_grouped['to'] = hold_grouped.apply(lambda x: f"ens,{x['name']}", axis=1)
            hold_grouped['source'] = "ens"
            hold_grouped['level'] = 5
            hold_df = hold_grouped[['from', 'to', 'source', 'level']]
            hold_df.to_csv(hold_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", hold_path, hold_df.shape[0])

            # Resolve.csv
            resolve_df = ensnames_df[ensnames_df['resolved_address'].notna()]
            resolve_grouped = resolve_df.groupby(['name', 'resolved_address'], as_index=False).first()
            resolve_grouped['from'] = resolve_grouped.apply(lambda x: f"ens,{x['name']}", axis=1)
            resolve_grouped['to'] = resolve_grouped.apply(lambda x: f"ethereum,{x['resolved_address']}", axis=1)
            resolve_grouped['source'] = "ens"
            resolve_grouped['level'] = 5
            resolve_df = resolve_grouped[['from', 'to', 'source', 'level']]
            resolve_df.to_csv(resolve_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", resolve_path, resolve_df.shape[0])

            # Reverse.csv
            reverse_resolve_df = ensnames_df[ensnames_df['reverse_address'].notna()]
            reverse_grouped = reverse_resolve_df.groupby(['name', 'reverse_address'], as_index=False).first()
            reverse_grouped['from'] = reverse_grouped.apply(lambda x: f"ethereum,{x['reverse_address']}", axis=1)
            reverse_grouped['to'] = reverse_grouped.apply(lambda x: f"ens,{x['name']}", axis=1)
            reverse_grouped['source'] = "ens"
            reverse_grouped['level'] = 5
            reverse_resolve_df = reverse_grouped[['from', 'to', 'source', 'level']]
            reverse_resolve_df.to_csv(reverse_resolve_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", reverse_resolve_path, reverse_resolve_df.shape[0])

            # Loading graph_id allocation table
            graph_id_table = "graph_id"
            columns = ['unique_id', 'graph_id', 'updated_nanosecond']
            select_sql = "SELECT %s FROM %s WHERE platform in ('ethereum', 'ens')" % (",".join(columns), graph_id_table)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            allocation_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table graph_id row_count: %d", allocation_df.shape[0])

            # only resolved_address == owner can add to identity_graph, otherwise ens just `Hold`
            filter_df = ensnames_df[(ensnames_df['owner'].notna()) &
                           (ensnames_df['resolved_address'].notna()) &
                           (ensnames_df['owner'] == ensnames_df['resolved_address'])]
            filter_df = filter_df[['name', 'owner', 'resolved_address']]

            # concat unique_id
            filter_df['ens_unique_id'] = "ens," + filter_df['name']
            filter_df['ethereum_unique_id'] = "ethereum," + filter_df['resolved_address']
            final_df = filter_df[['ens_unique_id', 'ethereum_unique_id', 'name', 'resolved_address']]

            logging.debug("Start merge final_df row_count: %d and allocation_df row_count: %d", final_df.shape[0], allocation_df.shape[0])
            # merge final_df with allocation_df for both `ens_unique_id` and `ethereum_unique_id`

            final_df = pd.merge(final_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                    left_on='ens_unique_id', right_on='unique_id', how='left', suffixes=('', '_ens'))

            final_df = pd.merge(final_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                    left_on='ethereum_unique_id', right_on='unique_id', how='left', suffixes=('', '_ethereum'))

            logging.debug("Successfully merge final_df and allocation_df to final_df row_count: %d", final_df.shape[0])
            # ['ens_unique_id', 'ethereum_unique_id', 'name', 'resolved_address',
            # 'unique_id', 'graph_id', 'updated_nanosecond', 'unique_id_ethereum',
            # 'graph_id_ethereum', 'updated_nanosecond_ethereum']
            final_df.drop(columns=['unique_id', 'unique_id_ethereum'], inplace=True)
            final_df = final_df.rename(columns={
                'graph_id': 'graph_id_ens',
                'updated_nanosecond': 'updated_nanosecond_ens'
            })

            logging.debug("Start combine_logic...")
            final_df[
                [
                    'ens_graph_id',
                    'ens_updated_nanosecond',
                    'ethereum_graph_id',
                    'ethereum_updated_nanosecond',
                    'combine_type'
            ]] = final_df.apply(combine_logic, axis=1, result_type="expand")
            logging.debug("End combine_logic...")

            # select relevant columns
            final_df = final_df[['combine_type', 'ethereum_unique_id', 'ethereum_graph_id', 'ethereum_updated_nanosecond',
                                'ens_unique_id', 'ens_graph_id', 'ens_updated_nanosecond', 'name', 'resolved_address']]

            identities_graph_df = final_df[['ethereum_graph_id', 'ethereum_updated_nanosecond']].copy()
            identities_graph_df = identities_graph_df[['ethereum_graph_id', 'ethereum_updated_nanosecond']]
            identities_graph_df = identities_graph_df.drop_duplicates(subset=['ethereum_graph_id'], keep='last')
            # rename the columns
            identities_graph_df = identities_graph_df.rename(columns={
                'ethereum_graph_id': 'primary_id',
                'ethereum_updated_nanosecond': 'updated_nanosecond'
            })
            identities_graph_df.to_csv(identities_graph_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", identities_graph_path, identities_graph_df.shape[0])

            partof_ethereum = final_df[['ethereum_unique_id', 'ethereum_graph_id']].copy()
            partof_ethereum = partof_ethereum[['ethereum_unique_id', 'ethereum_graph_id']]
            partof_ethereum = partof_ethereum.drop_duplicates(subset=['ethereum_unique_id', 'ethereum_graph_id'], keep='last')
            # rename the columns
            partof_ethereum = partof_ethereum.rename(columns={
                'ethereum_unique_id': 'from',
                'ethereum_graph_id': 'to'
            })

            partof_ensname = final_df[['ens_unique_id', 'ens_graph_id']].copy()
            partof_ensname = partof_ensname[['ens_unique_id', 'ens_graph_id']]
            partof_ensname = partof_ensname.drop_duplicates(subset=['ens_unique_id', 'ens_graph_id'], keep='last')
            # rename the columns
            partof_ensname = partof_ensname.rename(columns={
                'ens_unique_id': 'from',
                'ens_graph_id': 'to'
            })

            part_of_identities_graph_df = pd.concat([partof_ensname, partof_ethereum])
            part_of_identities_graph_df.to_csv(part_of_identities_graph_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", part_of_identities_graph_path, part_of_identities_graph_df.shape[0])

            # Filter out rows where combine_type is "both_exist_and_same"
            ethereum_part = final_df[final_df['combine_type'] != "both_exist_and_same"]
            ethereum_part = ethereum_part[['ethereum_unique_id', 'ethereum_graph_id', 'resolved_address', 'ethereum_updated_nanosecond']].copy()
            ethereum_part['platform'] = 'ethereum'
            ethereum_part = ethereum_part.rename(columns={
                'ethereum_unique_id': 'unique_id',
                'ethereum_graph_id': 'graph_id',
                'resolved_address': 'identity',
                'ethereum_updated_nanosecond': 'updated_nanosecond'
            })

            ens_part = final_df[final_df['combine_type'] != "both_exist_and_same"]
            ens_part = ens_part[['ens_unique_id', 'ens_graph_id', 'name', 'ens_updated_nanosecond']].copy()
            ens_part['platform'] = 'ens'
            ens_part = ens_part.rename(columns={
                'ens_unique_id': 'unique_id',
                'ens_graph_id': 'graph_id',
                'name': 'identity',
                'ens_updated_nanosecond': 'updated_nanosecond'
            })

            final_graph_id_df = pd.concat([ethereum_part, ens_part], ignore_index=True)
            final_graph_id_df = final_graph_id_df[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]
            final_graph_id_df = final_graph_id_df.drop_duplicates(subset=['unique_id'], keep='last')
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
        logging.info("processing ensname_identity_graph end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing ensname_identity_graph cost: %d", ts_delta)

    def save_graph_id(self, dump_batch_size=10000):
        graphdb_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/ensname")
        if not os.path.exists(graphdb_process_dirs):
            raise FileNotFoundError(f"No directory {graphdb_process_dirs}")

        # allocation
        allocation_path = os.path.join(graphdb_process_dirs, "graph_id.csv")
        if not os.path.exists(allocation_path):
            raise FileNotFoundError(f"No data path {allocation_path}")

        # df = pd.read_csv(allocation_path)
        # print(df.shape[0])
        # df_deduped = df.drop_duplicates(subset=['unique_id'], keep='last')
        # print(df_deduped.shape[0])

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
        ens_loading_job_name = "Load_Ens"
        start = time.time()
        logging.info("run loading job[%s] start at: %s", \
                ens_loading_job_name,
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
            "name": ens_loading_job_name,
            "sys.data_root": setting.TIGERGRAPH_SETTINGS["graph_data_root"],
            "dataSources": []
        }]
        response = requests.post(url=job_run_url, data=json.dumps(payload), headers=headers, timeout=60)
        if response.status_code != 200:
            error_msg = "graphdb_server run loading job[{}] failed: url={}, {} {}".format(
                job_run_url, ens_loading_job_name, response.status_code, response.reason)
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
                    job_run_url, ens_loading_job_name, res)
                logging.error(error_msg)
                raise Exception(error_msg)
            else:
                job_ids = res.get("jobIds")
                if len(job_ids) == 0:
                    error_msg = "graphdb_server run loading job[{}] failed: url={}, job_ids={} job_ids is empty".format(
                        job_run_url, ens_loading_job_name, job_ids)
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
                    ens_loading_job_name,
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("run loading job[%s] cost: %d", ens_loading_job_name, ts_delta)

    def dumps_to_graphdb(self):
        try:
            # self.update_job_status("start")
            # self.update_job_status("running")
            self.process_ensname_identity_graph()
            self.save_graph_id()
            self.run_loading_job()
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

    EnsGraphDB().process_ensname_identity_graph()
    # EnsGraphDB().save_graph_id()
    # EnsGraphDB().run_loading_job()