#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-27 00:12:45
LastEditors: Zella Zhong
LastEditTime: 2024-09-29 17:39:17
FilePath: /data_process/src/jobs/lens_graphdb_job.py
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
    new_graph_id = str(uuid.uuid4())
    current_time_ns = int(get_unix_milliconds())
    return new_graph_id, current_time_ns

def combine_logic(row):
    lens_graph_id = row['graph_id_lens']
    lens_updated_ns = row['updated_nanosecond_lens']
    eth_graph_id = row['graph_id_ethereum']
    eth_updated_ns = row['updated_nanosecond_ethereum']

    if pd.notna(lens_graph_id) and pd.notna(eth_graph_id):
        # Case 1: Both exist
        if lens_graph_id == eth_graph_id:
            return lens_graph_id, int(lens_updated_ns), eth_graph_id, int(eth_updated_ns), "both_exist_and_same"
        else:
            return eth_graph_id, int(eth_updated_ns), eth_graph_id, int(eth_updated_ns), "both_exist_but_use_ethereum_graph_id"

    elif pd.notna(lens_graph_id) and pd.isna(eth_graph_id):
        # Case 2: ens_unique_id exists but ethereum_unique_id does not exist
        new_graph_id = str(uuid.uuid4())
        current_time_ns = int(get_unix_milliconds())
        return new_graph_id, current_time_ns, new_graph_id, current_time_ns, "only_ens_exist_use_new_graph_id"

    elif pd.isna(lens_graph_id) and pd.notna(eth_graph_id):
        # Case 3: ethereum_unique_id exists but ens_unique_id does not exist
        return eth_graph_id, int(eth_updated_ns), eth_graph_id, int(eth_updated_ns), "only_ethereum_exist_use_ethereum_graph_id"

    else:
        # Case 4: Neither exist
        new_graph_id = str(uuid.uuid4())
        current_time_ns = int(get_unix_milliconds())
        return new_graph_id, current_time_ns, new_graph_id, current_time_ns, "both_missing"


class LensGraphDB(object):
    def __init__(self):
        self.job_name = "lens_graphdb_job"
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
    
    def process_lens_identity_graph(self):
        graphdb_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/lens")
        if not os.path.exists(graphdb_process_dirs):
            os.makedirs(graphdb_process_dirs)
        
        start = time.time()
        logging.info("processing lens_identity_graph start at: %s", \
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
            lens_profile = "lensv2_profile"
            columns = ['name', 'is_primary', 'address']
            select_sql = "SELECT %s FROM %s WHERE name is not null" % (",".join(columns), lens_profile)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            lens_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table lens_profile row_count: %d" % lens_df.shape[0])

            # Filter rows where 'name' length is less than 1024 characters
            lens_df = lens_df[lens_df['name'].str.len() < 1024]

            # Generate vertices and edges loading jobs for graphdb
            # Identities.csv
            ethereum_df = lens_df[['address']].copy()
            ethereum_df = ethereum_df.dropna(subset=['address'])
            # NOTICE: lens network is `polygon`, in order to connect identity to identity_graph
            # We use `ethereum` to replace all of ether-like address
            ethereum_df['primary_id'] = ethereum_df['address'].apply(lambda x: f"ethereum,{x}")
            ethereum_df['platform'] = "ethereum"
            ethereum_df['identity'] = ethereum_df['address']
            ethereum_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ethereum_df = ethereum_df.drop_duplicates(subset=['identity'], keep='first')
            ethereum_df = ethereum_df[['primary_id', 'platform', 'identity', 'update_time']]

            name_df = lens_df[['name']].copy()
            name_df = name_df.dropna(subset=['name'])
            name_df['primary_id'] = name_df['name'].apply(lambda x: f"lens,{x}")
            name_df['platform'] = 'lens'
            name_df['identity'] = name_df['name']
            name_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            name_df = name_df.drop_duplicates(subset=['identity'], keep='first')
            name_df = name_df[['primary_id', 'platform', 'identity', 'update_time']]

            # Identities.csv
            identities_df = pd.concat([name_df, ethereum_df])
            identities_df.to_csv(identities_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", identities_path, identities_df.shape[0])

            # Hold.csv
            hold_grouped = lens_df.groupby(['name', 'address'], as_index=False).first()
            hold_grouped = hold_grouped[hold_grouped['address'] != '0x0000000000000000000000000000000000000000']
            hold_grouped['from'] = hold_grouped.apply(lambda x: f"ethereum,{x['address']}", axis=1)
            hold_grouped['to'] = hold_grouped.apply(lambda x: f"lens,{x['name']}", axis=1)
            hold_grouped['source'] = "lens"
            hold_grouped['level'] = 5
            hold_df = hold_grouped[['from', 'to', 'source', 'level']]
            hold_df.to_csv(hold_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", hold_path, hold_df.shape[0])

            # Resolve.csv
            resolve_grouped = lens_df.groupby(['name', 'address'], as_index=False).first()
            resolve_grouped = resolve_grouped[resolve_grouped['address'] != '0x0000000000000000000000000000000000000000']
            resolve_grouped['from'] = resolve_grouped.apply(lambda x: f"ethereum,{x['address']}", axis=1)
            resolve_grouped['to'] = resolve_grouped.apply(lambda x: f"lens,{x['name']}", axis=1)
            resolve_grouped['source'] = "lens"
            resolve_grouped['level'] = 5
            resolve_df = resolve_grouped[['from', 'to', 'source', 'level']]
            resolve_df.to_csv(resolve_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", resolve_path, resolve_df.shape[0])

            # Reverse.csv
            reverse_grouped = lens_df[lens_df['is_primary'] == True]
            reverse_grouped = reverse_grouped.groupby(['name', 'address'], as_index=False).first()
            reverse_grouped = reverse_grouped[reverse_grouped['address'] != '0x0000000000000000000000000000000000000000']
            reverse_grouped['from'] = reverse_grouped.apply(lambda x: f"ethereum,{x['address']}", axis=1)
            reverse_grouped['to'] = reverse_grouped.apply(lambda x: f"lens,{x['name']}", axis=1)
            reverse_grouped['source'] = "lens"
            reverse_grouped['level'] = 5
            reverse_resolve_df = reverse_grouped[['from', 'to', 'source', 'level']]
            reverse_resolve_df.to_csv(reverse_resolve_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", reverse_resolve_path, reverse_resolve_df.shape[0])

            # Loading graph_id allocation table
            graph_id_table = "graph_id"
            columns = ['unique_id', 'graph_id', 'updated_nanosecond']
            select_sql = "SELECT %s FROM %s WHERE platform in ('ethereum', 'lens')" % (",".join(columns), graph_id_table)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            allocation_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table graph_id row_count: %d", allocation_df.shape[0])

            lens_df['lens_unique_id'] = "lens," + lens_df['name']
            lens_df['ethereum_unique_id'] = "ethereum," + lens_df['address']
            lens_owner_df = lens_df[['ethereum_unique_id']].copy()
            lens_owner_df = lens_owner_df.drop_duplicates(subset=['ethereum_unique_id'],keep='first')
            lens_owner_df[
                [
                    'new_ethereum_graph_id',
                    'new_ethereum_updated_nanosecond'
            ]] = lens_owner_df.apply(generate_new_graph_id, axis=1, result_type="expand")

            final_df = lens_df[['lens_unique_id', 'ethereum_unique_id', 'name', 'address']]

            # merge final_df with allocation_df for both `lens_unique_id` and `ethereum_unique_id`
            logging.debug("Start merge final_df row_count: %d and allocation_df row_count: %d", final_df.shape[0], allocation_df.shape[0])

            final_df = pd.merge(final_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                    left_on='lens_unique_id', right_on='unique_id', how='left', suffixes=('', '_lens'))
            final_df = pd.merge(final_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                    left_on='ethereum_unique_id', right_on='unique_id', how='left', suffixes=('', '_ethereum'))

            final_df.drop(columns=['unique_id', 'unique_id_ethereum'], inplace=True)
            final_df = final_df.rename(columns={
                'graph_id': 'graph_id_lens',
                'updated_nanosecond': 'updated_nanosecond_lens'
            })

            logging.debug("Successfully merge final_df and allocation_df to final_df row_count: %d", final_df.shape[0])

            logging.debug("Start combine_logic...")
            final_df[
                [
                    'lens_graph_id',
                    'lens_updated_nanosecond',
                    'ethereum_graph_id',
                    'ethereum_updated_nanosecond',
                    'combine_type'
            ]] = final_df.apply(combine_logic, axis=1, result_type="expand")
            logging.debug("End combine_logic...")

            # select relevant columns
            final_df = final_df[['combine_type', 'ethereum_unique_id', 'ethereum_graph_id', 'ethereum_updated_nanosecond',
                                'lens_unique_id', 'lens_graph_id', 'lens_updated_nanosecond', 'name', 'address']]

            identities_graph_df = final_df[['ethereum_graph_id', 'ethereum_updated_nanosecond']].copy()
            # rename the columns
            identities_graph_df = identities_graph_df.rename(columns={
                'ethereum_graph_id': 'primary_id',
                'ethereum_updated_nanosecond': 'updated_nanosecond'
            })
            identities_graph_df = identities_graph_df.drop_duplicates(subset=['primary_id'], keep='first')
            identities_graph_df.to_csv(identities_graph_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", identities_graph_path, identities_graph_df.shape[0])



        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()

        end = time.time()
        ts_delta = end - start
        logging.info("processing lens_identity_graph end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing lens_identity_graph cost: %d", ts_delta)