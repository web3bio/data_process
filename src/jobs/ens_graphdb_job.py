#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-26 16:48:23
LastEditors: Zella Zhong
LastEditTime: 2024-09-27 00:02:45
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

    def process_ensname_identity_graph(self):
        graphdb_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/ensname")
        if not os.path.exists(graphdb_process_dirs):
            os.makedirs(graphdb_process_dirs)

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
            select_sql = "SELECT %s FROM %s WHERE name is not null LIMIT 10000" % (",".join(columns), ensname)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            ensnames_df = pd.DataFrame(rows, columns=columns)

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
            ethereum_df = ethereum_df.drop_duplicates(subset=['identity'])

            name_df = ensnames_df[['name']].copy()
            name_df['primary_id'] = name_df['name'].apply(lambda x: f"ens,{x}")
            name_df['platform'] = 'ens'
            name_df['identity'] = name_df['name']
            name_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            name_df = name_df[['primary_id', 'platform', 'identity', 'update_time']]

            identities_df = pd.concat([name_df, ethereum_df])
            identities_df.to_csv(identities_path, sep='\t', index=False)

            # Hold.csv
            hold_df = ensnames_df[ensnames_df['owner'].notna()]
            hold_grouped = hold_df.groupby(['name', 'owner'], as_index=False).first()
            hold_grouped['from'] = hold_grouped.apply(lambda x: f"ethereum,{x['owner']}", axis=1)
            hold_grouped['to'] = hold_grouped.apply(lambda x: f"ens,{x['name']}", axis=1)
            hold_grouped['source'] = "ens"
            hold_grouped['level'] = 5
            hold_df = hold_grouped[['from', 'to', 'source', 'level']]
            hold_df.to_csv(hold_path, sep='\t', index=False)

            # Resolve.csv
            resolve_df = ensnames_df[ensnames_df['resolved_address'].notna()]
            resolve_grouped = resolve_df.groupby(['name', 'resolved_address'], as_index=False).first()
            resolve_grouped['from'] = resolve_grouped.apply(lambda x: f"ens,{x['name']}", axis=1)
            resolve_grouped['to'] = resolve_grouped.apply(lambda x: f"ethereum,{x['resolved_address']}", axis=1)
            resolve_grouped['source'] = "ens"
            resolve_grouped['level'] = 5
            resolve_df = resolve_grouped[['from', 'to', 'source', 'level']]
            resolve_df.to_csv(resolve_path, sep='\t', index=False)

            # Reverse.csv
            reverse_resolve_df = ensnames_df[ensnames_df['reverse_address'].notna()]
            reverse_grouped = reverse_resolve_df.groupby(['name', 'reverse_address'], as_index=False).first()
            reverse_grouped['from'] = reverse_grouped.apply(lambda x: f"ethereum,{x['reverse_address']}", axis=1)
            reverse_grouped['to'] = reverse_grouped.apply(lambda x: f"ens,{x['name']}", axis=1)
            reverse_grouped['source'] = "ens"
            reverse_grouped['level'] = 5
            reverse_resolve_df = reverse_grouped[['from', 'to', 'source', 'level']]
            reverse_resolve_df.to_csv(reverse_resolve_path, sep='\t', index=False)

            # Loading graph_id allocation table
            ensname = "graph_id"
            columns = ['unique_id', 'graph_id', 'updated_nanosecond']
            select_sql = "SELECT %s FROM %s" % (",".join(columns), ensname)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            allocation_df = pd.DataFrame(rows, columns=columns)

            # only resolved_address == owner can add to identity_graph, otherwise ens just `Hold`
            filter_df = ensnames_df[(ensnames_df['owner'].notna()) &
                           (ensnames_df['resolved_address'].notna()) &
                           (ensnames_df['owner'] == ensnames_df['resolved_address'])]
            filter_df = filter_df[['name', 'owner', 'resolved_address']]

            # concat unique_id
            filter_df['ens_unique_id'] = "ens," + filter_df['name']
            filter_df['ethereum_unique_id'] = "ethereum," + filter_df['resolved_address']
            final_df = filter_df[['ens_unique_id', 'ethereum_unique_id', 'name', 'resolved_address']]

            final_df.loc[:, 'ens_graph_id'] = None
            final_df.loc[:, 'ens_updated_nanosecond'] = None
            final_df.loc[:, 'ethereum_graph_id'] = None
            final_df.loc[:, 'ethereum_updated_nanosecond'] = None

            # Merge to find graph_id and updated_nanosecond for ens_unique_id
            merged_ens = pd.merge(final_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                                  left_on='ens_unique_id', right_on='unique_id', how='left')
            # Merge to find graph_id and updated_nanosecond for ethereum_unique_id
            merged_ethereum = pd.merge(final_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                                       left_on='ethereum_unique_id', right_on='unique_id', how='left')

            # Reset indexes to ensure alignment
            final_df = final_df.reset_index(drop=True)
            merged_ens = merged_ens.reset_index(drop=True)
            merged_ethereum = merged_ethereum.reset_index(drop=True)

            for idx, row in final_df.iterrows():
                ens_graph_id = merged_ens.loc[idx, 'graph_id']
                ens_updated_ns = merged_ens.loc[idx, 'updated_nanosecond']

                eth_graph_id = merged_ethereum.loc[idx, 'graph_id']
                eth_updated_ns = merged_ethereum.loc[idx, 'updated_nanosecond']

                # Case 1: Both ens_unique_id and ethereum_unique_id exist
                if pd.notna(ens_graph_id) and pd.notna(eth_graph_id):
                    if ens_graph_id == eth_graph_id:
                        # Both have the same graph_id and updated_nanosecond
                        final_df.loc[idx, 'ens_graph_id'] = ens_graph_id
                        final_df.loc[idx, 'ens_updated_nanosecond'] = int(ens_updated_ns)
                        final_df.loc[idx, 'ethereum_graph_id'] = eth_graph_id
                        final_df.loc[idx, 'ethereum_updated_nanosecond'] = int(eth_updated_ns)
                        final_df.loc[idx, 'combine_type'] = "both_exist_and_same"
                    else:
                        # Different graph_ids: update ens_unique_id to match ethereum_unique_id
                        final_df.loc[idx, 'ens_graph_id'] = eth_graph_id
                        final_df.loc[idx, 'ens_updated_nanosecond'] = int(eth_updated_ns)
                        final_df.loc[idx, 'ethereum_graph_id'] = eth_graph_id
                        final_df.loc[idx, 'ethereum_updated_nanosecond'] = int(eth_updated_ns)
                        final_df.loc[idx, 'combine_type'] = "both_exist_but_use_eth_graph_id"

                # Case 2: ens_unique_id exists but ethereum_unique_id does not exist
                elif pd.notna(ens_graph_id) and pd.isna(eth_graph_id):
                    new_graph_id = str(uuid.uuid4())
                    current_time_ns = int(get_unix_milliconds())

                    # Generate new for ethereum_unique_id and set both to the same
                    final_df.loc[idx, 'ethereum_graph_id'] = new_graph_id
                    final_df.loc[idx, 'ethereum_updated_nanosecond'] = current_time_ns
                    final_df.loc[idx, 'ens_graph_id'] = new_graph_id
                    final_df.loc[idx, 'ens_updated_nanosecond'] = current_time_ns
                    final_df.loc[idx, 'combine_type'] = "only_ens_exist_use_new_graph_id"

                # Case 3: ethereum_unique_id exists but ens_unique_id does not exist
                elif pd.isna(ens_graph_id) and pd.notna(eth_graph_id):
                    # Update ens_unique_id to match ethereum_unique_id's graph_id and updated_nanosecond
                    final_df.loc[idx, 'ens_graph_id'] = eth_graph_id
                    final_df.loc[idx, 'ens_updated_nanosecond'] = int(eth_updated_ns)
                    final_df.loc[idx, 'ethereum_graph_id'] = eth_graph_id
                    final_df.loc[idx, 'ethereum_updated_nanosecond'] = int(eth_updated_ns)
                    final_df.loc[idx, 'combine_type'] = "only_ethereum_exist_use_eth_graph_id"

                # Case 4: Neither exist, create new graph_id and updated_nanosecond for both
                elif pd.isna(ens_graph_id) and pd.isna(eth_graph_id):
                    new_graph_id = str(uuid.uuid4())
                    current_time_ns = int(get_unix_milliconds())
                    # Generate new for two and set both to the same
                    final_df.loc[idx, 'ens_graph_id'] = new_graph_id
                    final_df.loc[idx, 'ens_updated_nanosecond'] = current_time_ns
                    final_df.loc[idx, 'ethereum_graph_id'] = new_graph_id
                    final_df.loc[idx, 'ethereum_updated_nanosecond'] = current_time_ns
                    final_df.loc[idx, 'combine_type'] = "both_missing"

            final_df = final_df[['combine_type', 'ethereum_unique_id', 'ethereum_graph_id', 'ethereum_updated_nanosecond',
                                 'ens_unique_id', 'ens_graph_id', 'ens_updated_nanosecond', 'name', 'resolved_address']]

            identities_graph_df = final_df[['ethereum_graph_id', 'ethereum_updated_nanosecond']].copy()
            identities_graph_df = identities_graph_df[['ethereum_graph_id', 'ethereum_updated_nanosecond']]
            identities_graph_df = identities_graph_df.drop_duplicates(subset=['ethereum_graph_id'])
            # rename the columns
            identities_graph_df = identities_graph_df.rename(columns={
                'ethereum_graph_id': 'primary_id',
                'ethereum_updated_nanosecond': 'updated_nanosecond'
            })
            identities_graph_df.to_csv(identities_graph_path, sep='\t', index=False)

            partof_ethereum = final_df[['ethereum_unique_id', 'ethereum_graph_id']].copy()
            partof_ethereum = partof_ethereum[['ethereum_unique_id', 'ethereum_graph_id']]
            partof_ethereum = partof_ethereum.drop_duplicates(subset=['ethereum_unique_id', 'ethereum_graph_id'])
            # rename the columns
            partof_ethereum = partof_ethereum.rename(columns={
                'ethereum_unique_id': 'from',
                'ethereum_graph_id': 'to'
            })

            partof_ensname = final_df[['ens_unique_id', 'ens_graph_id']].copy()
            partof_ensname = partof_ensname[['ens_unique_id', 'ens_graph_id']]
            partof_ensname = partof_ensname.drop_duplicates(subset=['ens_unique_id', 'ens_graph_id'])
            # rename the columns
            partof_ensname = partof_ensname.rename(columns={
                'ens_unique_id': 'from',
                'ens_graph_id': 'to'
            })

            part_of_identities_graph_df = pd.concat([partof_ensname, partof_ethereum])
            part_of_identities_graph_df.to_csv(part_of_identities_graph_path, sep='\t', index=False)

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

            ens_part = final_df[['ens_unique_id', 'ens_graph_id', 'name', 'ens_updated_nanosecond']].copy()
            ens_part['platform'] = 'ens'
            ens_part = ens_part.rename(columns={
                'ens_unique_id': 'unique_id',
                'ens_graph_id': 'graph_id',
                'name': 'identity',
                'ens_updated_nanosecond': 'updated_nanosecond'
            })

            allocation_df = pd.concat([ethereum_part, ens_part], ignore_index=True)
            allocation_df.to_csv(allocation_path, index=False, quoting=csv.QUOTE_ALL)

        except Exception as ex:
            logging.exception(ex)
            raise ex
        finally:
            cursor.close()
            read_conn.close()


    def dumps_to_graphdb(self):
        # when it exists: update value
        # find the HyperVertex: IdentitiesGraph
        # if it doesn't exist: insert value
        # from（Identity）New, to（Identity）New: New_GraphID * 2
        # from（Identity）Old:From_GraphID, to（Identity）New:   Old:From_GraphID -> to
        # from（Identity）New, to（Identity）Old:To_GraphID: Old:To_GraphID -> from
        # from（Identity）Old:From_GraphID, to（Identity）Old:To_GraphID: Combine two GraphID if there are different
        pass


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    print(os.getenv("ENVIRONMENT"))
    import setting.filelogger as logger
    config = setting.load_settings(env=os.getenv("ENVIRONMENT"))
    logger.InitLogger(config)

    EnsGraphDB().process_ensname_identity_graph()