#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-27 00:12:45
LastEditors: Zella Zhong
LastEditTime: 2024-10-12 00:03:34
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
    graph_id_ethereum = row['graph_id_ethereum']
    updated_nanosecond_ethereum = row['updated_nanosecond_ethereum']
    if pd.isna(graph_id_ethereum):
        new_graph_id = str(uuid.uuid4())
        current_time_ns = int(get_unix_milliconds())
        return new_graph_id, current_time_ns, False
    else:
        return graph_id_ethereum, updated_nanosecond_ethereum, True


def combine_logic(row):
    graph_id_lens = row['graph_id_lens']
    updated_ns_lens = row['updated_nanosecond_lens']
    # new_eth_graph_id always exists because we generate(new or exist) before
    new_eth_graph_id = row['new_ethereum_graph_id']
    new_eth_updated_ns = row['new_ethereum_updated_nanosecond']

    if pd.notna(graph_id_lens) and pd.notna(new_eth_graph_id):
        # Case 1: Both exist
        if graph_id_lens == new_eth_graph_id:
            return graph_id_lens, int(updated_ns_lens), new_eth_graph_id, int(new_eth_updated_ns), "both_exist_and_same"
        else:
            return new_eth_graph_id, int(new_eth_updated_ns), new_eth_graph_id, int(new_eth_updated_ns), "both_exist_but_use_ethereum_graph_id"

    elif pd.isna(graph_id_lens) and pd.notna(new_eth_graph_id):
        # Case 3: ethereum_unique_id exists but ens_unique_id does not exist
        return new_eth_graph_id, int(new_eth_updated_ns), new_eth_graph_id, int(new_eth_updated_ns), "only_ethereum_exist_use_ethereum_graph_id"


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

            # merge final_df with allocation_df for both `lens_unique_id` and `ethereum_unique_id`
            logging.debug("Start merge lens_df row_count: %d and allocation_df row_count: %d", lens_df.shape[0], allocation_df.shape[0])

            # generate new graph_id or use existing graph_id in allocation_df
            lens_owner_df = lens_df[['ethereum_unique_id']].copy()
            lens_owner_df = lens_owner_df.drop_duplicates(subset=['ethereum_unique_id'],keep='first')

            lens_owner_df = pd.merge(lens_owner_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                                     left_on='ethereum_unique_id', right_on='unique_id', how='left', suffixes=('', '_ethereum'))
            lens_owner_df = lens_owner_df.rename(columns={
                'graph_id': 'graph_id_ethereum',
                'updated_nanosecond': 'updated_nanosecond_ethereum'
            })
            lens_owner_df[
                [
                    'new_ethereum_graph_id',
                    'new_ethereum_updated_nanosecond',
                    'is_exist'
            ]] = lens_owner_df.apply(generate_new_graph_id, axis=1, result_type="expand")
            logging.debug("Successfully merge lens_owner_df and allocation_df row_count: %d", lens_owner_df.shape[0])

            final_df = lens_df[['lens_unique_id', 'ethereum_unique_id', 'name', 'address']]

            final_df = pd.merge(final_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                    left_on='lens_unique_id', right_on='unique_id', how='left', suffixes=('', '_lens'))
            final_df = final_df.rename(columns={
                'graph_id': 'graph_id_lens',
                'updated_nanosecond': 'updated_nanosecond_lens'
            })

            final_df = pd.merge(final_df, lens_owner_df[['ethereum_unique_id', 'new_ethereum_graph_id', 'new_ethereum_updated_nanosecond']],
                    left_on='ethereum_unique_id', right_on='ethereum_unique_id', how='left', suffixes=('', '_ethereum'))

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

            partof_ethereum = final_df[['ethereum_unique_id', 'ethereum_graph_id']].copy()
            partof_ethereum = partof_ethereum[['ethereum_unique_id', 'ethereum_graph_id']]
            # rename the columns
            partof_ethereum = partof_ethereum.rename(columns={
                'ethereum_unique_id': 'from',
                'ethereum_graph_id': 'to'
            })

            partof_lens = final_df[['lens_unique_id', 'lens_graph_id']].copy()
            partof_lens = partof_lens[['lens_unique_id', 'lens_graph_id']]
            # rename the columns
            partof_lens = partof_lens.rename(columns={
                'lens_unique_id': 'from',
                'lens_graph_id': 'to'
            })

            part_of_identities_graph_df = pd.concat([partof_lens, partof_ethereum])
            part_of_identities_graph_df = part_of_identities_graph_df.drop_duplicates(subset=['from', 'to'], keep='first')
            part_of_identities_graph_df.to_csv(part_of_identities_graph_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", part_of_identities_graph_path, part_of_identities_graph_df.shape[0])

            # Filter out rows where combine_type is "both_exist_and_same"
            ethereum_part = final_df[['combine_type', 'ethereum_unique_id', 'ethereum_graph_id', 'address', 'ethereum_updated_nanosecond']].copy()
            ethereum_part = ethereum_part[ethereum_part['combine_type'] != "both_exist_and_same"]
            ethereum_part = ethereum_part.drop_duplicates(subset=['ethereum_unique_id'], keep='first')
            ethereum_part['platform'] = 'ethereum'
            ethereum_part = ethereum_part.rename(columns={
                'ethereum_unique_id': 'unique_id',
                'ethereum_graph_id': 'graph_id',
                'address': 'identity',
                'ethereum_updated_nanosecond': 'updated_nanosecond'
            })
            ethereum_part = ethereum_part[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]

            lens_part = final_df[['combine_type', 'lens_unique_id', 'lens_graph_id', 'name', 'lens_updated_nanosecond']].copy()
            lens_part = lens_part[lens_part['combine_type'] != "both_exist_and_same"]
            lens_part = lens_part.drop_duplicates(subset=['lens_unique_id'], keep='first')
            lens_part['platform'] = 'lens'
            lens_part = lens_part.rename(columns={
                'lens_unique_id': 'unique_id',
                'lens_graph_id': 'graph_id',
                'name': 'identity',
                'lens_updated_nanosecond': 'updated_nanosecond'
            })
            lens_part = lens_part[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]

            final_graph_id_df = pd.concat([ethereum_part, lens_part], ignore_index=True)
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
        logging.info("processing lens_identity_graph end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing lens_identity_graph cost: %d", ts_delta)


    def save_graph_id(self, dump_batch_size=10000):
        graphdb_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/lens")
        if not os.path.exists(graphdb_process_dirs):
            raise FileNotFoundError(f"No directory {graphdb_process_dirs}")

        # allocation
        allocation_path = os.path.join(graphdb_process_dirs, "graph_id.csv")
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
                    if len(row[0]) > 1249:
                        continue
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
        lens_loading_job_name = "Load_Lens"
        start = time.time()
        logging.info("run loading job[%s] start at: %s", \
                lens_loading_job_name,
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
            "name": lens_loading_job_name,
            "sys.data_root": setting.TIGERGRAPH_SETTINGS["graph_data_root"],
            "dataSources": []
        }]
        response = requests.post(url=job_run_url, data=json.dumps(payload), headers=headers, timeout=60)
        if response.status_code != 200:
            error_msg = "graphdb_server run loading job[{}] failed: url={}, {} {}".format(
                job_run_url, lens_loading_job_name, response.status_code, response.reason)
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
                    job_run_url, lens_loading_job_name, res)
                logging.error(error_msg)
                raise Exception(error_msg)
            else:
                job_ids = res.get("jobIds")
                if len(job_ids) == 0:
                    error_msg = "graphdb_server run loading job[{}] failed: url={}, job_ids={} job_ids is empty".format(
                        job_run_url, lens_loading_job_name, job_ids)
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
                    lens_loading_job_name,
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("run loading job[%s] cost: %d", lens_loading_job_name, ts_delta)

    def dumps_to_graphdb(self):
        try:
            self.update_job_status("start")
            self.update_job_status("running")
            self.process_lens_identity_graph()
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

    LensGraphDB().process_lens_identity_graph()
    # LensGraphDB().save_graph_id()
    # LensGraphDB().run_loading_job()