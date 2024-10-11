#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-10-11 12:06:44
LastEditors: Zella Zhong
LastEditTime: 2024-10-12 01:07:24
FilePath: /data_process/src/jobs/clusters_graphdb_job.py
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
        return graph_id_address, updated_nanosecond_address, True


def combine_logic(row):
    graph_id_clusters = row['graph_id_clusters']
    updated_ns_clusters = row['updated_nanosecond_clusters']
    # new_eth_graph_id always exists because we generate(new or exist) before
    new_address_graph_id = row['new_address_graph_id']
    new_address_updated_ns = row['new_address_updated_nanosecond']

    if pd.notna(graph_id_clusters) and pd.notna(new_address_graph_id):
        # Case 1: Both exist
        if graph_id_clusters == new_address_graph_id:
            return graph_id_clusters, int(updated_ns_clusters), new_address_graph_id, int(new_address_updated_ns), "both_exist_and_same"
        else:
            return new_address_graph_id, int(new_address_updated_ns), new_address_graph_id, int(new_address_updated_ns), "both_exist_but_use_address_graph_id"

    elif pd.isna(graph_id_clusters) and pd.notna(new_address_graph_id):
        # Case 3: address_graph_id exists but clusters_graph_id does not exist
        return new_address_graph_id, int(new_address_updated_ns), new_address_graph_id, int(new_address_updated_ns), "only_address_exist_use_address_graph_id"

class ClustersGraphDB(object):
    def __init__(self):
        self.job_name = "clusters_graphdb_job"
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

    def process_clusters_identity_graph(self):
        clusters_import_graphs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/clusters")
        if not os.path.exists(clusters_import_graphs):
            os.makedirs(clusters_import_graphs)

        start = time.time()
        logging.info("processing clusters_identity_graph start at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start)))

        # allocation
        allocation_path = os.path.join(clusters_import_graphs, "graph_id.csv")
        # vertices
        identities_path = os.path.join(clusters_import_graphs, "Identities.csv")
        identities_graph_path = os.path.join(clusters_import_graphs, "IdentitiesGraph.csv")
        # edges
        part_of_identities_graph_path = os.path.join(clusters_import_graphs, "PartOfIdentitiesGraph.csv")
        hold_path = os.path.join(clusters_import_graphs, "Hold.csv")
        proof_path = os.path.join(clusters_import_graphs, "Proof_Forward.csv")

        read_conn = psycopg2.connect(setting.PG_DSN["read"])
        cursor = read_conn.cursor()
        try:
            clusters_profile = "clusters_profile"
            columns = ['network', 'address', 'is_verified', 'cluster_name', 'name', 'delete_time']
            select_sql = "SELECT %s FROM %s" % (",".join(columns), clusters_profile)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            clusters_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table clusters_profile row_count: %d", clusters_df.shape[0])

            clusters_df = clusters_df[clusters_df['delete_time'].isna()]  # Keep rows where delete_time is NaN
            clusters_df = clusters_df[clusters_df['is_verified'] == True]  # Keep rows where is_verified is True
            clusters_df = clusters_df[clusters_df['network'] != 'unknown']  # Exclude rows where network is 'unknown'

            address_df = clusters_df[['network', 'address']].copy()
            address_df['primary_id'] = address_df['network'] + "," + address_df['address']
            address_df['platform'] = address_df['network']
            address_df['identity'] = address_df['address']
            address_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            address_df = address_df.drop_duplicates(subset=['platform', 'identity'], keep='first')
            address_df = address_df[['primary_id', 'platform', 'identity', 'update_time']]

            cluster_name_df = clusters_df[['cluster_name']].copy()
            cluster_name_df = cluster_name_df.dropna(subset=['cluster_name'])
            cluster_name_df = cluster_name_df.drop_duplicates(subset=['cluster_name'], keep='first')
            cluster_name_df['primary_id'] = cluster_name_df['cluster_name'].apply(lambda x: f"clusters,{x}")
            cluster_name_df['platform'] = 'clusters'
            cluster_name_df['identity'] = cluster_name_df['cluster_name']
            cluster_name_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cluster_name_df = cluster_name_df[['primary_id', 'platform', 'identity', 'update_time']]

            cluster_subname_df = clusters_df[['cluster_name', 'name']].copy()
            cluster_subname_df = cluster_subname_df.dropna(subset=['cluster_name'])
            cluster_subname_df = cluster_subname_df.drop_duplicates(subset=['cluster_name', 'name'], keep='first')
            cluster_subname_df['primary_id'] = cluster_subname_df.apply(lambda x: f"clusters,{x['cluster_name']}/{x['name']}", axis=1)
            cluster_subname_df['platform'] = 'clusters'
            cluster_subname_df['identity'] = cluster_subname_df.apply(lambda x: f"{x['cluster_name']}/{x['name']}", axis=1)
            cluster_subname_df['update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cluster_subname_df = cluster_subname_df[['primary_id', 'platform', 'identity', 'update_time']]

            # Identities.csv
            identities_df = pd.concat([cluster_name_df, cluster_subname_df, address_df])
            identities_df.to_csv(identities_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", identities_path, identities_df.shape[0])

            # Hold.csv
            hold_grouped = clusters_df.groupby(['cluster_name', 'network', 'address'], as_index=False).first()
            hold_grouped = hold_grouped[hold_grouped['address'] != '0x0000000000000000000000000000000000000000']
            hold_grouped['from'] = hold_grouped.apply(lambda x: f"{x['network']},{x['address']}", axis=1)
            hold_grouped['to'] = hold_grouped.apply(lambda x: f"clusters,{x['cluster_name']}", axis=1)
            hold_grouped['source'] = "clusters"
            hold_grouped['level'] = 5
            hold_df = hold_grouped[['from', 'to', 'source', 'level']]
            hold_df.to_csv(hold_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", hold_path, hold_df.shape[0])

            # Proof_Forward.csv
            proof_grouped = clusters_df.groupby(['cluster_name', 'name'], as_index=False).first()
            proof_grouped = proof_grouped.drop_duplicates(subset=['cluster_name', 'name'], keep='first')
            proof_grouped['from'] = proof_grouped.apply(lambda x: f"clusters,{x['cluster_name']}", axis=1)
            proof_grouped['to'] = proof_grouped.apply(lambda x: f"clusters,{x['cluster_name']}/{x['name']}", axis=1)
            proof_grouped['source'] = "clusters"
            proof_grouped['level'] = 5
            proof_grouped['record_id'] = ''
            proof_df = proof_grouped[['from', 'to', 'source', 'level', 'record_id']]
            proof_df.to_csv(proof_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", proof_path, proof_df.shape[0])

            # Loading graph_id allocation table
            graph_id_table = "graph_id"
            columns = ['unique_id', 'graph_id', 'updated_nanosecond']
            platform_list = ["clusters", "aptos", "ethereum", "solana", "doge", "near", "stacks", "tron", "ton", "xrpc", "bitcoin", "cosmos", "litecoin"]
            platforms = ",".join(["'" + x + "'" for x in platform_list])
            select_sql = "SELECT %s FROM %s WHERE platform in (%s)" % (",".join(columns), graph_id_table, platforms)
            cursor.execute(select_sql)
            rows = cursor.fetchall()
            allocation_df = pd.DataFrame(rows, columns=columns)
            logging.debug("Successfully load table graph_id row_count: %d", allocation_df.shape[0])

            clusters_df['clusters_unique_id'] = clusters_df.apply(lambda x: f"clusters,{x['cluster_name']}", axis=1)
            clusters_df['address_unique_id'] = clusters_df.apply(lambda x: f"{x['network']},{x['address']}", axis=1)

            logging.debug("Start merge clusters_df row_count: %d and allocation_df row_count: %d", clusters_df.shape[0], allocation_df.shape[0])
            # generate new graph_id or use existing graph_id in allocation_df
            clusters_address_df = clusters_df[['address_unique_id']].copy()
            clusters_address_df = clusters_address_df.drop_duplicates(subset=['address_unique_id'],keep='first')
            clusters_address_df = pd.merge(clusters_address_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                                     left_on='address_unique_id', right_on='unique_id', how='left', suffixes=('', '_address'))
            clusters_address_df = clusters_address_df.rename(columns={
                'graph_id': 'graph_id_address',
                'updated_nanosecond': 'updated_nanosecond_address'
            })

            clusters_address_df[
                [
                    'new_address_graph_id',
                    'new_address_updated_nanosecond',
                    'is_exist'
            ]] = clusters_address_df.apply(generate_new_graph_id, axis=1, result_type="expand")
            logging.debug("Successfully merge clusters_address_df and allocation_df row_count: %d", clusters_address_df.shape[0])

            final_df = clusters_df[['clusters_unique_id', 'address_unique_id', 'cluster_name', 'network', 'address']]

            final_df = pd.merge(final_df, allocation_df[['unique_id', 'graph_id', 'updated_nanosecond']],
                    left_on='clusters_unique_id', right_on='unique_id', how='left', suffixes=('', '_clusters'))
            final_df = final_df.rename(columns={
                'graph_id': 'graph_id_clusters',
                'updated_nanosecond': 'updated_nanosecond_clusters'
            })
            final_df = pd.merge(final_df, clusters_address_df[['address_unique_id', 'new_address_graph_id', 'new_address_updated_nanosecond']],
                    left_on='address_unique_id', right_on='address_unique_id', how='left', suffixes=('', '_address'))

            logging.debug("Successfully merge final_df and allocation_df to final_df row_count: %d", final_df.shape[0])

            logging.debug("Start combine_logic...")
            final_df[
                [
                    'clusters_graph_id',
                    'clusters_updated_nanosecond',
                    'address_graph_id',
                    'address_updated_nanosecond',
                    'combine_type'
            ]] = final_df.apply(combine_logic, axis=1, result_type="expand")
            logging.debug("End combine_logic...")

            # select relevant columns
            final_df = final_df[['combine_type', 'address_unique_id', 'address_graph_id', 'address_updated_nanosecond',
                                'clusters_unique_id', 'clusters_graph_id', 'clusters_updated_nanosecond',
                                'cluster_name', 'network', 'address']]

            identities_graph_df = final_df[['address_graph_id', 'address_updated_nanosecond']].copy()
            # rename the columns
            identities_graph_df = identities_graph_df.rename(columns={
                'address_graph_id': 'primary_id',
                'address_updated_nanosecond': 'updated_nanosecond'
            })
            identities_graph_df = identities_graph_df.drop_duplicates(subset=['primary_id'], keep='first')
            identities_graph_df.to_csv(identities_graph_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", identities_graph_path, identities_graph_df.shape[0])

            partof_address = final_df[['address_unique_id', 'address_graph_id']].copy()
            # rename the columns
            partof_address = partof_address.rename(columns={
                'address_unique_id': 'from',
                'address_graph_id': 'to'
            })

            partof_clusters = final_df[['clusters_unique_id', 'clusters_graph_id']].copy()
            # rename the columns
            partof_clusters = partof_clusters.rename(columns={
                'clusters_unique_id': 'from',
                'clusters_graph_id': 'to'
            })

            part_of_identities_graph_df = pd.concat([partof_clusters, partof_address])
            part_of_identities_graph_df = part_of_identities_graph_df.drop_duplicates(subset=['from', 'to'], keep='first')
            part_of_identities_graph_df = part_of_identities_graph_df[['from', 'to']]
            part_of_identities_graph_df.to_csv(part_of_identities_graph_path, sep='\t', index=False)
            logging.debug("Successfully save %s row_count: %d", part_of_identities_graph_path, part_of_identities_graph_df.shape[0])

            # Filter out rows where combine_type is "both_exist_and_same"
            address_part = final_df[['combine_type', 'address_unique_id', 'address_graph_id', 'network', 'address', 'address_updated_nanosecond']].copy()
            address_part = address_part[address_part['combine_type'] != "both_exist_and_same"]
            address_part = address_part.drop_duplicates(subset=['address_unique_id'], keep='first')
            address_part = address_part.rename(columns={
                'address_unique_id': 'unique_id',
                'address_graph_id': 'graph_id',
                'network': 'platform',
                'address': 'identity',
                'address_updated_nanosecond': 'updated_nanosecond'
            })
            address_part = address_part[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]

            cluster_part = final_df[['combine_type', 'clusters_unique_id', 'clusters_graph_id', 'cluster_name', 'clusters_updated_nanosecond']].copy()
            cluster_part = cluster_part[cluster_part['combine_type'] != "both_exist_and_same"]
            cluster_part = cluster_part.drop_duplicates(subset=['clusters_unique_id'], keep='first')
            cluster_part['platform'] = 'clusters'
            cluster_part = cluster_part.rename(columns={
                'clusters_unique_id': 'unique_id',
                'clusters_graph_id': 'graph_id',
                'cluster_name': 'identity',
                'clusters_updated_nanosecond': 'updated_nanosecond'
            })
            cluster_part = cluster_part[['unique_id', 'graph_id', 'platform', 'identity', 'updated_nanosecond']]

            final_graph_id_df = pd.concat([address_part, cluster_part], ignore_index=True)
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
        logging.info("processing clusters_identity_graph end at: %s", \
                      time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("processing clusters_identity_graph cost: %d", ts_delta)

    def save_graph_id(self, dump_batch_size=10000):
        graphdb_process_dirs = os.path.join(setting.Settings["datapath"], "tigergraph/import_graphs/clusters")
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
                        "filename": "/home/tigergraph/shared_data/import_graphs/clusters/Identities.csv"
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
        clusters_loading_job_name = "Load_Clusters"
        start = time.time()
        logging.info("run loading job[%s] start at: %s", \
                clusters_loading_job_name,
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
            "name": clusters_loading_job_name,
            "sys.data_root": setting.TIGERGRAPH_SETTINGS["graph_data_root"],
            "dataSources": []
        }]
        response = requests.post(url=job_run_url, data=json.dumps(payload), headers=headers, timeout=60)
        if response.status_code != 200:
            error_msg = "graphdb_server run loading job[{}] failed: url={}, {} {}".format(
                job_run_url, clusters_loading_job_name, response.status_code, response.reason)
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
                    job_run_url, clusters_loading_job_name, res)
                logging.error(error_msg)
                raise Exception(error_msg)
            else:
                job_ids = res.get("jobIds")
                if len(job_ids) == 0:
                    error_msg = "graphdb_server run loading job[{}] failed: url={}, job_ids={} job_ids is empty".format(
                        job_run_url, clusters_loading_job_name, job_ids)
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
                    clusters_loading_job_name,
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end)))
        logging.info("run loading job[%s] cost: %d", clusters_loading_job_name, ts_delta)

    def dumps_to_graphdb(self):
        try:
            self.update_job_status("start")
            self.update_job_status("running")
            self.process_clusters_identity_graph()
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

    # ClustersProcess().get_latest_timestamp()

    ClustersGraphDB().process_clusters_identity_graph()