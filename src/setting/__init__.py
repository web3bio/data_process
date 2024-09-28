#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-12 19:06:43
LastEditors: Zella Zhong
LastEditTime: 2024-09-28 21:55:28
FilePath: /data_process/src/setting/__init__.py
Description: 
'''
import sys
import logging
import os
import toml

Settings = {
    "env": "development",
    "datapath": "./data",
}

TIGERGRAPH_SETTINGS = {
    "host": "",
    "inner_port": 0,
    "restpp": 0,
    "username": "",
    "password": "",
    "graph_data_root": "",
    "social_graph_name": "",
    "social_graph_secret": "",
    "social_graph_token": "",
}

PG_DSN = {
    "read": "",
    "write": "",
}

CHAIN_RPC = {
    "ethereum": {},
    "gnosis": {},
    "base": {},
}

CHAINBASE = {
    "api": "",
    "api_key": "",
}

NEYNAR = {
    "api": "",
    "api_key": "",
}

LENS = {
    "api": "",
    "api_key": "",
}


def load_dsn(config_file):
    """
    @description: load pg dsn
    @params: config_file
    @return dsn_settings
    """
    try:
        config = toml.load(config_file)
        pg_dsn_settings = {
            "read": config["pg_dsn"]["read"],
            "write": config["pg_dsn"]["write"],
        }
        return pg_dsn_settings
    except Exception as ex:
        logging.exception(ex)

def load_settings(env="development"):
    """
    @description: load configurations from file
    """
    global Settings
    global PG_DSN
    global TIGERGRAPH_SETTINGS
    global CHAIN_RPC
    global CHAINBASE
    global NEYNAR
    global LENS

    config_file = "/app/config/production.toml"
    if env is not None:
        if env not in ["development", "test", "production"]:
            raise ValueError("Unknown environment")
        config_file = os.getenv("CONFIG_FILE")

    config = toml.load(config_file)
    Settings["env"] = env
    Settings["datapath"] = os.path.join(config["server"]["work_path"], "data")
    PG_DSN = load_dsn(config_file)
    CHAIN_RPC["ethereum"] = {
        "rpc": config["chain_rpc"]["ethereum"]["rpc"],
        "api": config["chain_rpc"]["ethereum"]["api"],
        "api_key": config["chain_rpc"]["ethereum"]["api_key"],
    }
    CHAIN_RPC["base"] = {
        "rpc": config["chain_rpc"]["base"]["rpc"],
        "api": config["chain_rpc"]["base"]["api"],
        "api_key": config["chain_rpc"]["base"]["api_key"],
    }
    CHAIN_RPC["gnosis"] = {
        "rpc": config["chain_rpc"]["gnosis"]["rpc"],
        "api": config["chain_rpc"]["gnosis"]["api"],
        "api_key": config["chain_rpc"]["gnosis"]["api_key"],
    }
    CHAINBASE = {
        "api": config["chainbase"]["api"],
        "api_key": config["chainbase"]["api_key"],
    }
    NEYNAR = {
        "api": config["neynar"]["api"],
        "api_key": config["neynar"]["api_key"],
    }
    LENS = {
        "api": config["lens"]["api"],
        "api_key": config["lens"]["api_key"],
    }
    TIGERGRAPH_SETTINGS = {
        "host": config["tdb"]["host"],
        "inner_port": config["tdb"]["inner_port"],
        "restpp": config["tdb"]["restpp"],
        "username": config["tdb"]["username"],
        "password": config["tdb"]["password"],
        "graph_data_root": config["tdb"]["graph_data_root"],
        "social_graph_name": config["tdb"]["social_graph_name"],
        "social_graph_secret": config["tdb"]["social_graph_secret"],
        "social_graph_token": config["tdb"]["social_graph_token"],
    }
    return config

# Preload configuration
load_settings(env=os.getenv("ENVIRONMENT"))