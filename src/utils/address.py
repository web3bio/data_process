#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-12 19:13:33
LastEditors: Zella Zhong
LastEditTime: 2024-09-15 01:05:38
FilePath: /data_process/src/utils/address.py
Description: 
'''
import base58
from eth_utils import to_checksum_address

def hexstr_to_solana_address(hex_data):
    # Strip the "0x" prefix if present
    if hex_data.startswith("0x"):
        hex_data = hex_data[2:]

    # Convert the hex string to bytes
    raw_bytes = bytes.fromhex(hex_data)

    # Base58 encode the raw bytes to get the Solana address
    solana_address = base58.b58encode(raw_bytes).decode('utf-8')

    return solana_address


def bytea_to_eth_checksum_address(bytea_value):
    ''' Function to convert BYTEA (byte string) to Ethereum address'''
    # Convert bytea_value (byte string) to hexadecimal, then to checksum address
    return to_checksum_address("0x" + bytea_value.hex())

def hexstr_to_eth_checksum_address(hex_data):
    ''' Function to convert hex_str to Ethereum address'''
    if hex_data.startswith("0x"):
        hex_data = hex_data[2:]
    return to_checksum_address("0x" + hex_data)

def bytea_to_hex_address(bytea_value):
    '''# Function to convert BYTEA to lowercase Ethereum address'''
    return "0x" + bytea_value.hex()


