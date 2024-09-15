#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-12 19:13:38
LastEditors: Zella Zhong
LastEditTime: 2024-09-12 19:20:55
FilePath: /data_process/src/utils/domain.py
Description: 
'''
import copy
from eth_utils import keccak, encode_hex

def bytes32_to_uint256(value):
    '''
    description: bytes32_to_uint256
    param: value bytes32 
    return: id uint256(str)
    '''
    # Remove the '0x' prefix if it exists and convert the hex string to an integer
    trim_value = value.lstrip('0x')
    # Convert the bytes32 address back to a uint256 integer
    return str(int(trim_value, 16))

def compute_namehash(name):
    node = b'\x00' * 32  # 32 bytes of zeroes (initial nodehash for the root)
    parent_node = b'\x00' * 32
    self_label = ""
    self_node = ""
    items = name.split('.')
    subname = items[0]
    for item in reversed(items):
        label_hash = keccak(item.encode('utf-8'))
        subname = item
        parent_node = copy.deepcopy(node)
        node = keccak(node + label_hash)  # keccak256 of node + label_hash
        self_node = node

    label_hash = keccak(subname.encode('utf-8'))
    self_label = encode_hex(label_hash)
    erc721_token_id = bytes32_to_uint256(self_label)
    erc1155_token_id = bytes32_to_uint256(encode_hex(self_node))
    return encode_hex(parent_node), self_label, erc721_token_id, erc1155_token_id, encode_hex(self_node)

