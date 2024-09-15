#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-12 19:23:53
LastEditors: Zella Zhong
LastEditTime: 2024-09-15 06:45:34
FilePath: /data_process/src/utils/contenthash.py
Description: 
'''
import base64
from multiformats import CID
from eth_utils import encode_hex

def hex_to_ipns(hex_data):
    # Step 1: Remove the "0x" prefix if it exists
    if hex_data.startswith("0x"):
        hex_data = hex_data[2:]

    # Step 2: Convert the hex string to bytes
    data_bytes = bytes.fromhex(hex_data)

    # Step 3: Use the public key bytes starting from byte 4 (assuming first 4 bytes are metadata)
    public_key_bytes = data_bytes[4:]

    cid = CID("base36", 1, "libp2p-key", public_key_bytes)

    # Step 6: Convert the CID to base36 encoding for the IPNS URL
    ipns_address = f"ipns://{cid.encode()}"

    return ipns_address


import multiformats

# Mapping of known multicodec prefixes to their names
code_to_name = {
    0xe3: "ipfs",
    0xe5: "ipns",
    0xe4: "swarm",
    0x01bc: "onion",
    0x01bd: "onion3",
    0xb19910: "skynet",
    0xb29910: "arweave",
}

def decode_contenthash(hex_data):
    if hex_data.startswith("0x"):
        hex_data = hex_data[2:]
    
    format_type = "unknown"
    if hex_data.startswith("e301"):
        format_type = "ipfs"
    if hex_data.startswith("e5"):
        format_type = "ipns"
    if hex_data.startswith("e4"):
        format_type = "swarm"
    if hex_data.startswith("bc03"):
        format_type = "onion"
    if hex_data.startswith("bd03"):
        format_type = "onion3"
    if hex_data.startswith("90b2c6"):
        format_type = "skynet"
    if hex_data.startswith("90b2ca"):
        format_type = "arweave"

    if format_type == "ipfs":
        data_bytes = bytes.fromhex(hex_data[4:])
        cid = CID.decode(data_bytes)
        cid_v1 = cid.encode("base32")
        return f"ipfs://{cid_v1}"
    elif format_type == "ipns":
        data_bytes = bytes.fromhex(hex_data)
        public_key_bytes = data_bytes[4:]
        cid = CID("base36", 1, "libp2p-key", public_key_bytes)
        decoded_cid = cid.encode()
        return f"ipns://{decoded_cid}"
    elif format_type == "swarm":
        data_bytes = bytes.fromhex(hex_data)
        return f"bzz://{data_bytes[7:].hex()}"
    elif format_type == "onion":
        data_bytes = bytes.fromhex(hex_data[2:])
        return f"onion://{data_bytes.decode('utf-8')}"
    elif format_type == "onion3":
        data_bytes = bytes.fromhex(hex_data[2:])
        return f"onion3://{data_bytes.decode('utf-8')}"
    elif format_type == "skynet":
        data_bytes = bytes.fromhex(hex_data[4:])
        base64_url_encoded = base64.urlsafe_b64encode(data_bytes[4:]).decode('utf-8').rstrip('=')
        return f"sia://{base64_url_encoded}"
    elif format_type == "arweave":
        data_bytes = bytes.fromhex(hex_data)
        base64_url_encoded = base64.urlsafe_b64encode(data_bytes[4:]).decode('utf-8').rstrip('=')
        return f"ar://{base64_url_encoded}"


if __name__ == "__main__":
    # Example usage
    ipfs_contentHash = "0xe301015512205822b241e505ef8acd4f11512cb8fb480b20b156a028b3d53acdcf3e01df1ea3"
    print(f"Decoded IPFS: {decode_contenthash(ipfs_contentHash)}")

    ipns_contentHash = "0xe5010172002408011220b4bc9d00d6d7da0c95d1d4f85517aa0d0952e03befb9088bd19574e154b9f211"
    print(f"Decoded IPNS: {decode_contenthash(ipns_contentHash)}")

    swarm_contentHash =  "0xe40101fa011b203b0568cb639544afec72e9574b6af7abea30cc2f97bd4ed2edbd4eb5fc5cded2"
    print(f"Decoded Bzz: {decode_contenthash(swarm_contentHash)}")

    onion_contentHash = "0xbc0332646b646a776b646f666b646a65646f"
    print(f"Decoded Onion: {decode_contenthash(onion_contentHash)}")
    # onion://2dkdjwkdofkdjedo

    onion3_contentHash = "0xbd03747769747465723365347469786c347879616a74727a6f36327a6735767a746d6a757269636c6a64703263356b73686a75346176796f6964"
    print(f"Decoded Onion3: {decode_contenthash(onion3_contentHash)}")
    # onion3://twitter3e4tixl4xyajtrzo62zg5vztmjuricljdp2c5kshju4avyoid

    skynet_contentHash = "0x90b2c6050100f0986dae225f3701dd24f2a5d6d7b03ae8a3566fbd35d9e63dac6709d6edfd8a"
    print(f"Decoded Skynet: {decode_contenthash(skynet_contentHash)}")
    # sia://AQDwmG2uIl83Ad0k8qXW17A66KNWb7012eY9rGcJ1u39ig

    arweave_contentHash = "0x90b2ca0523e905db226388a7375d5c16a14105c3636b5155ab5ea8feac22ad3289c35685"
    print(f"Decoded Arweave: {decode_contenthash(arweave_contentHash)}")
    # ar://I-kF2yJjiKc3XVwWoUEFw2NrUVWrXqj-rCKtMonDVoU
