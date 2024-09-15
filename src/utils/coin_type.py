#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Author: Zella Zhong
Date: 2024-09-15 01:05:47
LastEditors: Zella Zhong
LastEditTime: 2024-09-15 03:17:48
FilePath: /data_process/src/utils/coin_type.py
Description: 
'''
import base58
from bitcoinlib.encoding import pubkeyhash_to_addr_bech32
from eth_utils import is_checksum_address, to_checksum_address
import bech32
import crc32c
import hashlib
from hashlib import sha256


evmCoinTypeToNameMap = {
    "60": {"name": "eth", "full_name": "Ethereum", "method": "checksummed-hex"},
    "61": {"name": "etcLegacy", "full_name": "[LEGACY] Ethereum Classic", "method": "checksummed-hex"},
    "137": {"name": "rbtc", "full_name": "RSK", "method": "checksummed-hex"},
    "818": {"name": "vet", "full_name": "VeChain", "method": "checksummed-hex"},
    "2147483658": {"name": "op", "full_name": "Optimism", "method": "checksummed-hex"},
    "2147483673": {"name": "cro", "full_name": "Cronos", "method": "checksummed-hex"},
    "2147483704": {"name": "bsc", "full_name": "BNB Smart Chain", "method": "checksummed-hex"},
    "2147483708": {"name": "go", "full_name": "GoChain", "method": "checksummed-hex"},
    "2147483709": {"name": "etc", "full_name": "Ethereum Classic", "method": "checksummed-hex"},
    "2147483736": {"name": "tomo", "full_name": "TomoChain", "method": "checksummed-hex"},
    "2147483747": {"name": "poa", "full_name": "POA", "method": "checksummed-hex"},
    "2147483748": {"name": "gno", "full_name": "Gnosis", "method": "checksummed-hex"},
    "2147483756": {"name": "tt", "full_name": "ThunderCore", "method": "checksummed-hex"},
    "2147483785": {"name": "matic", "full_name": "Polygon", "method": "checksummed-hex"},
    "2147483817": {"name": "manta", "full_name": "Manta Pacific", "method": "checksummed-hex"},
    "2147483894": {"name": "ewt", "full_name": "Energy Web", "method": "checksummed-hex"},
    "2147483898": {"name": "ftm", "full_name": "Fantom Opera", "method": "checksummed-hex"},
    "2147483936": {"name": "boba", "full_name": "Boba", "method": "checksummed-hex"},
    "2147483972": {"name": "zksync", "full_name": "zkSync", "method": "checksummed-hex"},
    "2147484009": {"name": "theta", "full_name": "Theta", "method": "checksummed-hex"},
    "2147484468": {"name": "clo", "full_name": "Callisto", "method": "checksummed-hex"},
    "2147484736": {"name": "metis", "full_name": "Metis", "method": "checksummed-hex"},
    "2147488648": {"name": "mantle", "full_name": "Mantle", "method": "checksummed-hex"},
    "2147492101": {"name": "base", "full_name": "Base", "method": "checksummed-hex"},
    "2147523445": {"name": "nrg", "full_name": "Energi", "method": "checksummed-hex"},
    "2147525809": {"name": "arb1", "full_name": "Arbitrum One", "method": "checksummed-hex"},
    "2147525868": {"name": "celo", "full_name": "Celo", "method": "checksummed-hex"},
    "2147526762": {"name": "avaxc", "full_name": "Avalanche C-Chain", "method": "checksummed-hex"},
    "2147542792": {"name": "linea", "full_name": "Linea", "method": "checksummed-hex"},
    "2148018000": {"name": "scr", "full_name": "Scroll", "method": "checksummed-hex"},
    "2155261425": {"name": "zora", "full_name": "Zora", "method": "checksummed-hex"}
}

nonEvmCoinTypeToNameMap = {
    "0": {"name": "btc", "full_name": "Bitcoin", "method": "base58check_P2PKH_P2SH_bech32"},
    "2": {"name": "ltc", "full_name": "Litecoin", "method": "base58check_P2PKH_P2SH_bech32"},
    "3": {"name": "doge", "full_name": "Dogecoin", "method": "base58check_P2PKH_P2SH"}, #?
    "4": {"name": "rdd", "full_name": "Reddcoin", "method": "base58check_P2PKH_P2SH"},
    "5": {"name": "dash", "full_name": "Dash", "method": "base58check_P2PKH_P2SH"},
    "6": {"name": "ppc", "full_name": "Peercoin", "method": "base58check_P2PKH_P2SH"},
    "7": {"name": "nmc", "full_name": "Namecoin", "method": "base58check"},
    "14": {"name": "via", "full_name": "Viacoin", "method": "base58check_P2PKH_P2SH"},
    "20": {"name": "dgb", "full_name": "DigiByte", "method": "base58check_P2PKH_P2SH_bech32"},
    "22": {"name": "mona", "full_name": "Monacoin", "method": "base58check_P2PKH_P2SH_bech32"},
    "55": {"name": "aib", "full_name": "AIB", "method": "base58check_P2PKH_P2SH"},
    "57": {"name": "vsys", "full_name": "Syscoin", "method": "base58check_P2PKH_P2SH_bech32"},
    "145": {"name": "bch", "full_name": "Bitcoin Cash", "method": "base58check"},
    "236": {"name": "bsv", "full_name": "BitcoinSV", "method": "base58check"},
    "192": {"name": "lcc", "full_name": "LitecoinCash", "method": "base58check_P2PKH_P2SH_bech32"},
    "77": {"name": "xvg", "full_name": "Verge", "method": "base58check_P2PKH_P2SH"},
    "105": {"name": "strat", "full_name": "Stratis", "method": "base58check_P2PKH_P2SH"},
    "111": {"name": "ark", "full_name": "ARK", "method": "base58check"},
    "121": {"name": "zen", "full_name": "Zencash", "method": "base58check"},
    "133": {"name": "zec", "full_name": "Zcash", "method": "base58check_P2PKH_P2SH_bech32"},
    "136": {"name": "firo", "full_name": "Firo", "method": "base58check_P2PKH_P2SH"},
    "144": {"name": "xrp", "full_name": "Ripple", "method": "base58check"},
    "156": {"name": "btg", "full_name": "Bitcoin Gold", "method": "base58check_P2PKH_P2SH_bech32"},
    "175": {"name": "rvn", "full_name": "Ravencoin", "method": "base58check_P2PKH_P2SH"},
    "301": {"name": "divi", "full_name": "Divi Project", "method": "base58check_P2PKH_P2SH"},
    "888": {"name": "neo", "full_name": "NEO", "method": "base58check"},
    "489": {"name": "cca", "full_name": "Counos", "method": "base58check_P2PKH_P2SH"},
    "571": {"name": "ccxx", "full_name": "Counos X", "method": "base58check_P2PKH_P2SH_bech32"},
    "576": {"name": "bps", "full_name": "BitcoinPoS", "method": "base58check_P2PKH_P2SH_bech32"},
    "568": {"name": "lrg", "full_name": "Large Coin", "method": "base58check_P2PKH_P2SH"},
    "999": {"name": "bcd", "full_name": "Bitcoin Diamond", "method": "base58check_P2PKH_P2SH_bech32"},
    "1729": {"name": "xtz", "full_name": "Tezos", "method": "base58check"},
    "19167": {"name": "flux", "full_name": "Flux", "method": "base58check_P2PKH_P2SH_bech32"},
    "99999": {"name": "wicc", "full_name": "Waykichain", "method": "base58check_P2PKH_P2SH"},

    "42": {"name": "dcr", "full_name": "Decred", "method": "base58"},
    "128": {"name": "xmr", "full_name": "Monero", "method": "base58"},
    "397": {"name": "near", "full_name": "NEAR Protocol", "method": "base58"},
    "501": {"name": "sol", "full_name": "Solana", "method": "base58"},
    "535": {"name": "xhv", "full_name": "Haven Protocol", "method": "base58"},
    "825": {"name": "hive", "full_name": "Hive", "method": "base58"},


    "118": {"name": "atom", "full_name": "Atom", "method": "bech32"},
    "304": {"name": "iotx", "full_name": "IoTeX", "method": "bech32"},
    "330": {"name": "luna", "full_name": "Terra", "method": "bech32"},
    "391": {"name": "iota", "full_name": "IOTA", "method": "bech32"},
    "714": {"name": "bnb", "full_name": "BNB", "method": "bech32"},
    "1023": {"name": "one", "full_name": "HARMONY-ONE", "method": "bech32"},

    "3030": {"name": "hbar", "full_name": "Hedera HBAR", "method": "custom"},
    "360": {"name": "vsys", "full_name": "V Systems", "method": "custom"},
    "134": {"name": "lsk", "full_name": "Lisk", "method": "custom"},
    "135": {"name": "steem", "full_name": "Steem", "method": "custom"},
    "354": {"name": "dot", "full_name": "Polkadot", "method": "ss58"},

    "1815": {"name": "ada", "full_name": "Cardano", "method": "crc32c"},
    "539": {"name": "flow", "full_name": "Flow", "method": "hex"}		
}

def decode_cointype_address(coin_type, hex_data):
    if coin_type in evmCoinTypeToNameMap:
        name = evmCoinTypeToNameMap[coin_type]["name"]
        return name, checksummed_hex(hex_data)
    elif coin_type in nonEvmCoinTypeToNameMap:
        decode_type = nonEvmCoinTypeToNameMap[coin_type]["method"]
        name = nonEvmCoinTypeToNameMap[coin_type]["name"]
        if decode_type == "base58check_P2PKH_P2SH_bech32":
            if name == "btc":
                return name, base58check_P2PKH_P2SH_bech32(hex_data, 'bc')
            return name, base58check_P2PKH_P2SH_bech32(hex_data, name)
        elif decode_type == "base58check_P2PKH_P2SH":
            return name, pubkeyhash_to_base58check_P2PKH_P2SH(hex_data)
        elif decode_type == "base58":
            return name, decode_base58_nocheck(hex_data)
        elif decode_type == "bech32":
            return name, decode_bech32_address(hex_data)
        elif decode_type == "crc32c":
            return name, decode_crc32c_address(hex_data)
    return coin_type, hex_data

def checksummed_hex(hex_data):
    try:
        address = to_checksum_address(hex_data)
        return str(address).lower()
    except Exception as e:
        return hex_data


def base58check_P2PKH_P2SH_bech32(hex_data, prefix):
    '''Base58Check: Decoding (used by Bitcoin, Litecoin, etc.) '''
    try:
        hex_string = hex_data[2:]
        hex_bytes = bytes.fromhex(hex_string)
        bech32_address = pubkeyhash_to_addr_bech32(hex_bytes, prefix)
        return bech32_address
    except Exception as e:
        return hex_data

def pubkeyhash_to_base58check_P2PKH_P2SH(hex_data):
    try:
        # The hex data need (after removing the script sig prefix and suffix)
        pubkey_hash = bytes.fromhex(hex_data[8:-4])
        version_byte = b'\x1e'  # Dogecoin version for P2PKH addresses (0x1E)
        versioned_payload = version_byte + pubkey_hash

        # Perform double SHA-256 for checksum
        checksum = hashlib.sha256(hashlib.sha256(versioned_payload).digest()).digest()[:4]

        # Append checksum to the payload
        full_payload = versioned_payload + checksum

        # Step 4: Encode in Base58
        doge_address = base58.b58encode(full_payload)
        return doge_address.decode('utf-8')
    except Exception as e:
        return hex_data

def decode_base58_nocheck(hex_data):
    '''Base58: Decoding (no checksum, for Solana and NEAR)'''
    try:
        hex_string = hex_data[2:]
        hex_bytes = bytes.fromhex(hex_string)
        decoded = base58.b58encode(hex_bytes).decode('utf-8')
        return decoded
    except Exception as e:
        return hex_data


def decode_bech32_address(hex_data):
    '''Bech32 Decoding (used by BNB etc.)'''
    try:
        decoded_hrp, decoded_data = bech32.bech32_decode(hex_data)
        return bech32.convertbits(decoded_data, 5, 8, False)
    except Exception as e:
        return hex_data

def decode_crc32c_address(hex_data):
    '''CRC32 Checksum'''
    try:
        crc = crc32c.crc32c(hex_data.encode('utf-8'))
        return crc
    except Exception as e:
        return hex_data


if __name__ == '__main__':
    input = {
        "0": "0x00149b976cb07509043bd44a2994f618ccaa3887d5aa",
        "2": "0x00148fb3f7596b3a8dff02ae39c12848d3e49f0f9344",
        "60": "0x225f137127d9067788314bc7fcc1f36746a3c3b5",
        "501": "0xf021828347f287c7227667626c6cd3bcc5b45423234b5ed74ac4b304568dc4d7",
        "2147483658": "0x225f137127d9067788314bc7fcc1f36746a3c3b5",
        "2147483785": "0x225f137127d9067788314bc7fcc1f36746a3c3b5",
        "2147492101": "0x225f137127d9067788314bc7fcc1f36746a3c3b5",
        "2147525809": "0x225f137127d9067788314bc7fcc1f36746a3c3b5"
    }

    {"0": "0x00143cb159f787b8c21c10025b54220b53a546be58c0", "2": "0x001452fbf585cf63fbe1536aac615a04498239b15c70", "3": "0x76a914677e753d01b8f50f521676199d14145bdbe9860c88ac", "60": "0xac4c0ec6d9c7a5449401264332be4549d0732ffc", "2147483785": "0xac4c0ec6d9c7a5449401264332be4549d0732ffc"}

    print(decode_cointype_address("60", "0xac4c0ec6d9c7a5449401264332be4549d0732ffc"))
    # print(decode_base58_nocheck("0xf021828347f287c7227667626c6cd3bcc5b45423234b5ed74ac4b304568dc4d7"))


    # How about Doge 0x76a914677e753d01b8f50f521676199d14145bdbe9860c88ac convert to DEaKdUE52GRQhtyTWwy7uTZ6Q1o7H9qWvX 