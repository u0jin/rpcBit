import re
import json
import requests
import pandas as pd
import os
import time
import random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import sys
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

REPORTED_HACKER_ADDRESSES_FOLDER = '/Users/ujin/Desktop/Blockchain/hackerData/DBdata'
REPORTED_HACKER_TYPE_FOLDER = '/Users/ujin/Desktop/Blockchain/hackerData/Reportdata'
DATA_FILE_PATH = '/Users/yujin/Desktop/Blockchain/hacker_data.csv'

def connect_to_node(rpc_user, rpc_password, rpc_host, rpc_port):
    rpc_url = f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}"
    return AuthServiceProxy(rpc_url)

def load_hackers_data(file_path):
    hackers_data = []

    if not os.path.exists(file_path):
        print("File not found.")
        return hackers_data

    with open(file_path, 'r') as file:
        for line in file.readlines():
            hacker_address, report_type = line.strip().split(',')
            hackers_data.append({'hacker_address': hacker_address, 'report_type': report_type})

    return hackers_data

def check_repeated_address(transactions, threshold=1):
    address_counts = {}
    for transaction in transactions:
        receiving_wallet = transaction['receiving_wallet']
        if receiving_wallet in address_counts:
            address_counts[receiving_wallet] += 1
        else:
            address_counts[receiving_wallet] = 1

    for address, count in address_counts.items():
        if count > threshold:
            return address

    return None

def write_transaction_to_file(transaction_data, output_filename):
    if not os.path.exists(output_filename):
        with open(output_filename, 'w') as f:
            f.write(','.join(transaction_data.keys()) + '\n')

    with open(output_filename, 'a') as f:
        f.write(','.join(str(value) for value in transaction_data.values()) + '\n')

def get_transactions(hacker_address, report_type, rpc_connection):
    hacker_transactions = []
    output_filename = f"{report_type}.Transaction_{hacker_address}.csv"

    try:
        tx_ids = rpc_connection.getaddresstxids({"addresses": [hacker_address]})

        for tx_id in tx_ids:
            tx = rpc_connection.getrawtransaction(tx_id, True)
            for output in tx['vout']:
                if 'addresses' in output['scriptPubKey']:
                    receiving_wallet = output['scriptPubKey']['addresses'][0]
                    transaction_amount = output['value']

                    input_addresses = []
                    input_values = []
                    for tx_input in tx['vin']:
                        prev_tx = rpc_connection.getrawtransaction(tx_input['txid'], True)
                        prev_output = prev_tx['vout'][tx_input['vout']]
                        input_address = prev_output['scriptPubKey']['addresses'][0]
                        input_value = prev_output['value']
                        input_addresses.append(input_address)
                        input_values.append(input_value)

                    transaction_data = {
                        'tx_hash': tx['txid'],
                        'sending_wallet': hacker_address,
                        'receiving_wallet': receiving_wallet,
                        'transaction_amount': transaction_amount,
                        'coin_type': 'BTC',
                        'date_sent': datetime.fromtimestamp(tx['time']).strftime('%Y-%m-%d'),
                        'time_sent': datetime.fromtimestamp(tx['time']).strftime('%H:%M:%S'),
                        'sending_wallet_source': 'Hacker DB',
                        'receiving_wallet_source': 'Full-node',
                        'input_addresses': input_addresses,
                        'output_addresses': [out['scriptPubKey']['addresses'][0] for out in tx['vout'] if 'addresses' in out['scriptPubKey']],
                        'total_input_value': sum(input_values),
                        'total_output_value': sum([out['value'] for out in tx['vout']]),
                        'fee': tx['fee'] if 'fee' in tx else None
                    }
                    hacker_transactions.append(transaction_data)
                    write_transaction_to_file(transaction_data, output_filename)

    except JSONRPCException as e:
        print(f"Error: {e}")

    return hacker_transactions


def get_next_hacker_address(transactions):
    if transactions:
        last_transaction = transactions[-1]
        return last_transaction['receiving_wallet']
    return None

def process_hacker_data(hacker_data, rpc_connection):
    hacker_address = hacker_data['hacker_address']
    report_type = hacker_data['report_type']

    output_filename = f"{report_type}.Transaction_{hacker_address}.csv"
    hacker_transactions = get_transactions(hacker_address, report_type, rpc_connection)
    
    next_hacker_address = get_next_hacker_address(hacker_transactions)
    if next_hacker_address:
        output_filename = f"{report_type}.Transaction_{hacker_address}_trace{next_hacker_address}.csv"
        hacker_transactions = get_transactions(next_hacker_address, report_type, rpc_connection)

def main():
    hackers_data = load_hackers_data(DATA_FILE_PATH)
    rpc_user = "your_rpc_user"
    rpc_password = "your_rpc_password"
    rpc_host = "your_rpc_host"
    rpc_port = "your_rpc_port"
    
    rpc_connection = connect_to_node(rpc_user, rpc_password, rpc_host, rpc_port)

    for hacker_data in hackers_data:
        process_hacker_data(hacker_data, rpc_connection)

if __name__ == '__main__':
    main()
