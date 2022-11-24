from datetime import datetime
import json
from math import ceil
from multiprocessing import Pool
import os
from pathlib import Path
from random import choices, sample
import string
from sys import getsizeof
import time
from typing import Callable, Union

import pandas as pd
import numpy as np

from dotenv import load_dotenv
from flask import Flask, request
from flask_cors import CORS

import requests

####################################################################################################################################################################################################################################

load_dotenv()

app = Flask(__name__)
CORS(app)

FIREBASE_URL = os.environ.get('FIREBASE_URL')
FIREBASE_DEFAULT_DIR_PERMISSION = os.environ.get(
    'FIREBASE_DEFAULT_DIR_PERMISSION')
FIREBASE_DEFAULT_FILE_PERMISSION = os.environ.get(
    'FIREBASE_DEFAULT_FILE_PERMISSION')

REPLICATION_FACTOR = int(os.environ.get('REPLICATION_FACTOR'))
NUMBER_OF_DATANODES = int(os.environ.get('NUMBER_OF_DATANODES'))
# MAX_PARTITION_SIZE = int(os.environ.get('FIREBASE_MAX_PARTITION_SIZE'))
# MAX_PARTITION_SIZE = 32768
MAX_PARTITION_SIZE = 1024

JSON = ".json"
DATANODE = "datanode/"
METADATA = "metadata/"
NAMENODE = "namenode/"
INODE_DIRECTORY_SECTION = "inode_directory_section/"
INODE = "inodes/"


def is_valid_path(nodes: list) -> tuple[bool, list]:
    '''
    Helper function to check if given path exists in the EDFS
    Arguments:
        nodes - List of nodes in the path split on /
    Returns:
        bool - whether the current path is valid ot not
        order - the inode number in order of the path
    '''

    root_name = "1_\\"
    root_url = FIREBASE_URL + NAMENODE + INODE + root_name + JSON
    root = requests.get(root_url)
    root = root.json()

    inode_directory_section_url = FIREBASE_URL + NAMENODE + \
        INODE_DIRECTORY_SECTION + str(root['inode']) + JSON
    inode_directory_section = requests.get(inode_directory_section_url)
    inode_directory_section = inode_directory_section.json()

    # check every directory if it exists and get its inode, break when a directory doesn't exist
    order = [root['inode']]
    curr_parent = root['inode']
    curr_parent_hierarchy = inode_directory_section

    for node in nodes:
        url = FIREBASE_URL + NAMENODE + INODE + JSON + \
            '?orderBy="name"&equalTo="' + node + '"'
        r = requests.get(url)
        if r.status_code != 200 or r.json() == None or len(list(r.json().values())) == 0:
            return False, []
        curr_inode = list(r.json().values())[0]
        if str(curr_inode['inode']) not in curr_parent_hierarchy:
            return False, []
        else:
            curr_parent = curr_inode['inode']
            curr_parent_hierarchy = curr_parent_hierarchy[str(
                curr_inode['inode'])]
            order.append(curr_inode['inode'])

    return True, order


@app.route('/mkdir', methods=['GET'])
def mkdir() -> tuple[str, int]:
    '''
    This function creates a new directory in the EDFS. Returns error if the directory already exists.
    Arguments:
        path: Path of the new directory in the EDFS
    '''
    path = request.args.get('path')
    nodes = list(filter(None, path.split("/")))

    answer, order = is_valid_path(nodes)
    if answer:
        return f"mkdir: {path}: Directory exists", 400

    answer, order = is_valid_path(nodes[:-1])
    if not answer:
        return f"mkdir: {path}: No such file or directory.", 400

    last_dir = nodes[-1]  # create this directory

    # generate a new inode_num and key
    url = FIREBASE_URL + NAMENODE + INODE + JSON
    r = requests.get(url)
    existing_inode_nums = list(r.json().values())
    existing_inode_nums = list(map(lambda x: x['inode'], existing_inode_nums))

    inode_num = max(existing_inode_nums) + 1
    inode_name = str(inode_num) + '_' + last_dir.replace('.', '_')

    curr_inode = {}
    current_timestamp = int(time.time())
    curr_inode['atime'] = current_timestamp
    curr_inode['ctime'] = current_timestamp
    curr_inode['inode'] = inode_num
    curr_inode['mtime'] = current_timestamp
    curr_inode['name'] = last_dir
    curr_inode['permission'] = FIREBASE_DEFAULT_DIR_PERMISSION
    curr_inode['type'] = "DIRECTORY"

    url = FIREBASE_URL + NAMENODE + INODE + inode_name + JSON
    r = requests.put(url, data=json.dumps(curr_inode))

    url = FIREBASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + \
        "/".join(list(map(lambda x: str(x), order))) + JSON
    r = requests.get(url)
    dir_val = r.json()
    dir_val['empty'] = False
    dir_val[inode_num] = {'empty': True}
    r = requests.put(url, data=json.dumps(dir_val))

    return f"Created {path}", 200


@app.route('/ls', methods=['GET'])
def ls() -> tuple[str, int]:
    '''
    This function lists the content of the given directory in the EDFS. Returns error if the directory doesn't exists.
    Arguments:
        path: Path of the directory in the EDFS
    '''
    path = request.args.get('path')
    nodes = list(filter(None, path.split("/")))

    answer, order = is_valid_path(nodes)
    if not answer:
        return f"ls: {path}: No such file or directory", 400

    url = FIREBASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + \
        "/".join(list(map(lambda x: str(x), order))) + JSON
    r = requests.get(url)
    dir_inodes = list(r.json().keys())
    dir_inodes.remove('empty')
    dir_inodes = list(map(lambda x: int(x), dir_inodes))

    count = 0
    lsinfo = ""
    for inode_num in dir_inodes:
        url = FIREBASE_URL + NAMENODE + INODE + JSON + \
            '?orderBy="inode"&equalTo=' + str(inode_num)
        r = requests.get(url)
        curr_inode = list(r.json().values())[0]
        count += 1
        lsinfo += ls_format_print(curr_inode) + '\n'

    lsinfo = f"Found {count} items\n" + lsinfo
    return lsinfo, 200


def ls_format_print(node) -> str:
    '''
    Helper function to format the inode data into command line structure:
    # files: permissions number_of_replicas userid groupid filesize modification_date modification_time filename
    # directories: permissions userid groupid modification_date modification_time dirname
    Arguments:
        node - an inode object with all the data
    Returns:
        info - string of ls info in required format
    '''
    def permission_format(chmod, node_type):
        ans = ""
        if node_type == "DIRECTORY":
            ans += 'd'
        else:
            ans += '-'

        value_letters = [(4, "r"), (2, "w"), (1, "x")]
        for digit in [int(n) for n in str(chmod[1:])]:
            for value, letter in value_letters:
                if digit >= value:
                    ans += letter
                    digit -= value
                else:
                    ans += '-'
        return ans

    def find_size(blocks):
        blocks_list = list(blocks.values())
        sizes = list(map(lambda x: x['num_bytes'], blocks_list))
        return sum(sizes)//REPLICATION_FACTOR - len(blocks_list)//2 + 1

    answer = ['']*8
    userid, groupid, permissions = node['permission'].split(':')
    date_object = datetime.fromtimestamp(node['mtime'])

    answer[0] = permission_format(permissions, node['type'])
    answer[2] = userid
    answer[3] = groupid
    answer[1] = node['replication'] if 'replication' in node else 0
    answer[4] = find_size(node['blocks']) if 'blocks' in node else 0
    answer[5] = str(date_object.date())
    answer[6] = str(date_object.time())[:5]
    answer[7] = node['name']
    info = '\t'.join(str(i) if i else '-' for i in answer)
    return info


@app.route('/rmdir', methods=['GET'])
def rmdir() -> tuple[str, int]:
    '''
    This function removes a directory from the EDFS. Returns error if directory is not empty or doesn't exist, or if path is invalid
    Arguments:
        path: Path of the directory in the EDFS
    '''
    path = request.args.get('path')
    if path == "/":
        return f"Cannot remove {path}: Root directory", 400
    answer, order = is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"Cannot remove {path}: No such file or directory", 400
    last_inode = order[-1]

    url = FIREBASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + \
        "/".join(list(map(lambda x: str(x), order[:-1]))) + JSON
    r = requests.get(url)
    current_parent_directory = r.json()
    directory = current_parent_directory[str(last_inode)]
    if directory == '$':
        return f"Cannot remove {path}: Not a directory", 400
    elif type(directory) == dict:
        if directory['empty'] == False:
            return f"Cannot remove {path}: Directory is not empty", 400
        else:
            # delete from inode-directory-section
            del current_parent_directory[str(last_inode)]
            if len(current_parent_directory) == 1 and list(current_parent_directory.keys())[0] == 'empty':
                current_parent_directory['empty'] = True
            r = requests.put(url, data=json.dumps(current_parent_directory))

            # delete from inodes
            key_inode = str(last_inode) + "_" + list(filter(None,
                                                            path.split("/")))[-1].replace(".", "_")
            url = FIREBASE_URL + NAMENODE + INODE + key_inode + JSON
            r = requests.delete(url)

    return f"Deleted {path}", 200


@app.route('/rm', methods=['GET'])
def rm() -> tuple[str, int]:
    '''
    This function removes a file from the EDFS. Returns error if not a file or if path is invalid
    Arguments:
        path: Path of the file in the EDFS
    '''
    path = request.args.get('path')
    answer, order = is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"Cannot remove {path}: No such file or directory", 400
    last_inode = order[-1]

    url = FIREBASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + \
        "/".join(list(map(lambda x: str(x), order[:-1]))) + JSON
    r = requests.get(url)
    current_parent_directory = r.json()
    directory = current_parent_directory[str(last_inode)]
    if type(directory) == dict:
        return f"Cannot remove {path}: is a directory", 400
    elif directory == '$':
        # delete from inode-directory-section
        del current_parent_directory[str(last_inode)]
        if len(current_parent_directory) == 1 and list(current_parent_directory.keys())[0] == 'empty':
            current_parent_directory['empty'] = True
        r = requests.put(url, data=json.dumps(current_parent_directory))

        # delete from inodes and datanodes
        deletions_in_datanodes = {
            i: 0 for i in range(1, NUMBER_OF_DATANODES+1)}
        key_inode = str(last_inode) + "_" + list(filter(None,
                                                        path.split("/")))[-1].replace(".", "_")
        url = FIREBASE_URL + NAMENODE + INODE + key_inode + JSON
        r = requests.get(url)
        inode = r.json()
        blocks = inode.get('blocks', {})
        for block_id, block in blocks.items():
            datanode_id = block['datanode_id']
            deletions_in_datanodes[datanode_id] += 1
            datanode_url = FIREBASE_URL + DATANODE + \
                str(datanode_id) + '/' + str(block_id) + JSON
            r = requests.delete(datanode_url)

        datanode_metadata_url = FIREBASE_URL + DATANODE + METADATA + JSON
        r = requests.get(datanode_metadata_url)
        datanode_metadata = r.json()
        for key, value in deletions_in_datanodes.items():
            data_dict = datanode_metadata[str(key)]
            data_dict['count'] -= value
            if data_dict['count'] == 0:
                datanode_url = FIREBASE_URL + DATANODE + str(key) + JSON
                r = requests.patch(
                    datanode_url, data=json.dumps({"empty": True}))
        r = requests.put(datanode_metadata_url,
                         data=json.dumps(datanode_metadata))

        # delete from inodes
        r = requests.delete(url)

    return f"Deleted {path}", 200


@app.route('/cat', methods=['GET'])
def cat() -> tuple[str, int]:
    '''
    This function returns the content of the file
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    answer, order = is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"{path}: No such file or directory", 400
    last_inode = order[-1]

    url = FIREBASE_URL + NAMENODE + INODE + JSON + \
        '?orderBy="inode"&equalTo=' + str(last_inode)
    r = requests.get(url)
    curr_inode = list(r.json().values())[0]
    if curr_inode['type'] == 'DIRECTORY':
        return f"{path}: {curr_inode['name']} is directory", 400
    blocks = curr_inode.get('blocks', {})
    if len(blocks) == 0:
        return "", 200
    df = pd.DataFrame()
    for block_id, block in blocks.items():
        datanode_id = block['datanode_id']
        url = FIREBASE_URL + DATANODE + \
            str(datanode_id) + '/' + str(block_id) + JSON
        r = requests.get(url)
        df = pd.concat([df, pd.DataFrame.from_dict(r.json())])
    df = df.drop_duplicates()
    df = df.sort_values(by='index')
    df = df.drop('index', axis=1)
    data = df.to_csv(index=False)
    return data, 200


@app.route('/put', methods=['GET'])
def put() -> tuple[str, int]:
    '''
    This function puts the file specified into the EDFS. Returns error if the path is invalid or file is invalid
    Arguments:
        source: Path of the file in the local file system
        destination: Path of the file in the EDFS
        Optional:
            partitions: Number of partitions of the file to be stored
            hash: The column on which the file is to be hashed
    '''
    args = request.args.to_dict()
    source = args['source']
    if not os.path.exists(source):
        return f"put: File does not exist: {source}", 400
    csvFile = Path(source)
    if not csvFile.is_file or not csvFile.suffix == ".csv":
        return f"put: Invalid file: {source}", 400
    destination = args['destination']
    answer, order_of_dir = is_valid_path(
        list(filter(None, destination.split("/")))[:-1])
    if not answer:
        return f"Path does not exist: {destination}", 400
    curr_file = destination.split('/')[-1]
    curr_parent = '/'.join(destination.split('/')[:-1])

    partitions = 1
    if 'partitions' in args:
        partitions = int(args['partitions'])
    og_partitions = partitions
    hash_attr = 0
    if 'hash' in args:
        hash_attr = args['hash']

    # 4 Steps:
    ## update in datanode
    ## update in datanode_metadata
    ## update in inode
    ## update in inode_directory_section

    # generate an inode
    url = FIREBASE_URL + NAMENODE + INODE + JSON
    r = requests.get(url)
    existing_inode_nums = list(r.json().values())
    existing_inode_nums = list(map(lambda x: x['inode'], existing_inode_nums))
    inode_num = max(existing_inode_nums) + 1
    inode_name = str(inode_num) + '_' + curr_file.replace('.', '_')
    curr_inode = {}
    curr_timestamp = int(time.time())
    curr_inode['atime'] = curr_timestamp
    curr_inode['ctime'] = curr_timestamp
    curr_inode['inode'] = inode_num
    curr_inode['mtime'] = curr_timestamp
    curr_inode['name'] = curr_file
    curr_inode['permission'] = FIREBASE_DEFAULT_FILE_PERMISSION
    curr_inode['replication'] = REPLICATION_FACTOR
    curr_inode['type'] = "FILE"

    # blocks
    file_size = os.path.getsize(source)
    df = pd.read_csv(source)

    attribute_type = df[hash_attr].dtypes
    if attribute_type == np.dtype('float64') or attribute_type == np.dtype('int64'):
        df[hash_attr].fillna(0, inplace=True)
    else:
        df[hash_attr].fillna("NULL", inplace=True)

    grouped_df = df.reset_index().replace(
        np.nan, '', regex=True).groupby(by=hash_attr)
    number_of_groups = len(grouped_df)

    # since we are hashing on an attribute, the number of partitions is decided by number of unique values of that attribute
    # if file can be stored in number of partitions lesser than mentioned by user, then we pick the lesser value
    # we are not implementing bucketing so even if a block is storing a partition of a file that is very small and there is memory wastage, we do not care
    # write-once-read-many so we do not need think about what if file is modified

    if number_of_groups > partitions:
        partitions = number_of_groups
    partition_size = min(ceil(file_size/partitions), MAX_PARTITION_SIZE)
    rows_per_partition = ceil((df.shape[0]*partition_size)/file_size)

    label_row_size = sum([len(col)+2 for col in df.columns]
                         ) + len(df.columns) - 1

    block_number_offset = -1
    blocks = {}
    actual_total_partitions = 0
    additions_in_datanodes = {i: 0 for i in range(1, NUMBER_OF_DATANODES+1)}
    for hash_val, hash_df in grouped_df:
        hash_num_partitions = ceil(hash_df.shape[0]/rows_per_partition)
        actual_total_partitions += hash_num_partitions
        data = hash_df.to_dict(orient='records')
        i = 0
        end_row = rows_per_partition - 1
        for order in range(hash_num_partitions):
            block_number_offset += 1
            if order < hash_num_partitions - 1:
                data_to_put = data[i:end_row+1]
                i = end_row + 1
                end_row += rows_per_partition - 1
            elif order == hash_num_partitions - 1:
                data_to_put = data[i:len(data)]
            d = [len(','.join([str(int(e)) if isinstance(e, float) and e.is_integer(
            ) else str(e) for e in list(d.values())])) for d in data_to_put]
            data_to_put_size = sum(d) + len(data_to_put) + label_row_size
            datanode_nums = sample(
                range(1, NUMBER_OF_DATANODES+1), REPLICATION_FACTOR)
            for rep_i in range(REPLICATION_FACTOR):
                block = {}
                block_id = "".join(choices(string.ascii_letters, k=32))
                block['block_num'] = block_number_offset
                block['datanode_id'] = datanode_nums[rep_i]
                block['hash_attr_val'] = hash_val
                block['num_bytes'] = data_to_put_size
                block['order'] = order
                block['replica_num'] = rep_i+1
                blocks[block_id] = block

                ## update in datanode
                datanode_id = datanode_nums[rep_i]
                additions_in_datanodes[datanode_id] += 1
                url = FIREBASE_URL + DATANODE + str(datanode_id) + JSON
                r = requests.get(url)
                curr_datanode = r.json()
                curr_datanode['empty'] = False
                curr_datanode[block_id] = data_to_put
                r = requests.put(url, data=json.dumps(curr_datanode))

    ## update in datanode_metadata
    datanode_metadata_url = FIREBASE_URL + DATANODE + METADATA + JSON
    r = requests.get(datanode_metadata_url)
    datanode_metadata = r.json()
    for key, value in additions_in_datanodes.items():
        data_dict = datanode_metadata[str(key)]
        data_dict['count'] += value
    r = requests.put(datanode_metadata_url, data=json.dumps(datanode_metadata))

    ## update in inode
    curr_inode['blocks'] = blocks
    url = FIREBASE_URL + NAMENODE + INODE + inode_name + JSON
    r = requests.put(url, data=json.dumps(curr_inode))

    ## update in inode_directory_section
    url = FIREBASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + \
        "/".join(list(map(lambda x: str(x), order_of_dir))) + JSON
    r = requests.get(url)
    dir_val = r.json()
    dir_val['empty'] = False
    dir_val[inode_num] = "$"
    r = requests.put(url, data=json.dumps(dir_val))

    return f"File stored in {actual_total_partitions} partitions", 200


@app.route('/getPartitionLocations', methods=['GET'])
def getPartitionLocations() -> tuple[str, int]:
    '''
    This function returns the partition locations of a file in the EDFS. Returns error if path is invalid
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    answer, order = is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"{path}: No such file or directory", 400
    inode_num = order[-1]

    partition_info, status = getPartitionIds(path, inode_num)
    return f"Partitions: {partition_info}", status


def getPartitionIds(path: str, inode_num: int, hash_attr_val: str = "") -> tuple[Union[str, dict], int]:

    url = FIREBASE_URL + NAMENODE + INODE + JSON + \
        '?orderBy="inode"&equalTo=' + str(inode_num)
    r = requests.get(url)
    curr_inode = list(r.json().values())[0]
    # if curr_inode['type'] == 'DIRECTORY':
    #     return f"{path}: {curr_inode['name']} is directory", 400

    partitions = {
        "Replica 1": dict(),
        "Replica 2": dict()
    }
    blocks = curr_inode.get('blocks', {})
    if hash_attr_val != "":
        blocks = dict(filter(lambda block: block[1]['hash_attr_val'] == float(
            hash_attr_val), blocks.items()))

    for block_id, block in blocks.items():
        block_num = str(block['block_num'] + 1)
        datanode = "Datanode " + str(block['datanode_id'])
        replica_key = "Replica " + str(block['replica_num'])
        partitions[replica_key][block_num] = {datanode: block_id}
    if not partitions["Replica 1"] and not partitions["Replica 2"]:
        return f"No partitions found for {path}", 200
    return partitions, 200


@app.route('/readPartition', methods=['GET'])
def readPartition() -> tuple[str, int]:
    '''
    This function returns the content of the partition of the file specified by the partition and path parameters
    Arguments:
        path: Path of the file/directory in the EDFS
        partition: Partition number to be read (1-indexed)
    '''
    path = request.args.get('path')
    partition = int(request.args.get('partition'))
    answer, order = is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"{path}: No such file or directory", 400
    inode_num = order[-1]

    data, status = readPartitionContent(path, inode_num, partition)
    content = data
    if not isinstance(data, str):
        df = pd.DataFrame.from_dict(data)
        df = df.sort_values(by='index')
        df = df.drop('index', axis=1)
        content = df.to_csv(index=False)
    return content, status


def readPartitionContent(path: str, inode_num: int, partition: int) -> tuple[Union[str, dict], int]:

    inode_name = str(inode_num) + '_' + \
        list(filter(None, path.split("/")))[-1].replace('.', '_')
    url = FIREBASE_URL + NAMENODE + INODE + inode_name + '/blocks/' + JSON + \
        '?orderBy="block_num"&equalTo=' + str(partition-1)
    r = requests.get(url)
    blocks = r.json()
    if not blocks or len(blocks) == 0:
        return f"No partitions found for {path}", 200

    block_id, block = list(blocks.items())[0]
    datanode_id = block['datanode_id']
    url = FIREBASE_URL + DATANODE + \
        str(datanode_id) + '/' + str(block_id) + JSON
    r = requests.get(url)
    data = r.json()

    if len(data) == 0:
        return f"No content found for partition {partition} of file {path}", 400

    return data, 200


@app.route('/getAvgArmCircum', methods = ['GET'])
def getAvgArmCircum() -> tuple[str, int]:
    args = request.args.to_dict()
    path = args["path"]
    hash = float(args["hash"])
    debug = False
    if "debug" in args:
        debug = bool(request.args.get("debug"))
    
    answer, order = is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"{path}: No such file or directory", 400
    inode_num = order[-1]
    
    partitions, status = getPartitionIds(path, inode_num, hash)
    resultPromises = []
    if status == 200 and isinstance(partitions, str) == False:
        with Pool(processes=max(len(partitions["Replica 1"]), len(partitions["Replica 2"]))) as pool:
            if partitions["Replica 1"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, inode_num, partition, calcAvgArmCircum, debug)) for partition, _ in partitions["Replica 1"].items()]
            elif partitions["Replica 2"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, inode_num, partition, calcAvgArmCircum, debug)) for partition, _ in partitions["Replica 2"].items()]
            results = [promise.get() for promise in resultPromises]
        pool.join()
        return reduce(results, combineAverages, debug)
    return partitions, status

def mapPartition(path: str, inode_num: int, partition: str, callback: Callable[[str], tuple[dict, int]], debug: bool = False) -> tuple[dict, int]:
    '''
    This function takes partition identified by partitionId and transforms the data in it according to the callback function
    Arguments:
        path - The path of the file in the EDFS
        inode_num - The inode number of the file in EDFS
        partition - Partition number of the partition
        callback - The callback function used to transform the data in the partition
    Returns:
        res - The data after transforming the content from the partition
    '''
    res, status = readPartitionContent(path, inode_num, int(partition))
    if status == 200 and isinstance(res, str) == False:
        output, s = callback(res)
        if s == 200 and debug:
            output["explanation"] = {
                "Partition": partition,
                "Input": res,
                "Output": output["data"]
            }
        return output, s
    return {
        "message": res,
        "data": {}
    }, status


def reduce(results: list, callback: Callable[[list, bool], tuple[str, int]], debug: bool = False) -> tuple[str, int]:
    return callback(results, debug)

def calcAvgArmCircum(data) -> tuple[dict, int]:
    print(type(data), 'Expected List')
    df = pd.DataFrame.from_dict(data)
    df = df.sort_values(by='index')
    df = df.drop('index', axis=1)
    return {
        "message": "Successfully calculated average",
        "data": {
            "average": df['BMXARMC'].mean(),
            "size": len(df.index)
        }
    }, 200

def combineAverages(results: list, debug: bool) -> tuple[str, int]:
    cumulativeAvg = sum([0 if status != 200 else result["data"]["average"]*result["data"]["size"] for result, status in results])
    totalCount = sum([0 if status != 200 else result["data"]["size"] for result, status in results])
    res = {
        "result": "No data found"
    }
    status = 400
    if totalCount > 0:
        res["result"] = f"The overall average is {(cumulativeAvg/totalCount)}"
        if debug:
            res["explanation"] = [result['explanation'] for result, _ in results]
        status = 200
    return res, status