import json
import numpy as np
import os
import pandas as pd
import pymysql
import requests
import string
import time
from ast import literal_eval
from datetime import datetime
from io import StringIO
from dotenv import load_dotenv
from flask import Flask, request
from flask_cors import CORS
from math import ceil, inf
from multiprocessing import Pool
from pathlib import Path
from random import choices, sample
from sys import getsizeof
from typing import Callable, Union

load_dotenv()

app = Flask(__name__)
CORS(app)

HOST_NAME = os.environ.get('HOST')
DB_USERNAME = os.environ.get('USERNAME')
DB_PASSWORD = os.environ.get('PASSWORD')
DATABASE = os.environ.get('DATABASE')
MAX_PARTITION_SIZE = int(os.environ.get('MAX_PARTITION_SIZE'))
DEFAULT_DIR_PERMISSION = os.environ.get('DEFAULT_DIR_PERMISSION')
DEFAULT_FILE_PERMISSION = os.environ.get('DEFAULT_FILE_PERMISSION')
REPLICATION_FACTOR = 2
MAX_THREADS = int(os.environ.get('MAX_THREADS'))

FIREBASE_URL = os.environ.get('FIREBASE_URL')
FIREBASE_DEFAULT_DIR_PERMISSION = os.environ.get('FIREBASE_DEFAULT_DIR_PERMISSION')
FIREBASE_DEFAULT_FILE_PERMISSION = os.environ.get('FIREBASE_DEFAULT_FILE_PERMISSION')
FIREBASE_MAX_PARTITION_SIZE = int(os.environ.get('FIREBASE_MAX_PARTITION_SIZE'))
NUMBER_OF_DATANODES = 3
JSON = ".json"
DATANODE = "datanode/"
METADATA = "metadata/"
NAMENODE = "namenode/"
INODE_DIRECTORY_SECTION = "inode_directory_section/"
INODE = "inodes/"

# MySQL APIS
def is_valid_path(nodes: list) -> tuple[str, int]:
    '''
    Helper function to check if given path exists in the EDFS
    Arguments:
        nodes - List of nodes in the path split on /
    Returns:
        curParent - The node after which the path doesn't exists or empty string
        missingChildDepth - The depth of the missing child in the path or -1
    '''
    conn = pymysql.connect(
        host=HOST_NAME,
        user=DB_USERNAME, 
        password = DB_PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    query = "SELECT nn.name AS parent_name, nn2.name AS child_name FROM parent_child pc " + \
        "JOIN namenode nn ON pc.parent_inode = nn.inode_num " + \
        "JOIN namenode nn2 ON pc.child_inode = nn2.inode_num"
    cursor.execute(query)
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    parent_child = {v: k for k,v in res}
    curParent = "/"
    depth = 0
    for node in nodes:
        if curParent+(depth != 0)*'/'+node in parent_child:
            if parent_child[curParent+(depth != 0)*'/'+node] == curParent:
                curParent += (depth != 0)*'/' + node
                depth += 1
        else:
            return curParent, depth
    return "", -1

@app.route('/mkdir', methods = ['GET'])
def mkdir() -> tuple[object, int]:
    '''
    This function creates a new directory in the EDFS. Returns error if the directory already exists.
    Arguments:
        path: Path of the new directory in the EDFS
    '''
    path = request.args.get('path')
    nodes = list(filter(None, path.split("/")))
    curParent, missingChildDepth = is_valid_path(nodes)
    if missingChildDepth == -1:
        return {
            "response": f"mkdir: {path}: File exists", 
            "status": "EDFS400"
        }, 200
    else:
        conn = pymysql.connect(
            host=HOST_NAME,
            user=DB_USERNAME, 
            password = DB_PASSWORD,
            database=DATABASE
        )
        cursor = conn.cursor()
        depth = missingChildDepth
        for node in nodes[missingChildDepth:]:
            query = "INSERT INTO Namenode VALUES (" + \
                "UUID()," + \
                "'d'," + \
                f"'{curParent+(depth != 0)*'/'+node}'," + \
                "NULL," + \
                "NULL," + \
                "NULL," + \
                "NOW()," + \
                f"{DEFAULT_DIR_PERMISSION}" + \
                ")"
            cursor.execute(query)
            query = "SELECT nn.inode_num AS parent_inode, " + \
                "nn2.inode_num AS child_inode " + \
                "FROM Namenode nn, Namenode nn2 " + \
                f"WHERE nn.name='{curParent}' AND nn2.name='{curParent+(depth != 0)*'/'+node}'"
            cursor.execute(query)
            res = cursor.fetchall()
            query = "INSERT INTO Parent_Child VALUES (" + \
                f"'{res[0][0]}'," + \
                f"'{res[0][1]}')"
            cursor.execute(query)
            curParent += (depth != 0)*'/'+node
        cursor.close()
        conn.commit()
        conn.close()
        return {
            "response": "",
            "status": "EDFS200"
        }, 200

@app.route('/ls', methods=['GET'])
def ls() -> tuple[object, int]:
    '''
    This function lists the content of the given directory in the EDFS. Returns error if the directory doesn't exists.
    Arguments:
        path: Path of the directory in the EDFS
    '''
    path = request.args.get('path')
    nodes = list(filter(None, path.split("/")))
    _, pathMissing = is_valid_path(nodes)
    if pathMissing == -1:
        conn = pymysql.connect(
            host=HOST_NAME,
            user=DB_USERNAME, 
            password = DB_PASSWORD,
            database=DATABASE
        )
        cursor = conn.cursor()
        query = "SELECT nn2.node_type, " + \
            "nn2.permission, " + \
            "nn2.mtime, " + \
            "nn2.name " + \
            "FROM Parent_Child pc " + \
            "JOIN Namenode nn ON pc.parent_inode = nn.inode_num " + \
            "JOIN Namenode nn2 ON pc.child_inode = nn2.inode_num " + \
            f"WHERE nn.name = '{path}'"
        cursor.execute(query)
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        lsinfo = ""
        for row in res:
            formatted_permission = format_permissions(row[1])
            lsinfo += row[0]+formatted_permission+'\t'
            lsinfo += '\t'.join(str(i) if i else '-' for i in row[2:]) + '\n'
        if lsinfo:
            lsinfo = f"Found {len(res)} items\n" + lsinfo
        return {
            "response": lsinfo,
            "status": "EDFS200"
        }, 200
    else:
        return {
            "response": f"ls: {path}: No such file or directory",
            "status": "EDFS400"
        }, 200

def format_permissions(permission: int) -> str:
    res = ""
    power = 100
    while permission > 0:
        if permission < power:
            res += '---'
        else:
            digit = permission//power
            permission -= (digit*power)
            if digit > 3:
                res += 'r'
                digit -= 4
            else:
                res += '-'
            if digit > 1:
                res += 'w'
                digit -= 2
            else:
                res += '-'
            if digit > 0:
                res += 'x'
                digit -= 1
            else:
                res += '-'
        power = power//10
    return res

@app.route('/rm', methods=['GET'])
def rm() -> tuple[object, int]:
    '''
    This function removes a file/directory from the EDFS. Returns error if directory is not empty or if path is invalid
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    if path == "/":
        return {
            "response": f"Cannot remove {path}: Root directory",
            "status": "EDFS400"
        }, 200
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return {
            "response": f"Cannot remove {path}: No such file or directory",
            "status": "EDFS400"
        }, 200
    query = f"SELECT child_inode FROM Namenode nn LEFT JOIN Parent_Child pc ON nn.inode_num = pc.parent_inode WHERE nn.name = '{path}'"
    conn = pymysql.connect(
        host=HOST_NAME,
        user=DB_USERNAME, 
        password = DB_PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    child_inode = res[0][0]
    if child_inode:
        cursor.close()
        conn.close()
        return {
            "response": f"Cannot remove {path}: Directory is not empty",
            "status": "EDFS400"
        }, 200
    query = "DELETE nn, pc, bi, d1, d2, d3, d4, d5, d6 FROM Namenode nn" + \
        " INNER JOIN Parent_Child pc ON nn.inode_num = pc.child_inode" + \
        " LEFT JOIN Block_info_table bi ON nn.inode_num = bi.file_inode" + \
        " LEFT JOIN Datanode_1 d1 ON bi.replica1_data_blk_id = d1.data_block_id" + \
        " LEFT JOIN Datanode_2 d2 ON bi.replica1_data_blk_id = d2.data_block_id" + \
        " LEFT JOIN Datanode_3 d3 ON bi.replica1_data_blk_id = d3.data_block_id" + \
        " LEFT JOIN Datanode_1 d4 ON bi.replica2_data_blk_id = d4.data_block_id" + \
        " LEFT JOIN Datanode_2 d5 ON bi.replica2_data_blk_id = d5.data_block_id" + \
        " LEFT JOIN Datanode_3 d6 ON bi.replica2_data_blk_id = d6.data_block_id" + \
        f" WHERE nn.name = '{path}'"
    cursor.execute(query)
    cursor.close()
    conn.commit()
    conn.close()
    return {
        "response": f"Deleted {path}",
        "status": "EDFS200"
    }, 200

@app.route('/cat', methods=['GET'])
def cat() -> tuple[object, int]:
    '''
    This function returns the content of the file
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return {
            "response": f"{path}: No such file or directory",
            "status": "EDFS400"
        }, 200
    query = "SELECT IFNULL(" + \
                "CONCAT(COALESCE(d1.content, ''), COALESCE(d2.content, ''), COALESCE(d3.content, '')), " + \
                "CONCAT(COALESCE(d4.content, ''), COALESCE(d5.content, ''), COALESCE(d6.content, ''))" + \
            ") AS content FROM Block_info_table bi" + \
            " INNER JOIN Namenode nn ON nn.inode_num = bi.file_inode" + \
            " LEFT JOIN Datanode_1 d1 ON d1.data_block_id = bi.replica1_data_blk_id" + \
            " LEFT JOIN Datanode_2 d2 ON d2.data_block_id = bi.replica1_data_blk_id" + \
            " LEFT JOIN Datanode_3 d3 ON d3.data_block_id = bi.replica1_data_blk_id" + \
            " LEFT JOIN Datanode_1 d4 ON d4.data_block_id = bi.replica2_data_blk_id" + \
            " LEFT JOIN Datanode_2 d5 ON d5.data_block_id = bi.replica2_data_blk_id" + \
            " LEFT JOIN Datanode_3 d6 ON d6.data_block_id = bi.replica2_data_blk_id" + \
            f" WHERE nn.name = '{path}'"
    conn = pymysql.connect(
        host=HOST_NAME,
        user=DB_USERNAME, 
        password = DB_PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    if len(res) > 0:
        df = pd.DataFrame()
        for row in res:
            csvStringIO = StringIO(row[0])
            row_df = pd.read_csv(csvStringIO, sep=",")
            df = pd.concat([df, row_df])
        df = df.sort_values(by='index')
        df = df.drop('index', axis=1)
        return {
            "response": df.to_csv(index=False),
            "status": "EDFS200"
        }, 200
    return {
        "response": "",
        "status": "EDFS204"
    }, 200

@app.route('/put', methods=['GET'])
def put() -> tuple[object, int]:
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
        return {
            "response": f"put: File does not exist: {source}",
            "status": "EDFS400"
        }, 200
    csvFile = Path(source)
    if not csvFile.is_file or not csvFile.suffix == ".csv":
        return {
            "response": f"put: Invalid file: {source}",
            "status": "EDFS400"
        }, 200
    destination = args['destination']
    _, missingChildDepth = is_valid_path(list(filter(None, destination.split("/")))[:-1])
    if missingChildDepth != -1:
        return {
            "response": f"Path does not exist: {destination}",
            "status": "EDFS400"
        }, 200
    curParent = '/'.join(destination.split('/')[:-1])
    partitions = int(args['partitions'])
    hash_attr = 0
    if 'hash' in args:
        hash_attr = args['hash']
    file_size = os.path.getsize(source)
    partition_size = min(ceil(file_size/partitions), MAX_PARTITION_SIZE)
    conn = pymysql.connect(
        host=HOST_NAME,
        user=DB_USERNAME, 
        password = DB_PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    query = "INSERT INTO Namenode VALUES (" + \
        "UUID()," + \
        "'-'," + \
        f"'{destination}'," + \
        f"{REPLICATION_FACTOR}," + \
        "NULL," + \
        "NULL," + \
        "NOW()," + \
        f"{DEFAULT_FILE_PERMISSION}" + \
        ")"
    cursor.execute(query)
    cursor.execute(f"SELECT nn.inode_num, nn2.inode_num FROM Namenode nn, Namenode nn2 WHERE nn.name = '{destination}' AND nn2.name = '{curParent}'")
    res = cursor.fetchall()
    inode_num = res[0][0]
    parent_inode_num = res[0][1]
    blk_info_query = "INSERT INTO Block_info_table VALUES (" + \
        "'{}'," + \
        f"'{inode_num}'," + \
        "'{}'," + \
        "{}," + \
        "{}," + \
        "'{}'," + \
        "{}," + \
        "'{}'," + \
        "{}" + \
        ")"
    datanode_query = "INSERT INTO Datanode_{} VALUES (" + \
        "'{}'," + \
        "\"{}\"" + \
        ")"
    parent_child_query = "INSERT INTO Parent_Child VALUES ('{}', '{}')"
    df = pd.read_csv(source)
    df = df.reset_index()
    rowsPerPartition = ceil((df.shape[0]*partition_size)/file_size)
    offset = 0
    try:
        if np.issubdtype(df[hash_attr].dtypes, np.number):
            df[hash_attr].fillna(0, inplace=True)
        else:
            df[hash_attr].fillna("NULL", inplace=True)
        groups = df.groupby(by=hash_attr)
        
    except KeyError:
        df["hash"] = pd.cut(x=df[df.columns[0]], bins=partitions)
        df["hash"] = df["hash"].astype(str)
        groups = df.groupby(by="hash")
        del df["hash"]
    for hash_val, data in groups:
        num_partitions = ceil(data.shape[0]/rowsPerPartition)
        for chunk in np.array_split(data, num_partitions):
            chunk_str = chunk.to_csv(index=False)
            block_id = "".join(choices(string.ascii_letters, k=32))
            data_block_id1 = "".join(choices(string.ascii_letters, k=32))
            data_block_id2 = "".join(choices(string.ascii_letters, k=32))
            data_block_ids = [data_block_id1, data_block_id2]
            datanode_nums = sample(range(1, 4), REPLICATION_FACTOR)
            for i in range(REPLICATION_FACTOR):
                cursor.execute(datanode_query.format(datanode_nums[i], data_block_ids[i], chunk_str))
            cursor.execute(blk_info_query.format(block_id, hash_val, getsizeof(chunk_str), offset, data_block_ids[0], datanode_nums[0], data_block_ids[1], datanode_nums[1]))
            offset += 1
    cursor.execute(parent_child_query.format(parent_inode_num, inode_num))
    cursor.close()
    conn.commit()
    conn.close()
    return {
        "response": "",
        "status": "EDFS200"
    }, 200

@app.route('/getPartitionLocations', methods=['GET'])
def getPartitionLocations() -> tuple[object, int]:
    '''
    This function returns the partition locations of a file in the EDFS. Returns error if path is invalid
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    response, status = getPartitionIds(path)
    return {
        "response": response,
        "status": "EDFS"+str(status)
    }, 200

def getPartitionIds(path: str, hash: str = None) -> tuple[Union[str, dict], int]:
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return f"{path}: No such file or directory", 400
    query = "SELECT bi.offset, bi.replica1_datanode_num, bi.replica1_data_blk_id, bi.replica2_datanode_num, bi.replica2_data_blk_id FROM Block_info_table bi" + \
        " INNER JOIN Namenode nn ON nn.inode_num = bi.file_inode" + \
        f" WHERE nn.name = '{path}'"
    if hash:
        try:
            hash = float(hash)
        except ValueError:
            pass
        query += f" AND bi.hash_attribute = '{hash}'"
    query += " ORDER BY bi.offset"
    conn = pymysql.connect(
        host=HOST_NAME,
        user=DB_USERNAME, 
        password = DB_PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    partitions = {
        "Replica 1": dict(),
        "Replica 2": dict()
    }
    for id_set in res:
        partitions["Replica 1"][str(id_set[0]+1)] = {
            "Datanode "+str(id_set[1]): id_set[2]
        }
        partitions["Replica 2"][str(id_set[0]+1)] = {
            "Datanode "+str(id_set[3]): id_set[4]
        }
    if not partitions["Replica 1"] and not partitions["Replica 2"]:
        return f"No partitions found for {path}", 200
    return partitions, 200

@app.route('/readPartition', methods=['GET'])
def readPartition() -> tuple[object, int]:
    '''
    This function returns the content of the partition of the file specified by the partition and path parameters
    Arguments:
        path: Path of the file/directory in the EDFS
        partition: Partition number to be read (1-indexed)
    '''
    path = request.args.get('path')
    partition = request.args.get('partition')
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return {
            "response": f"{path}: No such file or directory",
            "status": "EDFS400"
        }, 200
    response, status = readPartitionContent(path, partition)
    df = pd.DataFrame()
    if status == 200:
        csvStringIO = StringIO(response)
        df = pd.read_csv(csvStringIO, sep=",")
        df = df.sort_values(by='index')
        df = df.drop('index', axis=1)
        response = df.to_csv(index=False)
    return {
        "response": response,
        "status": "EDFS"+str(status)
    }, 200

def readPartitionContent(path: str, partition: int) -> tuple[str, int]:
    query = "SELECT IFNULL(" + \
                "CONCAT(COALESCE(d1.content, ''), COALESCE(d2.content, ''), COALESCE(d3.content, '')), " + \
                "CONCAT(COALESCE(d4.content, ''), COALESCE(d5.content, ''), COALESCE(d6.content, ''))" + \
            ") AS content FROM Block_info_table bi" + \
            " INNER JOIN Namenode nn ON nn.inode_num = bi.file_inode" + \
            " LEFT JOIN Datanode_1 d1 ON d1.data_block_id = bi.replica1_data_blk_id" + \
            " LEFT JOIN Datanode_2 d2 ON d2.data_block_id = bi.replica1_data_blk_id" + \
            " LEFT JOIN Datanode_3 d3 ON d3.data_block_id = bi.replica1_data_blk_id" + \
            " LEFT JOIN Datanode_1 d4 ON d4.data_block_id = bi.replica2_data_blk_id" + \
            " LEFT JOIN Datanode_2 d5 ON d5.data_block_id = bi.replica2_data_blk_id" + \
            " LEFT JOIN Datanode_3 d6 ON d6.data_block_id = bi.replica2_data_blk_id" + \
            f" WHERE nn.name = '{path}' AND bi.offset = {int(partition) - 1}"
    conn = pymysql.connect(
        host=HOST_NAME,
        user=DB_USERNAME, 
        password = DB_PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    cursor.close()
    conn.close()
    if len(res) == 0:
        return f"No content found for partition {partition} of file {path}", 400
    return res[0][0], 200

@app.route('/getAvg', methods = ['GET'])
def getAvg() -> tuple[str, int]:
    args = request.args.to_dict()
    path = args["path"]
    col = args["col"]
    hash = None
    if "hash" in args:
        hash = args["hash"]
    debug = False
    if "debug" in args:
        try:
            debug = literal_eval(args["debug"])
        except ValueError:
            pass
    data, status = readPartitionContent(path, 1)
    if status != 200:
        return data, status
    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    try:
        if not np.issubdtype(df[col].dtypes, np.number):
            return {
                "response": f"Cannot calculate average on column {col}: Data not numeric",
                "status": "EDFS400"
            }, 200
    except KeyError:
        return {
            "response": f"Column {col} doesn't exist",
            "status": "EDFS400"
        }, 200
    partitions, status = getPartitionIds(path, hash)
    resultPromises = []
    if status == 200:
        with Pool(processes=max(len(partitions["Replica 1"]), len(partitions["Replica 2"]))) as pool:
            if partitions["Replica 1"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, partition, calcAvg, col, debug)) for partition, _ in partitions["Replica 1"].items()]
            elif partitions["Replica 2"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, partition, calcAvg, col, debug)) for partition, _ in partitions["Replica 2"].items()]
            results = [promise.get() for promise in resultPromises]
        pool.join()
        response, red_status = reduce(results, combineAverages, debug)
        return {
            "response": response,
            "status": "EDFS"+str(red_status)
        }, 200
    return {
        "response": partitions,
        "status": "EDFS"+str(status)
    }, 200

@app.route('/getMax', methods=['GET'])
def getMax() -> tuple[str, int]:
    args = request.args.to_dict()
    path = args["path"]
    col = args["col"]
    hash = None
    if "hash" in args:
        hash = args["hash"]
    debug = False
    if "debug" in args:
        try:
            debug = literal_eval(args["debug"])
        except ValueError:
            pass
    data, status = readPartitionContent(path, 1)
    if status != 200:
        return data, status
    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    try:
        if not np.issubdtype(df[col].dtypes, np.number):
            return {
                "response": f"Cannot calculate max on column {col}: Data not numeric",
                "status": "EDFS400"
            }, 200
    except KeyError:
        return {
            "response": f"Column {col} doesn't exist",
            "status": "EDFS400"
        }, 200
    partitions, status = getPartitionIds(path, hash)
    resultPromises = []
    if status == 200:
        with Pool(processes=max(len(partitions["Replica 1"]), len(partitions["Replica 2"]))) as pool:
            if partitions["Replica 1"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, partition, calcMax, col, debug)) for partition, _ in partitions["Replica 1"].items()]
            elif partitions["Replica 2"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, partition, calcMax, col, debug)) for partition, _ in partitions["Replica 2"].items()]
            results = [promise.get() for promise in resultPromises]
        pool.join()
        response, red_status = reduce(results, cumulativeMax, debug)
        return {
            "response": response,
            "status": "EDFS"+str(red_status)
        }, 200
    return {
        "response": partitions,
        "status": "EDFS"+str(status)
    }, 200

@app.route('/getMin', methods=['GET'])
def getMin() -> tuple[str, int]:
    args = request.args.to_dict()
    path = args["path"]
    col = args["col"]
    hash = None
    if "hash" in args:
        hash = args["hash"]
    debug = False
    if "debug" in args:
        try:
            debug = literal_eval(args["debug"])
        except ValueError:
            pass
    data, status = readPartitionContent(path, 1)
    if status != 200:
        return data, status
    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    try:
        if not np.issubdtype(df[col].dtypes, np.number):
            return {
                "response": f"Cannot calculate min on column {col}: Data not numeric",
                "status": "EDFS400"
            }, 200
    except KeyError:
        return {
            "response": f"Column {col} doesn't exist",
            "status": "EDFS400"
        }, 200
    partitions, status = getPartitionIds(path, hash)
    resultPromises = []
    if status == 200:
        with Pool(processes=max(len(partitions["Replica 1"]), len(partitions["Replica 2"]))) as pool:
            if partitions["Replica 1"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, partition, calcMin, col, debug)) for partition, _ in partitions["Replica 1"].items()]
            elif partitions["Replica 2"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, partition, calcMin, col, debug)) for partition, _ in partitions["Replica 2"].items()]
            results = [promise.get() for promise in resultPromises]
        pool.join()
        response, red_status = reduce(results, cumulativeMin, debug)
        return {
            "response": response,
            "status": "EDFS"+str(red_status)
        }, 200
    return {
        "response": partitions,
        "status": "EDFS"+str(status)
    }, 200

def mapPartition(path: str, partition: str, callback: Callable[[str, str], tuple[dict, int]], column: str, debug: bool = False) -> tuple[dict, int]:
    '''
    This function takes partition identified by partitionId and transforms the data in it according to the callback function
    Arguments:
        path - The path of the file in the EDFS
        partition - Partition number of the partition
        callback - The callback function used to transform the data in the partition
    Returns:
        res - The data after transforming the content from the partition
    '''
    res, status = readPartitionContent(path, partition)
    if status == 200:
        output, s = callback(res, column)
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

def calcAvg(data: str, col: str) -> tuple[dict, int]:
    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    return {
        "message": "Successfully calculated average",
        "data": {
            "average": df[col].mean(),
            "size": len(df.index)
        }
    }, 200

def calcMax(data: str, col: str) -> tuple[dict, int]:
    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    return {
        "message": "Successfully calculated maximum",
        "data": {
            "max": df[col].max(),
            "size": len(df.index)
        }
    }, 200

def calcMin(data: str, col: str) -> tuple[dict, int]:
    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    return {
        "message": "Successfully calculated minimum",
        "data": {
            "min": df[col].min(),
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
        res["result"] = f"The overall average is {(cumulativeAvg/totalCount):.3f}"
        if debug:
            res["explanation"] = [result['explanation'] for result, _ in results]
        status = 200
    return res, status

def cumulativeMax(results: list, debug: bool) -> tuple[str, int]:
    cumulativeMax = max([0 if status != 200 else result["data"]["max"] for result, status in results])
    totalCount = sum([0 if status != 200 else result["data"]["size"] for result, status in results])
    res = {
        "result": "No data found"
    }
    status = 400
    if totalCount > 0:
        res["result"] = f"The overall maximum is {(cumulativeMax):.3f}"
        if debug:
            res["explanation"] = [result['explanation'] for result, _ in results]
        status = 200
    return res, status

def cumulativeMin(results: list, debug: bool) -> tuple[str, int]:
    cumulativeMax = min([inf if status != 200 else result["data"]["min"] for result, status in results])
    totalCount = sum([0 if status != 200 else result["data"]["size"] for result, status in results])
    res = {
        "result": "No data found"
    }
    status = 400
    if totalCount > 0:
        res["result"] = f"The overall minimum is {(cumulativeMax):.3f}"
        if debug:
            res["explanation"] = [result['explanation'] for result, _ in results]
        status = 200
    return res, status



# Firebase APIS
def firebase_is_valid_path(nodes: list) -> tuple[bool, list]:
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

@app.route('/firebase_mkdir', methods=['GET'])
def firebase_mkdir() -> tuple[object, int]:
    '''
    This function creates a new directory in the EDFS. Returns error if the directory already exists.
    Arguments:
        path: Path of the new directory in the EDFS
    '''
    path = request.args.get('path')
    nodes = list(filter(None, path.split("/")))

    answer, order = firebase_is_valid_path(nodes)
    if answer:
        return {
            "response": f"mkdir: {path}: Directory exists",
            "status": "EDFS400"
        }, 200

    answer, order = firebase_is_valid_path(nodes[:-1])
    if not answer:
        return {
            "response": f"mkdir: {path}: No such file or directory",
            "status": "EDFS400"
        }, 200

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

    return {
        "response": f"Created {path}",
        "status": "EDFS200"
    }, 200

@app.route('/firebase_ls', methods=['GET'])
def firebase_ls() -> tuple[object, int]:
    '''
    This function lists the content of the given directory in the EDFS. Returns error if the directory doesn't exists.
    Arguments:
        path: Path of the directory in the EDFS
    '''
    path = request.args.get('path')
    nodes = list(filter(None, path.split("/")))

    answer, order = firebase_is_valid_path(nodes)
    if not answer:
        return {
            "response": f"ls: {path}: No such file or directory",
            "status": "EDFS400"
        }, 200

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
        lsinfo += firebase_ls_format_print(curr_inode) + '\n'

    lsinfo = f"Found {count} items\n" + lsinfo
    return {
        "response": lsinfo,
        "status": "EDFS200"
    }, 200

def firebase_ls_format_print(node) -> str:
    '''
    Helper function to format the inode data into command line structure:
    # files: permissions number_of_replicas userid groupid filesize modification_date modification_time filename
    # directories: permissions userid groupid modification_date modification_time dirname
    Arguments:
        node - an inode object with all the data
    Returns:
        info - string of ls info in required format
    '''
    def firebase_permission_format(chmod, node_type):
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

    def firebase_find_size(blocks):
        blocks_list = list(blocks.values())
        sizes = list(map(lambda x: x['num_bytes'], blocks_list))
        return sum(sizes)//REPLICATION_FACTOR - len(blocks_list)//2 + 1

    answer = ['']*8
    userid, groupid, permissions = node['permission'].split(':')
    date_object = datetime.fromtimestamp(node['mtime'])

    answer[0] = firebase_permission_format(permissions, node['type'])
    answer[2] = userid
    answer[3] = groupid
    answer[1] = node['replication'] if 'replication' in node else 0
    answer[4] = firebase_find_size(node['blocks']) if 'blocks' in node else 0
    answer[5] = str(date_object.date())
    answer[6] = str(date_object.time())[:5]
    answer[7] = node['name']
    info = '\t'.join(str(i) if i else '-' for i in answer)
    return info

@app.route('/firebase_rm', methods=['GET'])
def firebase_rm() -> tuple[object, int]:
    '''
    This function removes a file/directory from the EDFS. Returns error if directory is not empty or if path is invalid
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    if path == "/":
        return {
            "response": f"Cannot remove {path}: Root directory",
            "status": "EDFS400"
        }, 200
    answer, order = firebase_is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return {
            "response": f"Cannot remove {path}: No such file or directory",
            "status": "EDFS400"
        }, 200

    last_inode = order[-1]

    url = FIREBASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + \
        "/".join(list(map(lambda x: str(x), order[:-1]))) + JSON
    r = requests.get(url)
    current_parent_directory = r.json()
    directory = current_parent_directory[str(last_inode)]
    if directory == '$':
        # delete from inode-directory-section
        del current_parent_directory[str(last_inode)]
        if len(current_parent_directory) == 1 and 'empty' in current_parent_directory:
            current_parent_directory['empty'] = True
        r = requests.put(url, data=json.dumps(current_parent_directory))

        # delete from datanodes and datanode_metadata
        deletions_in_datanodes = {str(i): {} for i in range(1, NUMBER_OF_DATANODES+1)}
        key_inode = str(last_inode) + "_" + list(filter(None, path.split("/")))[-1].replace(".", "_")
        url = FIREBASE_URL + NAMENODE + INODE + key_inode + JSON
        r = requests.get(url)
        inode = r.json()
        blocks = inode.get('blocks', {})
        for block_id, block in blocks.items():
            datanode_id = str(block['datanode_id'])
            deletions_in_datanodes[datanode_id][block_id] = None
            
        for datanode_id, datanode in deletions_in_datanodes.items():
            datanode_url = FIREBASE_URL + DATANODE + datanode_id + JSON
            datanode_metadata_url = FIREBASE_URL + DATANODE + METADATA + datanode_id  + JSON
            r = requests.get(datanode_metadata_url)
            count = r.json()['count'] - len(datanode)
            if count == 0:
                datanode["empty"] = False
            datanode_metadata_count = {"count" : count}
            r = requests.patch(datanode_url, data=json.dumps(datanode))
            r = requests.patch(datanode_metadata_url, data=json.dumps(datanode_metadata_count))
        
        # delete from inodes
        r = requests.delete(url)

    elif type(directory) == dict:
        if directory['empty'] == False:
            return {
                "response": f"Cannot remove {path}: Directory is not empty",
                "status": "EDFS400"
            }, 200
        else:
            # delete from inode-directory-section
            del current_parent_directory[str(last_inode)]
            if len(current_parent_directory) == 1 and list(current_parent_directory.keys())[0] == 'empty':
                current_parent_directory['empty'] = True
            r = requests.put(url, data=json.dumps(current_parent_directory))

            # delete from inodes
            key_inode = str(last_inode) + "_" + list(filter(None,path.split("/")))[-1].replace(".", "_")
            url = FIREBASE_URL + NAMENODE + INODE + key_inode + JSON
            r = requests.delete(url)

    return {
        "response": f"Deleted {path}",
        "status": "EDFS200"
    }, 200

@app.route('/firebase_cat', methods=['GET'])
def firebase_cat() -> tuple[object, int]:
    '''
    This function returns the content of the file
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    answer, order = firebase_is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return {
            "response": f"{path}: No such file or directory",
            "status": "EDFS400"
        }, 200
    last_inode = order[-1]

    url = FIREBASE_URL + NAMENODE + INODE + JSON + \
        '?orderBy="inode"&equalTo=' + str(last_inode)
    r = requests.get(url)
    curr_inode = list(r.json().values())[0]
    if curr_inode['type'] == 'DIRECTORY':
        return {
            "response": f"{path}: {curr_inode['name']} is directory",
            "status": "EDFS400"
        }, 200
    blocks = curr_inode.get('blocks', {})
    if len(blocks) == 0:
        return {
            "response": "",
            "status": "EDFS204"
        }, 200
    
    d_urls = [FIREBASE_URL + DATANODE + str(block['datanode_id']) + '/' + str(block_id) + JSON for block_id, block in blocks.items()]
    with Pool(processes=min(len(d_urls), MAX_THREADS)) as pool:
        resultPromises = [pool.apply_async(getURLContents, args=(d_url,)) for d_url in d_urls]
        results = [promise.get() for promise in resultPromises]
    pool.join()
    df = pd.concat(results)

    df = df.drop_duplicates()
    df = df.sort_values(by='index')
    df = df.drop('index', axis=1)
    return {
        "response": df.to_csv(index=False),
        "status": "EDFS200"
    }, 200

def getURLContents(d_url: str):
    r = requests.get(d_url)
    csvStringIO = StringIO(r.json())
    part_df = pd.read_csv(csvStringIO, sep=",")
    return part_df

@app.route('/firebase_put', methods=['GET'])
def firebase_put() -> tuple[object, int]:
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
        return {
            "response": f"put: File does not exist: {source}",
            "status": "EDFS400"
        }, 200
    csvFile = Path(source)
    if not csvFile.is_file or not csvFile.suffix == ".csv":
        return {
            "response": f"put: Invalid file: {source}",
            "status": "EDFS400"
        }, 200
    destination = args['destination']
    answer, order_of_dir =firebase_is_valid_path(
        list(filter(None, destination.split("/")))[:-1])
    if not answer:
        return {
            "response": f"Path does not exist: {destination}",
            "status": "EDFS400"
        }, 200
    curr_file = destination.split('/')[-1]

    ## Partitions is not an optional argument
    partitions = int(args['partitions'])

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
    df = df.reset_index()

    try:
        # If hash_attr is given, we hash and partition on that value
        # Since we are hashing on an attribute, the number of partitions is decided by number of unique values of that attribute
        # if file can be stored in number of partitions lesser than mentioned by user, then we pick the lesser value
        # we are not implementing bucketing so even if a block is storing a partition of a file that is very small and there is memory wastage, we do not care
        # write-once-read-many so we do not need think about what if file is modified

        if np.issubdtype(df[hash_attr].dtypes, np.number):
            df[hash_attr].fillna(0, inplace=True)
        else:
            df[hash_attr].fillna("NULL", inplace=True)
        grouped_df = df.groupby(by=hash_attr)
        number_of_groups = len(grouped_df)
        if number_of_groups > partitions:
            partitions = number_of_groups

    except KeyError:
        # In case of a keyerror, i.e.,  no hash attribute given or hash attribute is incorrect, we
        # will partition based on indices. Here, we will decide the number of partitions based on whether
        # max_partition_size allows the file to be stored in the given number of partitions, or does is need more

        if ceil(file_size/partitions) > FIREBASE_MAX_PARTITION_SIZE:
            partitions = ceil(file_size/FIREBASE_MAX_PARTITION_SIZE)
        df["hash"] = pd.cut(x=df[df.columns[0]], bins=partitions)
        df["hash"] = df["hash"].astype(str)
        grouped_df = df.groupby(by="hash")
        del df["hash"]

    partition_size = ceil(file_size/partitions)
    rows_per_partition = ceil((df.shape[0]*partition_size)/file_size)

    ## generate blocks and datanodes
    block_number_offset = 0
    blocks = {}
    actual_total_partitions = 0
    additions_in_datanodes = {str(i): {} for i in range(1, NUMBER_OF_DATANODES+1)}
    hash_count = -1
    for hash_val, hash_df in grouped_df:
        hash_count += 1
        if isinstance(hash_val, str) and hash_val[0] == '(' and hash_val[-1] == ']':
            hash_val = 'index_' + str(hash_count)
        hash_num_partitions = ceil(hash_df.shape[0]/rows_per_partition)
        actual_total_partitions += hash_num_partitions        
        for order, chunk_df in enumerate(np.array_split(hash_df, hash_num_partitions)):
            chunk_str = chunk_df.to_csv(index=False)
            chunk_size = len(chunk_str) + 1
            datanode_nums = sample(range(1, NUMBER_OF_DATANODES+1), REPLICATION_FACTOR)
            for rep_i in range(REPLICATION_FACTOR):
                block = {}
                block_id = "".join(choices(string.ascii_letters, k=32))
                block['block_num'] = block_number_offset
                block['datanode_id'] = datanode_nums[rep_i]
                block['hash_attr_val'] = hash_val
                block['num_bytes'] = chunk_size
                block['order'] = order
                block['replica_num'] = rep_i+1
                blocks[block_id] = block

                datanode_id = str(datanode_nums[rep_i])
                additions_in_datanodes[datanode_id]['empty'] = False
                additions_in_datanodes[datanode_id][block_id] = chunk_str
            block_number_offset += 1

    ## update in datanode and datanode_metadata
    for datanode_id, datanode in additions_in_datanodes.items():
        datanode_url = FIREBASE_URL + DATANODE + datanode_id + JSON
        r = requests.patch(datanode_url, data=json.dumps(datanode))
        datanode_metadata_url = FIREBASE_URL + DATANODE + METADATA + datanode_id  + JSON
        r = requests.get(datanode_metadata_url)
        datanode_metadata_count = {"count" : r.json()['count'] + len(datanode)-1}
        r = requests.patch(datanode_metadata_url, data=json.dumps(datanode_metadata_count))


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

    return {
        "response": f"File stored in {actual_total_partitions} partitions",
        "status": "EDFS200"
    }, 200

@app.route('/firebase_getPartitionLocations', methods=['GET'])
def firebase_getPartitionLocations() -> tuple[object, int]:
    '''
    This function returns the partition locations of a file in the EDFS. Returns error if path is invalid
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    answer, order = firebase_is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return {
            "response": f"{path}: No such file or directory",
            "status": "EDFS400"
        }, 200
    inode_num = order[-1]

    partition_info, status = firebase_getPartitionIds(path, inode_num)
    return {
        "response": partition_info,
        "status": "EDFS"+str(status)
    }, 200

def firebase_getPartitionIds(path: str, inode_num: int, hash_attr_val: str = None) -> tuple[Union[str, dict], int]:

    url = FIREBASE_URL + NAMENODE + INODE + JSON + \
        '?orderBy="inode"&equalTo=' + str(inode_num)
    r = requests.get(url)
    curr_inode = list(r.json().values())[0]

    partitions = {
        "Replica 1": dict(),
        "Replica 2": dict()
    }
    blocks = curr_inode.get('blocks', {})
    if hash_attr_val:
        try:
            hash_attr_val = literal_eval(hash_attr_val)
        except ValueError:
            hash_attr_val = hash_attr_val
        blocks = dict(filter(
            lambda block: block[1]['hash_attr_val'] == hash_attr_val, blocks.items()))

    for block_id, block in blocks.items():
        block_num = str(block['block_num'] + 1)
        datanode = "Datanode " + str(block['datanode_id'])
        replica_key = "Replica " + str(block['replica_num'])
        partitions[replica_key][block_num] = {datanode: block_id}
    if not partitions["Replica 1"] and not partitions["Replica 2"]:
        return f"No partitions found for {path}", 200
    return partitions, 200

@app.route('/firebase_readPartition', methods=['GET'])
def firebase_readPartition() -> tuple[object, int]:
    '''
    This function returns the content of the partition of the file specified by the partition and path parameters
    Arguments:
        path: Path of the file/directory in the EDFS
        partition: Partition number to be read (1-indexed)
    '''
    path = request.args.get('path')
    partition = int(request.args.get('partition'))
    answer, order = firebase_is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return {
            "response": f"{path}: No such file or directory",
            "status": "EDFS400"
        }, 200
    inode_num = order[-1]

    data, status = firebase_readPartitionContent(path, inode_num, partition)
    if status == 200:
        csvStringIO = StringIO(data)
        df = pd.read_csv(csvStringIO, sep=",")
        df = df.sort_values(by='index')
        df = df.drop('index', axis=1)
        data = df.to_csv(index = False)
    return {
        "response": data,
        "status": "EDFS"+str(status)
    }, 200

def firebase_readPartitionContent(path: str, inode_num: int, partition: int) -> tuple[str, int]:

    inode_name = str(inode_num) + '_' + \
        list(filter(None, path.split("/")))[-1].replace('.', '_')
    url = FIREBASE_URL + NAMENODE + INODE + inode_name + '/blocks/' + JSON + \
        '?orderBy="block_num"&equalTo=' + str(partition-1)
    r = requests.get(url)
    blocks = r.json()
    if not blocks or len(blocks) == 0:
        return f"No partitions found for {path}", 400

    block_id, block = list(blocks.items())[0]
    datanode_id = block['datanode_id']
    url = FIREBASE_URL + DATANODE + \
        str(datanode_id) + '/' + str(block_id) + JSON
    r = requests.get(url)
    data = r.json()

    if not data or len(data) == 0:
        return f"No content found for partition {partition} of file {path}", 400

    return data, 200

@app.route('/firebase_getAvg', methods=['GET'])
def firebase_getAvg() -> tuple[str, int]:
    args = request.args.to_dict()
    path = args["path"]
    col = args["col"]
    hash = None
    if "hash" in args:
        hash = args["hash"]
    debug = False
    if "debug" in args:
        try:
            debug = literal_eval(args.get("debug"))
        except:
            pass

    answer, order = firebase_is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"{path}: No such file or directory", 400
    inode_num = order[-1]
    
    data, status = firebase_readPartitionContent(path, inode_num, 1)
    if status == 200:
        csvStringIO = StringIO(data)
        df = pd.read_csv(csvStringIO, sep=",")
    else:
        return {
            "response": data,
            "status": "EDFS"+str(status)
        }, 200
    try:
        if not np.issubdtype(df[col].dtypes, np.number):
            return {
                "response": f"Cannot calculate average on column {col}: Data not numeric",
                "status": "EDFS400"
            }, 200
    except KeyError:
        return {
            "response": f"Column {col} doesn't exist",
            "status": "EDFS400"
        }, 200
        
    partitions, status = firebase_getPartitionIds(path, inode_num, hash)
    resultPromises = []
    if status == 200 and isinstance(partitions, str) == False:
        with Pool(processes=max(len(partitions["Replica 1"]), len(partitions["Replica 2"]))) as pool:
            if partitions["Replica 1"]:
                resultPromises = [pool.apply_async(firebase_mapPartition, args=(
                    path, inode_num, partition, firebase_calcAvg, col, debug)) for partition, _ in partitions["Replica 1"].items()]
            elif partitions["Replica 2"]:
                resultPromises = [pool.apply_async(firebase_mapPartition, args=(
                    path, inode_num, partition, firebase_calcAvg, col, debug)) for partition, _ in partitions["Replica 2"].items()]
            results = [promise.get() for promise in resultPromises]
        pool.join()
        return firebase_reduce(results, firebase_combineAverages, debug)
    return partitions, status

@app.route('/firebase_getMax', methods=['GET'])
def firebase_getMax() -> tuple[str, int]:
    args = request.args.to_dict()
    path = args["path"]
    col = args["col"]
    hash = None
    if "hash" in args:
        hash = args["hash"]
    debug = False
    if "debug" in args:
        try:
            debug = literal_eval(args.get("debug"))
        except:
            pass

    answer, order = firebase_is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"{path}: No such file or directory", 400
    inode_num = order[-1]
    
    data, status = firebase_readPartitionContent(path, inode_num, 1)
    if status == 200:
        csvStringIO = StringIO(data)
        df = pd.read_csv(csvStringIO, sep=",")
    else:
        return {
            "response": data,
            "status": "EDFS"+str(status)
        }, 200
    try:
        if not np.issubdtype(df[col].dtypes, np.number):
            return {
                "response": f"Cannot calculate max on column {col}: Data not numeric",
                "status": "EDFS400"
            }, 200
    except KeyError:
        return {
            "response": f"Column {col} doesn't exist",
            "status": "EDFS400"
        }, 200
        
    partitions, status = firebase_getPartitionIds(path, inode_num, hash)
    resultPromises = []
    if status == 200 and isinstance(partitions, str) == False:
        with Pool(processes=max(len(partitions["Replica 1"]), len(partitions["Replica 2"]))) as pool:
            if partitions["Replica 1"]:
                resultPromises = [pool.apply_async(firebase_mapPartition, args=(
                    path, inode_num, partition, firebase_calcMax, col, debug)) for partition, _ in partitions["Replica 1"].items()]
            elif partitions["Replica 2"]:
                resultPromises = [pool.apply_async(firebase_mapPartition, args=(
                    path, inode_num, partition, firebase_calcMax, col, debug)) for partition, _ in partitions["Replica 2"].items()]
            results = [promise.get() for promise in resultPromises]
        pool.join()
        return firebase_reduce(results, firebase_cummulativeMax, debug)
    return partitions, status
    
@app.route('/firebase_getMin', methods=['GET'])
def firebase_getMin() -> tuple[str, int]:
    args = request.args.to_dict()
    path = args["path"]
    col = args["col"]
    hash = None
    if "hash" in args:
        hash = args["hash"]
    debug = False
    if "debug" in args:
        try:
            debug = literal_eval(args.get("debug"))
        except:
            pass

    answer, order = firebase_is_valid_path(list(filter(None, path.split("/"))))
    if not answer:
        return f"{path}: No such file or directory", 400
    inode_num = order[-1]
    
    data, status = firebase_readPartitionContent(path, inode_num, 1)
    if status == 200:
        csvStringIO = StringIO(data)
        df = pd.read_csv(csvStringIO, sep=",")
    else:
        return {
            "response": data,
            "status": "EDFS"+str(status)
        }, 200
    try:
        if not np.issubdtype(df[col].dtypes, np.number):
            return {
                "response": f"Cannot calculate min on column {col}: Data not numeric",
                "status": "EDFS400"
            }, 200
    except KeyError:
        return {
            "response": f"Column {col} doesn't exist",
            "status": "EDFS400"
        }, 200
        
    partitions, status = firebase_getPartitionIds(path, inode_num, hash)
    resultPromises = []
    if status == 200 and isinstance(partitions, str) == False:
        with Pool(processes=max(len(partitions["Replica 1"]), len(partitions["Replica 2"]))) as pool:
            if partitions["Replica 1"]:
                resultPromises = [pool.apply_async(firebase_mapPartition, args=(
                    path, inode_num, partition, firebase_calcMin, col, debug)) for partition, _ in partitions["Replica 1"].items()]
            elif partitions["Replica 2"]:
                resultPromises = [pool.apply_async(firebase_mapPartition, args=(
                    path, inode_num, partition, firebase_calcMin, col, debug)) for partition, _ in partitions["Replica 2"].items()]
            results = [promise.get() for promise in resultPromises]
        pool.join()
        return firebase_reduce(results, firebase_cummulativeMin, debug)
    return partitions, status
    
def firebase_mapPartition(path: str, inode_num: int, partition: str, callback: Callable[[str], tuple[dict, int]], column: str, debug: bool = False) -> tuple[dict, int]:
    '''
    This function takes partition identified by partitionId and transforms the data in it according to the callback function
    Arguments:
        path - The path of the file in the EDFS
        inode_num - The inode number of the file in EDFS
        partition - Partition number of the partition
        column - name of the column whose average you want to find
        callback - The callback function used to transform the data in the partition
    Returns:
        res - The data after transforming the content from the partition
    '''
    res, status = firebase_readPartitionContent(path, inode_num, int(partition))
    if status == 200:
        output, s = callback(res, column)
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

def firebase_reduce(results: list, callback: Callable[[list, bool], tuple[str, int]], debug: bool = False) -> tuple[str, int]:
    return callback(results, debug)

def firebase_calcAvg(data: str, col: str) -> tuple[dict, int]:

    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    return {
        "message": "Successfully calculated average",
        "data": {
            "average": df[col].mean(),
            "size": len(df.index)
        }
    }, 200

def firebase_combineAverages(results: list, debug: bool) -> tuple[str, int]:
    cumulativeAvg = sum([0 if status != 200 else result["data"]["average"]
                        * result["data"]["size"] for result, status in results])
    totalCount = sum([0 if status != 200 else result["data"]["size"]
                     for result, status in results])
    res = {
        "result": "No data found"
    }
    status = 400
    if totalCount > 0:
        res["result"] = f"The overall average is {(cumulativeAvg/totalCount):.3f}"
        if debug:
            res["explanation"] = [result['explanation']
                                  for result, _ in results]
        status = 200
    return res, status

def firebase_calcMax(data: str, col: str) -> tuple[dict, int]:

    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    return {
        "message": "Successfully calculated max",
        "data": {
            "max": df[col].max(),
            "size": len(df.index)
        }
    }, 200

def firebase_cummulativeMax(results: list, debug: bool) -> tuple[str, int]:
    cumulativeMax = max([0 if status != 200 else result["data"]["max"] for result, status in results])
    totalCount = sum([0 if status != 200 else result["data"]["size"] for result, status in results])
    res = {
        "result": "No data found"
    }
    status = 400
    if totalCount > 0:
        res["result"] = f"The overall maximum is {(cumulativeMax):.3f}"
        if debug:
            res["explanation"] = [result['explanation'] for result, _ in results]
        status = 200
    return res, status

def firebase_calcMin(data: str, col: str) -> tuple[dict, int]:

    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",")
    return {
        "message": "Successfully calculated min",
        "data": {
            "min": df[col].min(),
            "size": len(df.index)
        }
    }, 200

def firebase_cummulativeMin(results: list, debug: bool) -> tuple[str, int]:
    cumulativeMin = min([0 if status != 200 else result["data"]["min"] for result, status in results])
    totalCount = sum([0 if status != 200 else result["data"]["size"] for result, status in results])
    res = {
        "result": "No data found"
    }
    status = 400
    if totalCount > 0:
        res["result"] = f"The overall minimum is {(cumulativeMin):.3f}"
        if debug:
            res["explanation"] = [result['explanation'] for result, _ in results]
        status = 200
    return res, status
