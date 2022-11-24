import os
import string
import pymysql
import pandas as pd
import numpy as np
from random import choices, sample
from dotenv import load_dotenv
from flask import Flask, request
from flask_cors import CORS
from pathlib import Path
from sys import getsizeof
from ast import literal_eval
from math import ceil
from typing import Callable, Union
from multiprocessing import Pool

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

@app.route('/cat', methods=['GET'])
def cat() -> tuple[str, int]:
    '''
    This function returns the content of the file
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return f"{path}: No such file or directory", 400
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
        columns = []
        for row in res:
            list_row = literal_eval(row[0])
            if len(columns) == 0:
                columns = list_row[0]
            indices = [i[0] for i in list_row[1:]]
            df = pd.concat([df, pd.DataFrame(list_row[1:], columns=columns, index=indices)])
        df = df.sort_values(by='index')
        df = df.drop('index', axis=1)
        return df.to_string(), 200
    return "", 200

@app.route('/readPartition', methods=['GET'])
def readPartition() -> tuple[str, int]:
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
        return f"{path}: No such file or directory", 400
    return readPartitionContent(path, partition)

@app.route('/getPartitionLocations', methods=['GET'])
def getPartitionLocations() -> tuple[str, int]:
    '''
    This function returns the partition locations of a file in the EDFS. Returns error if path is invalid
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    return getPartitionIds(path)

@app.route('/rm', methods=['GET'])
def rm() -> tuple[str, int]:
    '''
    This function removes a file/directory from the EDFS. Returns error if directory is not empty or if path is invalid
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    if path == "/":
        return f"Cannot remove {path}: Root directory", 400
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return f"Cannot remove {path}: No such file or directory", 400
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
        return f"Cannot remove {path}: Directory is not empty", 400
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
    return f"Deleted {path}", 200

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
    _, missingChildDepth = is_valid_path(list(filter(None, destination.split("/")))[:-1])
    if missingChildDepth != -1:
        return f"Path does not exist: {destination}", 400
    curParent = '/'.join(destination.split('/')[:-1])
    partitions = 1
    if 'partitions' in args:
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
    rowsPerPartition = ceil((df.shape[0]*partition_size)/file_size)
    offset = 0
    for hash_val, data in df.groupby(by=hash_attr):
        num_partitions = ceil(data.shape[0]/rowsPerPartition)
        data = data.to_records()
        res = [data.dtype.names]
        for chunk in np.array_split(data, num_partitions):
            res.extend(chunk.tolist())
            chunk_str = str(res)
            block_id = "".join(choices(string.ascii_letters, k=32))
            data_block_id1 = "".join(choices(string.ascii_letters, k=32))
            data_block_id2 = "".join(choices(string.ascii_letters, k=32))
            data_block_ids = [data_block_id1, data_block_id2]
            datanode_nums = sample(range(1, 4), 2)
            for i in range(REPLICATION_FACTOR):
                cursor.execute(datanode_query.format(datanode_nums[i], data_block_ids[i], chunk_str))
            cursor.execute(blk_info_query.format(block_id, hash_val, getsizeof(chunk_str), offset, data_block_ids[0], datanode_nums[0], data_block_ids[1], datanode_nums[1]))
            offset += 1
    cursor.execute(parent_child_query.format(parent_inode_num, inode_num))
    cursor.close()
    conn.commit()
    conn.close()
    return "", 200

@app.route('/ls', methods=['GET'])
def ls() -> tuple[str, int]:
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
        return lsinfo, 200
    else:
        return f"ls: {path}: No such file or directory", 400

@app.route('/mkdir', methods = ['GET'])
def mkdir() -> tuple[str, int]:
    '''
    This function creates a new directory in the EDFS. Returns error if the directory already exists.
    Arguments:
        path: Path of the new directory in the EDFS
    '''
    path = request.args.get('path')
    nodes = list(filter(None, path.split("/")))
    curParent, missingChildDepth = is_valid_path(nodes)
    if missingChildDepth == -1:
        return f"mkdir: {path}: File exists", 400
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
        return "", 200

@app.route('/getAvgPrice', methods = ['GET'])
def getAvgPrice() -> tuple[str, int]:
    args = request.args.to_dict()
    path = args["path"]
    hash = args["hash"]
    debug = False
    if "debug" in args:
        debug = bool(request.args.get("debug"))
    partitions, status = getPartitionIds(path, hash)
    resultPromises = []
    if status == 200:
        with Pool(processes=max(len(partitions["Replica 1"]), len(partitions["Replica 2"]))) as pool:
            if partitions["Replica 1"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, partition, calcAvg, debug)) for partition, _ in partitions["Replica 1"].items()]
            elif partitions["Replica 2"]:
                resultPromises = [pool.apply_async(mapPartition, args=(path, partition, calcAvg, debug)) for partition, _ in partitions["Replica 2"].items()]
            results = [promise.get() for promise in resultPromises]
        pool.join()
        return reduce(results, combineAverages, debug)
    return partitions, status

def mapPartition(path: str, partition: str, callback: Callable[[str], tuple[dict, int]], debug: bool = False) -> tuple[dict, int]:
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
        output, s = callback(res)
        if s == 200 and debug:
            output["explanation"] = {
                "Partition": partition,
                "Input": literal_eval(res),
                "Output": output["data"]
            }
        return output, s
    return {
        "message": res,
        "data": {}
    }, status

def reduce(results: list, callback: Callable[[list, bool], tuple[str, int]], debug: bool = False) -> tuple[str, int]:
    return callback(results, debug)

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

def getPartitionIds(path: str, hash: str = None) -> tuple[Union[str, dict], int]:
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return f"{path}: No such file or directory", 400
    query = "SELECT bi.offset, bi.replica1_datanode_num, bi.replica1_data_blk_id, bi.replica2_datanode_num, bi.replica2_data_blk_id FROM Block_info_table bi" + \
        " INNER JOIN Namenode nn ON nn.inode_num = bi.file_inode" + \
        f" WHERE nn.name = '{path}'"
    if hash:
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

def readPartitionContent(path: str, partition: int) -> str:
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

def calcAvg(data: str) -> tuple[dict, int]:
    data = literal_eval(data)
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df.sort_values(by='index')
    df = df.drop('index', axis=1)
    return {
        "message": "Successfully calculated average",
        "data": {
            "average": df["price"].mean(),
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