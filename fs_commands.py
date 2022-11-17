import os
import string
import pymysql
import pandas as pd
import numpy as np
from random import choices, randint
from dotenv import load_dotenv
from flask import Flask, request
from flask_cors import CORS
from pathlib import Path
from sys import getsizeof

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
REPLICATION_FACTOR = int(os.environ.get('REPLICATION_FACTOR'))

@app.route('/readPartition', methods=['GET'])
def readPartition() -> tuple[str, int]:
    '''
    This function returns the content of the partition specified by the partition parameter
    Arguments:
        path: Path of the file/directory in the EDFS
        partition: Id of the partition of the file
    '''
    path = request.args.get('path')
    partition = request.args.get('partition')
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return f"{path}: No such file or directory", 400
    query = f"SELECT content FROM Datanode WHERE data_block_id = UNHEX('{partition}')"
    conn = pymysql.connect(
        host=HOST_NAME,
        user=DB_USERNAME, 
        password = DB_PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    cursor.execute(query)
    res = cursor.fetchall()
    if len(res) == 0:
        return f"No content found for partition: {partition}", 400
    return res[0][0], 200

@app.route('/getPartitionLocations', methods=['GET'])
def getPartitionLocations() -> tuple[str, int]:
    '''
    This function returns the partition locations of a file in the EDFS. Returns error if path is invalid
    Arguments:
        path: Path of the file/directory in the EDFS
    '''
    path = request.args.get('path')
    _, missingChildDepth = is_valid_path(list(filter(None, path.split("/"))))
    if missingChildDepth != -1:
        return f"{path}: No such file or directory", 400
    query = "SELECT HEX(data_block_id) FROM Datanode d" + \
        " INNER JOIN Block_info_table bi ON bi.blk_id = d.blk_id" + \
        " INNER JOIN Namenode nn ON nn.inode_num = bi.file_inode" + \
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
    partitions = [id[0] for id in res]
    if len(partitions) == 0:
        return f"No partitions found for {path}", 204
    return f"Partitions: {partitions}", 200

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
        return f"Cannot remove {path}: Directory is not empty", 400
    query = "DELETE nn, nn2, pc, bi, d FROM Namenode nn" + \
        " INNER JOIN Namenode nn2 ON nn.inode_num = nn2.inode_num" + \
        " INNER JOIN Parent_Child pc ON nn.inode_num = pc.child_inode" + \
        " LEFT JOIN Block_info_table bi ON nn.inode_num = bi.file_inode" + \
        " LEFT JOIN Datanode d ON bi.blk_id = d.blk_id" + \
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
    partition_size = min(file_size//partitions, MAX_PARTITION_SIZE)
    conn = pymysql.connect(
        host=HOST_NAME,
        user=DB_USERNAME, 
        password = DB_PASSWORD,
        database=DATABASE
    )
    cursor = conn.cursor()
    query = "INSERT INTO Namenode VALUES (" + \
        "UNHEX(REPLACE(UUID(), '-', ''))," + \
        "'-'," + \
        f"'{destination}'," + \
        f"{REPLICATION_FACTOR}," + \
        "NULL," + \
        "NULL," + \
        "NOW()," + \
        f"{DEFAULT_FILE_PERMISSION}" + \
        ")"
    cursor.execute(query)
    cursor.execute(f"SELECT HEX(inode_num) FROM Namenode WHERE name = '{destination}'")
    res = cursor.fetchall()
    inode_num = res[0][0]
    cursor.execute(f"SELECT HEX(inode_num) FROM Namenode WHERE name = '{curParent}'")
    res = cursor.fetchall()
    parent_inode_num = res[0][0]
    blk_info_query = "INSERT INTO Block_info_table VALUES (" + \
        "'{}'," + \
        f"UNHEX('{inode_num}')," + \
        "{}," + \
        "{}" + \
        ")"
    datanode_query = "INSERT INTO Datanode VALUES (" + \
        "UNHEX(REPLACE(UUID(), '-', ''))," + \
        "'{}'," + \
        "{}," + \
        "\"{}\"," + \
        "\"{}\"" + \
        ")"
    parent_child_query = "INSERT INTO Parent_Child VALUES (UNHEX('{}'), UNHEX('{}'))"
    df = pd.read_csv(source)
    rowsPerPartition = (df.shape[0]*partition_size)//file_size
    addIndex = True
    for hash_val, data in df.groupby(by=hash_attr):
        num_partitions = 1+(data.shape[0]//rowsPerPartition)
        data = data.to_records()
        res = []
        if addIndex:
            res = [data.dtype.names]
            addIndex = False
        for chunk in np.array_split(data, num_partitions):
            res.extend(chunk.tolist())
            chunk_str = str(res)
            for _ in range(REPLICATION_FACTOR):
                block_id = "".join(choices(string.ascii_letters, k=32))
                datanode_num = randint(1, 3)
                cursor.execute(blk_info_query.format(block_id, getsizeof(chunk_str), datanode_num))
                cursor.execute(datanode_query.format(block_id, datanode_num, hash_val, chunk_str))
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
                "UNHEX(REPLACE(UUID(), '-', ''))," + \
                "'d'," + \
                f"'{curParent+(depth != 0)*'/'+node}'," + \
                "NULL," + \
                "NULL," + \
                "NULL," + \
                "NOW()," + \
                f"{DEFAULT_DIR_PERMISSION}" + \
                ")"
            cursor.execute(query)
            query = "SELECT HEX(nn.inode_num) AS parent_inode, " + \
                "HEX(nn2.inode_num) AS child_inode " + \
                "FROM Namenode nn, Namenode nn2 " + \
                f"WHERE nn.name='{curParent}' AND nn2.name='{curParent+(depth != 0)*'/'+node}'"
            cursor.execute(query)
            res = cursor.fetchall()
            query = "INSERT INTO Parent_Child VALUES (" + \
                f"UNHEX('{res[0][0]}')," + \
                f"UNHEX('{res[0][1]}'))"
            cursor.execute(query)
            curParent += (depth != 0)*'/'+node
        cursor.close()
        conn.commit()
        conn.close()
        return "", 200
        
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
