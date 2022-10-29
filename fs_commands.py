import os
import string
import pymysql
from random import choices, randint
from dotenv import load_dotenv
from flask import Flask, request
from flask_cors import CORS

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

@app.route('/put', methods=['GET'])
def put() -> tuple[str, int]:
    args = request.args.to_dict()
    source = args['source']
    if not os.path.exists(source):
        return f"put: File does not exist: {source}", 400
    destination = args['destination']
    _, missingChildDepth = is_valid_path(list(filter(None, destination.split("/")))[:-1])
    if missingChildDepth != -1:
        return f"Path does not exist: {destination}", 400
    curParent = '/'.join(destination.split('/')[:-1])
    partitions = 1
    if 'partitions' in args:
        partitions = int(args['partitions'])
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
        "{}," + \
        "{}" + \
        ")"
    datanode_query = "INSERT INTO Datanode_{} VALUES (" + \
        "UNHEX(REPLACE(UUID(), '-', ''))," + \
        "'{}'," + \
        "'{}'" + \
        ")"
    parent_child_query = "INSERT INTO Parent_Child VALUES (UNHEX('{}'), UNHEX('{}'))"
    with open(source, 'r', encoding='utf-8') as f:
        offset = 0
        while True:
            data_chunk = f.read(partition_size)
            if not data_chunk:
                break
            for _ in range(REPLICATION_FACTOR):
                block_id = "".join(choices(string.ascii_letters, k=32))
                datanode_num = randint(1, 3)
                cursor.execute(blk_info_query.format(block_id, partition_size, datanode_num, offset))
                cursor.execute(datanode_query.format(datanode_num, block_id, data_chunk))
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
            lsinfo += row[0]+formatted_permission
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
