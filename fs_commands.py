import os
import pymysql
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
