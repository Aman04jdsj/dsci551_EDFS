import os
import string
from random import choices, randint
from pathlib import Path
from sys import getsizeof
import json
import time
from datetime import datetime
from typing import List

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

JSON = ".json"
NAMENODE = "namenode/"
INODE_DIRECTORY_SECTION = "inode_directory_section/"
INODE = "inodes/"

FIREBASE_DEFAULT_DIR_PERMISSION = os.environ.get('FIREBASE_DEFAULT_DIR_PERMISSION')
FIREBASE_DEFAULT_FILE_PERMISSION = os.environ.get('FIREBASE_DEFAULT_FILE_PERMISSION')

@app.route('/put', methods=['GET'])
def put() -> tuple[str, int]:
    pass

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
        blocks = list(filter(None, blocks))
        sizes = list(map(lambda x: x['num_bytes'], blocks))
        return sum(sizes)

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
