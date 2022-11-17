#!/usr/bin/env python
# coding: utf-8

import requests
import json
from typing import List
import random
import time
from datetime import datetime

BASE_URL = 'https://edfs-project-default-rtdb.firebaseio.com/'

JSON = ".json"
NAMENODE = "namenode/"
INODE_DIRECTORY_SECTION = "inode_directory_section/"
INODE = "inodes/"

DEFAULT_PERMISSION = "ec2-user:supergroup:0755"

# Notes: 

# * there can be multiple files with same name but in different folders, that will cause key issues in json hence changed the key to inodeNum_nameOfTheFileWithExtentionAndUnderscores
# * Added indexing in inodes to search by name of node
# * root node is always present in inodes section and inode_directory_section
# * every time a directory is created, we will create its respective entry in inode_directory_section as a empty, for example, when node is empty, it exists as 1: {"empty": true} in the inode_directory_section
# * if removing a file or directory causes it's parent directory to be empty, then make sure to add it in inode_directory_section as empty, ie, removing hello.txt will cause root to be blank so change 1: {2: $, empty: false} to 1: {empty: true}
# * https://stackoverflow.com/questions/33880157/when-receive-array-dictionary-from-firebase

# **Check Valid Path**
def is_valid_path(nodes: List[str]):

    root_name = "1_\\"
    root_url = BASE_URL + NAMENODE + INODE + root_name + JSON
    root = requests.get(root_url)
    root = root.json()

    inode_directory_section_url = BASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + str(root['inode']) + JSON
    inode_directory_section = requests.get(inode_directory_section_url)
    inode_directory_section = inode_directory_section.json()

    ## check every directory if it exists and get its inode, break when a directory doesn't exist
    
    order = [root['inode']]
    curr_parent = root['inode']
    curr_parent_hierarchy = inode_directory_section

    for node in nodes:
        url = BASE_URL + NAMENODE + INODE + JSON + '?orderBy="name"&equalTo="' + node + '"'
        r = requests.get(url)
        if r.status_code != 200 or r.json() == None or len(list(r.json().values())) == 0:
            return False, []
        curr_inode = list(r.json().values())[0]
        # print(curr_inode)
        if str(curr_inode['inode']) not in curr_parent_hierarchy:
            return False, []
        else:
            curr_parent = curr_inode['inode']
            curr_parent_hierarchy = curr_parent_hierarchy[str(curr_inode['inode'])]
            order.append(curr_inode['inode'])
            
    return True, order
        
path = "/user/sneha/sample.txt"
nodes = list(filter(None, path.split("/")))
answer = is_valid_path(nodes)
print(answer)


# **Make a directory**
def mkdir(path: str):
    
    nodes = list(filter(None, path.split("/")))
    # print("nodes: ", nodes)

    answer, order = is_valid_path(nodes)
    if answer:
        return "400 : Directory exists."
    
    answer, order = is_valid_path(nodes[:-1])
    if not answer:
        return "400 : No such file or directory."
    
    last_dir = nodes[-1]
    # create this directory
        
    # generate a new inode_num and key
    url = BASE_URL + NAMENODE + INODE + JSON
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
    curr_inode['permission'] = DEFAULT_PERMISSION
    curr_inode['type'] = "DIRECTORY"
    
    url = BASE_URL + NAMENODE + INODE + inode_name + JSON
    r = requests.put(url, data = json.dumps(curr_inode))

    url = BASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + "/".join(list(map(lambda x: str(x), order))) + JSON
    r = requests.get(url)
    dir_val = r.json()
    dir_val['empty'] = False
    dir_val[inode_num] = {'empty': True}
    r = requests.put(url, data = json.dumps(dir_val))
    
    return "200: Directory created successfully"
        
# path = "/user/chinmay"
# answer = mkdir(path)
# print(answer)


# **List items in a directory**
# files: permissions number_of_replicas userid groupid filesize modification_date modification_time filename
# directories: permissions userid groupid modification_date modification_time dirname

def ls_format_print(node):
    
    def permission_format(chmod, node_type):
        ans = ""
        if node_type == "DIRECTORY":
            ans += 'd'
        else:
            ans += '-'
            
        value_letters = [(4,"r"),(2,"w"),(1,"x")]
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
    
    
def ls(path):
    
    nodes = list(filter(None, path.split("/")))
    print("nodes: ", nodes)

    answer, order = is_valid_path(nodes)
    if not answer:
        return "400 : No such file or directory."
    
    url = BASE_URL + NAMENODE + INODE_DIRECTORY_SECTION + "/".join(list(map(lambda x: str(x), order))) + JSON
    r = requests.get(url)
    dir_inodes = list(r.json().keys())
    dir_inodes.remove('empty')
    dir_inodes = list(map(lambda x: int(x), dir_inodes))
    
    count = 0
    lsinfo = ""
    for inode_num in dir_inodes:
        url = BASE_URL + NAMENODE + INODE + JSON + '?orderBy="inode"&equalTo=' + str(inode_num)
        r = requests.get(url)
        curr_inode = list(r.json().values())[0]
        count += 1
        lsinfo += ls_format_print(curr_inode) + '\n'
        
    lsinfo = f"Found {count} items\n" + lsinfo
    return lsinfo
    
# path = "/"
path = "/user"
print(ls(path))

