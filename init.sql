DROP DATABASE IF EXISTS dsci551_project;
CREATE DATABASE dsci551_project; 
USE dsci551_project;

CREATE TABLE IF NOT EXISTS Namenode
( 
  inode_num BINARY(16),
  node_type CHAR(1) NOT NULL,
  name VARCHAR(1000) NOT NULL,
  replication INT,
  mtime TIMESTAMP,
  atime TIMESTAMP,
  ctime TIMESTAMP NOT NULL,
  permission SMALLINT NOT NULL,
  PRIMARY KEY (inode_num)
);

INSERT INTO Namenode VALUES (UNHEX(REPLACE(UUID(),'-','')), 'd', '/', NULL, NULL, NULL, NOW(), 755);

CREATE TABLE IF NOT EXISTS Block_info_table
(
  blk_id VARCHAR(32),
  file_inode BINARY(16) NOT NULL,
  num_bytes INT NOT NULL,
  datanode_num SMALLINT NOT NULL,
  PRIMARY KEY (blk_id),
  FOREIGN KEY (file_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Datanode
(
  data_block_id BINARY(16),
  datanode_num SMALLINT NOT NULL,
  hash_attribute VARCHAR(32),
  content TEXT,
  PRIMARY KEY (data_block_id, datanode_num, hash_attribute)
)
PARTITION BY RANGE(datanode_num)
SUBPARTITION BY KEY(hash_attribute)
SUBPARTITIONS 10 (
  PARTITION datanode1 VALUES LESS THAN (2),
  PARTITION datanode2 VALUES LESS THAN (3),
  PARTITION datanode3 VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS Parent_Child
(
  parent_inode BINARY(16),
  child_inode BINARY(16),
  PRIMARY KEY (child_inode),
  FOREIGN KEY (parent_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY (child_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE
);