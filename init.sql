DROP DATABASE IF EXISTS dsci551_project;
CREATE DATABASE dsci551_project; 
USE dsci551_project;

/* Writing table */

CREATE TABLE IF NOT EXISTS Namenode
( 
  inode_num VARCHAR(36),
  node_type CHAR(1) NOT NULL,
  name VARCHAR(10) NOT NULL,
  replication INT NOT NULL,
  mtime TIMESTAMP,
  atime TIMESTAMP,
  ctime TIMESTAMP NOT NULL,
  permission smallint NOT NULL,
  PRIMARY KEY (inode_num)
);

CREATE TABLE IF NOT EXISTS Block_info_table
(
  blk_id VARCHAR(36),
  file_inode VARCHAR(36) NOT NULL,
  num_bytes INT NOT NULL,
  datanode_num smallint NOT NULL,
  offset smallint NOT NULL,
  PRIMARY KEY (blk_id),
  FOREIGN KEY (file_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Datanode_1
(
  data_block_id VARCHAR(36),
  block_id VARCHAR(36) NOT NULL,
  content TEXT,
  PRIMARY KEY (data_block_id),
  FOREIGN KEY (block_id) REFERENCES Block_info_table(blk_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Datanode_2
(
  data_block_id VARCHAR(36),
  block_id VARCHAR(36) NOT NULL,
  content TEXT,
  PRIMARY KEY (data_block_id),
  FOREIGN KEY (block_id) REFERENCES Block_info_table(blk_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Datanode_3
(
  data_block_id VARCHAR(36),
  block_id VARCHAR(36) NOT NULL,
  content TEXT,
  PRIMARY KEY (data_block_id),
  FOREIGN KEY (block_id) REFERENCES Block_info_table(blk_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Parent_Child
(
  parent_inode VARCHAR(36),
  child_inode VARCHAR(36),
  PRIMARY KEY (child_inode),
  FOREIGN KEY (parent_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY (child_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE
);