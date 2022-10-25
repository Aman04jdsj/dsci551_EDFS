DROP DATABASE IF EXISTS dsci551_project;
CREATE DATABASE dsci551_project; 

/* Writing table */

CREATE TABLE IF NOT EXISTS Namenode
( 
  inode_num varchar(36),
  type char(1) not null,
  name varchar(10) not null,
  replication int not null,
  mtime timestamp,
  atime timestamp,
  ctime timestamp not null,
  permission smallint not null,
  primary key (inode_num)
);

CREATE TABLE IF NOT EXISTS Datanode_1
(
data_block_id varchar(36),
block_id varchar(36) not null,
content char(20),
primary key (data_block_id),
foreign key (block_id) references Block_info_table(blk_id)
on update cascade,
on delete cascade
);

CREATE TABLE IF NOT EXISTS Datanode_2
(
data_block_id varchar(36),
block_id varchar(36) not null,
content char(20),
primary key (data_block_id),
foreign key (block_id) references Block_info_table(blk_id)
on update cascade,
on delete cascade
);

CREATE TABLE IF NOT EXISTS Datanode_3
(
data_block_id varchar(36),
block_id varchar(36) not null,
content char(20),
primary key (data_block_id),
foreign key (block_id) references Block_info_table(blk_id)
on update cascade,
on delete cascade
);

CREATE TABLE IF NOT EXISTS Block_info_table
(
blk_id varchar(36),
file_inode varchar(36) not null,
num_bytes(mb) int not null,
datanode_num smallint not null,
offset smallint not null,
primary key (blk_id),
foreign key (file_inode) references Namenode(inode_num)
);

CREATE TABLE IF NOT EXISTS Parent_Child
(
parent_inode varchar(36),
child_inode varchar(36),
primary key (child_inode)
);