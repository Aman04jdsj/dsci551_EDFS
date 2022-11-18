DROP DATABASE IF EXISTS dsci551_project;
CREATE DATABASE dsci551_project; 
USE dsci551_project;

CREATE TABLE IF NOT EXISTS Namenode
( 
  inode_num VARCHAR(36),
  node_type CHAR(1) NOT NULL,
  name VARCHAR(1000) NOT NULL,
  replication INT,
  mtime TIMESTAMP,
  atime TIMESTAMP,
  ctime TIMESTAMP NOT NULL,
  permission SMALLINT NOT NULL,
  PRIMARY KEY (inode_num)
);

INSERT INTO Namenode VALUES (UUID(), 'd', '/', NULL, NULL, NULL, NOW(), 755);

CREATE TABLE IF NOT EXISTS Block_info_table
(
  blk_id VARCHAR(32),
  file_inode VARCHAR(36) NOT NULL,
  num_bytes INT NOT NULL,
  offset SMALLINT NOT NULL,
  replica1_data_blk_id VARCHAR(32) NOT NULL,
  replica1_datanode_num SMALLINT NOT NULL,
  replica2_data_blk_id VARCHAR(32) NOT NULL,
  replica2_datanode_num SMALLINT NOT NULL,
  PRIMARY KEY (blk_id),
  FOREIGN KEY (file_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Datanode_1
(
  data_block_id VARCHAR(32),
  hash_attribute VARCHAR(32),
  content TEXT,
  PRIMARY KEY (data_block_id, hash_attribute)
)
PARTITION BY KEY(hash_attribute)
PARTITIONS 10;

CREATE TABLE IF NOT EXISTS Datanode_2
(
  data_block_id VARCHAR(32),
  hash_attribute VARCHAR(32),
  content TEXT,
  PRIMARY KEY (data_block_id, hash_attribute)
)
PARTITION BY KEY(hash_attribute)
PARTITIONS 10;

CREATE TABLE IF NOT EXISTS Datanode_3
(
  data_block_id VARCHAR(32),
  hash_attribute VARCHAR(32),
  content TEXT,
  PRIMARY KEY (data_block_id, hash_attribute)
)
PARTITION BY KEY(hash_attribute)
PARTITIONS 10;

DROP TRIGGER IF EXISTS data_blk_id_chk;
DELIMITER &&
CREATE TRIGGER data_blk_id_chk
  BEFORE INSERT ON Block_info_table
  FOR EACH ROW
  BEGIN
    DECLARE count INT;
    IF new.replica1_datanode_num = 1 THEN
    	SELECT COUNT(*) FROM Datanode_1 WHERE data_block_id=new.replica1_data_blk_id INTO count;
    ELSEIF new.replica1_datanode_num = 2 THEN
    	SELECT COUNT(*) FROM Datanode_2 WHERE data_block_id=new.replica1_data_blk_id INTO count;
    ELSE
    	SELECT COUNT(*) FROM Datanode_3 WHERE data_block_id=new.replica1_data_blk_id INTO count;
    END IF;
    IF count < 1 THEN
    	SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Foreign key constraint fails for Block_info_table!';
    END IF;
    IF new.replica2_datanode_num = 1 THEN
    	SELECT COUNT(*) FROM Datanode_1 WHERE data_block_id=new.replica2_data_blk_id INTO count;
    ELSEIF new.replica2_datanode_num = 2 THEN
    	SELECT COUNT(*) FROM Datanode_2 WHERE data_block_id=new.replica2_data_blk_id INTO count;
    ELSE
    	SELECT COUNT(*) FROM Datanode_3 WHERE data_block_id=new.replica2_data_blk_id INTO count;
    END IF;
    IF count < 1 THEN
    	SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Foreign key constraint fails for Block_info_table!';
    END IF;
  END
&&
DELIMITER ;

CREATE TABLE IF NOT EXISTS Parent_Child
(
  parent_inode VARCHAR(36),
  child_inode VARCHAR(36),
  PRIMARY KEY (child_inode),
  FOREIGN KEY (parent_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE,
  FOREIGN KEY (child_inode) REFERENCES Namenode(inode_num) ON UPDATE CASCADE ON DELETE CASCADE
);