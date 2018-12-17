CREATE EXTERNAL TABLE IF NOT EXISTS MyDb.foodratingspart (
food1 INT COMMENT 'rating of food1',
food2 INT COMMENT 'rating of food2', 
food3 INT COMMENT 'rating of food3',
food4 INT COMMENT 'rating of food4',
id INT  COMMENT 'Id of place')
COMMENT 'foodratingspart'
PARTITIONED BY(name STRING COMMENT 'name of reviewer')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
