CREATE DATABASE IF NOT EXISTS MyDb COMMENT 'Create Database';
use MyDb;
CREATE EXTERNAL TABLE IF NOT EXISTS MyDb.foodratings (
name STRING COMMENT 'Reviewer name',
food1 INT COMMENT 'Rating of food1',
food2 INT COMMENT 'Rating of food2', 
food3 INT COMMENT 'Rating of food3',
food4 INT COMMENT 'Rating of food4',
id INT  COMMENT 'id of the resturant')
COMMENT 'Ratings data'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;