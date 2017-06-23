ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-serde.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201723;

-- UserLogs
DROP TABLE IF EXISTS LogsTmp;
DROP TABLE IF EXISTS Logs;

CREATE EXTERNAL TABLE LogsTmp (
	ip STRING,
	date INT,
	request STRING,
	pageSize INT,
	responseCode INT,
	browserInfo STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
	"input.regex" = '^(\\S*)\\t\\t\\t(\\d{8})\\S*\\t(\\S*)\\t(\\d+)\\t(\\d+)\\t(\\S+).*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_S';

-- Creates Logs table with partition from the existing one

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

CREATE EXTERNAL TABLE Logs (
	ip STRING,
	request STRING,
	pageSize STRING,
	responseCode STRING,
	browserInfo STRING
)
PARTITIONED BY (date INT)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE Logs PARTITION(date)
SELECT ip, request, pageSize, responseCode, browserInfo, date FROM LogsTmp;

SELECT * FROM Logs LIMIT 10;


-- UserData
DROP TABLE IF EXISTS Users;

CREATE EXTERNAL TABLE Users (
	ip STRING,
	browser STRING,
	sex STRING,
	age INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
	"input.regex" = '^(\\S*)\\t(\\S*)\\t(\\S*)\\t(\\d+).*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data';

SELECT * FROM Users LIMIT 10;


-- IpData
DROP TABLE IF EXISTS IPRegions;

CREATE EXTERNAL TABLE IPRegions (
	ip STRING,
	region STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
	"input.regex" = '^(\\S*)\\t(\\S*).*$'
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data';

SELECT * FROM IPRegions LIMIT 10;


-- Subnets
DROP TABLE IF EXISTS Subnets;

CREATE EXTERNAL TABLE Subnets (
    ip STRING,
    mask STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
	"input.regex" = '^(\\S*)\\t(\\S*).*$'
)
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';

SELECT * FROM Subnets LIMIT 10;
