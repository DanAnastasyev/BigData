ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201723;

-- Logs
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

DROP TABLE IF EXISTS LogsCompressed;

CREATE EXTERNAL TABLE LogsCompressed (
	ip STRING,
	request STRING,
	pageSize STRING,
	responseCode STRING,
	browserInfo STRING
)
PARTITIONED BY (date INT)
STORED AS ORC TBLPROPERTIES ("ocr.compress"="ZLIB");

INSERT OVERWRITE TABLE LogsCompressed PARTITION(date)
SELECT ip, request, pageSize, responseCode, browserInfo, date FROM LogsTmp;

-- IPRegions
DROP TABLE IF EXISTS IPRegionsCompressed;

CREATE EXTERNAL TABLE IPRegionsCompressed (
	ip STRING,
	region STRING
)
STORED AS ORC 
TBLPROPERTIES ("ocr.compress"="ZLIB");

INSERT OVERWRITE TABLE IPRegionsCompressed SELECT * FROM IPRegions;

-- -- Optimized tables query 
-- DROP TABLE IF EXISTS ViewsByRegion;

-- CREATE TABLE ViewsByRegion AS
-- SELECT COUNT(*) as viewsCount, IPRegionsCompressed.region as region, LogsCompressed.request as request 
-- FROM LogsCompressed
-- INNER JOIN IPRegionsCompressed ON LogsCompressed.ip = IPRegionsCompressed.ip
-- GROUP BY IPRegionsCompressed.region, LogsCompressed.request;

-- SELECT request
-- FROM ViewsByRegion 
-- INNER JOIN (
-- 	SELECT MAX(viewsCount) as viewsCount, region
-- 	FROM ViewsByRegion
-- 	GROUP BY region
-- ) MaxViews 
-- ON ViewsByRegion.region = MaxViews.region AND ViewsByRegion.viewsCount = MaxViews.viewsCount;

-- -- Simple tables query
-- DROP TABLE IF EXISTS ViewsByRegion;

-- CREATE TABLE ViewsByRegion AS
-- SELECT COUNT(*) as viewsCount, IPRegions.region as region, Logs.request as request 
-- FROM Logs
-- INNER JOIN IPRegions ON Logs.ip = IPRegions.ip
-- GROUP BY IPRegions.region, Logs.request;

-- SELECT request
-- FROM ViewsByRegion 
-- INNER JOIN (
-- 	SELECT MAX(viewsCount) as viewsCount, region
-- 	FROM ViewsByRegion
-- 	GROUP BY region
-- ) MaxViews 
-- ON ViewsByRegion.region = MaxViews.region AND ViewsByRegion.viewsCount = MaxViews.viewsCount;