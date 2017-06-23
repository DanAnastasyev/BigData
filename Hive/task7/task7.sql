ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
ADD FILE ./task7.py;

USE s201723;

DROP VIEW IF EXISTS DotComRecords;

CREATE VIEW DotComRecords AS
SELECT TRANSFORM(ip, request, pageSize, responseCode, browserInfo, date)
USING './task7.py' AS (ip, request, pageSize, responseCode, browserInfo, date)
FROM Logs;

SELECT * FROM DotComRecords LIMIT 10;