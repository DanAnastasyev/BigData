ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201723;

SELECT SUM(IF(Users.sex='male',1,0)), 
	   SUM(IF(Users.sex='female',1,0)), 
	   IPRegions.region 
FROM Logs TABLESAMPLE(1 PERCENT)
INNER JOIN IPRegions ON Logs.ip = IPRegions.ip
INNER JOIN Users ON (
	Logs.ip = Users.ip
	AND Logs.browserInfo = Users.browser
)
GROUP BY IPRegions.region;
