ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201723;

DROP TABLE IF EXISTS ViewsByRegion;

CREATE TABLE ViewsByRegion AS
SELECT COUNT(*) as viewsCount, IPRegions.region as region, Logs.request as request 
FROM Logs
INNER JOIN IPRegions ON Logs.ip = IPRegions.ip
GROUP BY IPRegions.region, Logs.request;

SELECT ViewsByRegion.request, ViewsByRegion.region
FROM ViewsByRegion 
INNER JOIN (
	SELECT MAX(viewsCount) as viewsCount, region
	FROM ViewsByRegion
	GROUP BY region
) MaxViews 
ON ViewsByRegion.region = MaxViews.region AND ViewsByRegion.viewsCount = MaxViews.viewsCount;