ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201723;

DROP TABLE IF EXISTS ViewsByRegion;

CREATE TABLE ViewsByRegion AS
SELECT COUNT(*) as viewsCount, IPRegionsCompressed.region as region, LogsCompressed.request as request 
FROM LogsCompressed
INNER JOIN IPRegionsCompressed ON LogsCompressed.ip = IPRegionsCompressed.ip
GROUP BY IPRegionsCompressed.region, LogsCompressed.request;

SELECT ViewsByRegion.request, ViewsByRegion.region
FROM ViewsByRegion 
INNER JOIN (
	SELECT MAX(viewsCount) as viewsCount, region
	FROM ViewsByRegion
	GROUP BY region
) MaxViews 
ON ViewsByRegion.region = MaxViews.region AND ViewsByRegion.viewsCount = MaxViews.viewsCount;