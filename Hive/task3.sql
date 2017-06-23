ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201723;

SELECT AverageCounts.region FROM (
	SELECT AVG(Counts.visitsCount) OVER() as avg, Counts.visitsCount as visitsCount, Counts.region as region
	FROM (
		SELECT COUNT(*) AS visitsCount, IPRegions.region AS region FROM Logs
		INNER JOIN IPRegions ON Logs.ip = IPRegions.ip
		GROUP BY IPRegions.region
	) Counts
) AverageCounts
WHERE AverageCounts.visitsCount > AverageCounts.avg;