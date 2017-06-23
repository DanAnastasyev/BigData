ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;

USE s201723;

SELECT date, COUNT(*) as cnt
FROM Logs
GROUP BY date
ORDER BY cnt DESC;
