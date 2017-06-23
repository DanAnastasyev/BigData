ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-exec.jar;
ADD JAR target/UDTF-1.0.0.jar;

USE s201723;

CREATE TEMPORARY FUNCTION ipRange as 'ru.mipt.bigdata.IpRangeUDTF';

SELECT ipRange(ip, mask)
FROM subnets
LIMIT 100;