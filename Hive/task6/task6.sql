ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-contrib.jar;
ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-exec.jar;
ADD JAR target/Reverser-1.0.0.jar;

USE s201723;

create temporary function reverser as 'ru.mipt.bigdata.Reverser';

SELECT reverser(ip) FROM Logs LIMIT 10;