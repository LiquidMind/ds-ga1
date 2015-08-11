REM start "DataNode-A RMI Registry" /min /normal rmiregistry 20001
start "DataNode-A" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy mapreduce/dfs/DataNode "../log/DataNodeA.txt" "../hdd/data_a/" 1073741824 "../hdd/dbs/data_a.sql" 20001

REM start "DataNode-B RMI Registry" /min /normal rmiregistry 20002
start "DataNode-B" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy mapreduce/dfs/DataNode "../log/DataNodeB.txt" "../hdd/data_b/" 1073741824 "../hdd/dbs/data_b.sql" 20002

REM start "DataNode-C RMI Registry" /min /normal rmiregistry 20003
start "DataNode-C" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy mapreduce/dfs/DataNode "../log/DataNodeC.txt" "../hdd/data_c/" 1073741824 "../hdd/dbs/data_c.sql" 20003

REM start "DataNode-D RMI Registry" /min /normal rmiregistry 20004
start "DataNode-D" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy mapreduce/dfs/DataNode "../log/DataNodeD.txt" "../hdd/data_d/" 1073741824 "../hdd/dbs/data_d.sql" 20004

pause

REM start "NameNode RMI Registry" /min /normal rmiregistry
start "NameNode" java -cp ".;../bin" -Djava.security.policy=server.policy mapreduce/dfs/NameNode "../log/NameNode.txt" "../hdd/name/" 1073741824 DataNodes.txt 20000

pause