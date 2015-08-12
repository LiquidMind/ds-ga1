REM start "WorkerNode-A RMI Registry" /min /normal rmiregistry 21001
start "WorkerNode-A" java -cp ".;../bin" -Djava.security.policy=server.policy mapreduce/node/WorkerStarter 21001

REM start "WorkerNode-B RMI Registry" /min /normal rmiregistry 21002
start "WorkerNode-B" java -cp ".;../bin" -Djava.security.policy=server.policy mapreduce/node/WorkerStarter 21002

REM start "WorkerNode-C RMI Registry" /min /normal rmiregistry 21003
start "WorkerNode-C" java -cp ".;../bin" -Djava.security.policy=server.policy mapreduce/node/WorkerStarter 21003

REM start "WorkerNode-D RMI Registry" /min /normal rmiregistry 21004
start "WorkerNode-D" java -cp ".;../bin" -Djava.security.policy=server.policy mapreduce/node/WorkerStarter 21004

REM start "WorkerNode-E RMI Registry" /min /normal rmiregistry 21005
start "WorkerNode-E" java -cp ".;../bin;../bin/mapreduce/node" -Djava.security.policy=server.policy mapreduce/node/WorkerStarter 21005

pause

REM start "MasterNode RMI Registry" /min /normal rmiregistry
REM start "MasterNode" java -cp ".;../bin;../bin/mapreduce/node" -Djava.security.policy=server.policy MasterNode "../log/MasterNode.txt" localhost WorkerNodes.txt 20000

REM pause