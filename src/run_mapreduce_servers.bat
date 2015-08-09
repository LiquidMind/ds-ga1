start "WorkerNode-A RMI Registry" /min /normal rmiregistry 21001
start "WorkerNode-A" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy WorkerNode 21001

start "WorkerNode-B RMI Registry" /min /normal rmiregistry 21002
start "WorkerNode-B" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy WorkerNode 21002

start "WorkerNode-C RMI Registry" /min /normal rmiregistry 21003
start "WorkerNode-C" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy WorkerNode 21003

start "WorkerNode-D RMI Registry" /min /normal rmiregistry 21004
start "WorkerNode-D" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy WorkerNode 21004

start "WorkerNode-E RMI Registry" /min /normal rmiregistry 21005
start "WorkerNode-E" java -cp ".;../lib/sqlite-jdbc-3.8.10.1.jar;../bin" -Djava.security.policy=server.policy WorkerNode 21005

%pause

%start "MasterNode RMI Registry" /min /normal rmiregistry
%start "MasterNode" java -cp ".;../bin" -Djava.security.policy=server.policy MasterNode "../log/MasterNode.txt" localhost WorkerNodes.txt

%pause