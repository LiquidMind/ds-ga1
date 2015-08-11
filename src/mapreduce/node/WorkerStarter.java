package mapreduce.node;

import mapreduce.utils.MapReduce;

/**
 * Created by Aidar on 09.08.2015.
 */
public class WorkerStarter {
    public static void main(String [] args){
        int port;
        try {
            port=Integer.parseInt(args[0]);

            WorkerNode node;
            node=new WorkerNode(port, "mpnode");
            node.startRMI();
            node.addJob("qwe", MapReduce.TYPE_MAPPER, "\\tasks\\example1.jar", "TestMapReduce", "\\tasks\\nycrime2014.csv");
//            node.addJob("qwe", MapReduce.TYPE_REDUCER, "example1.jar", "TestMapReduce");
        }
        catch (Exception e){
            SysLogger.getInstance().warning("Can not start a worker");
            e.printStackTrace();
        }
    }
}
