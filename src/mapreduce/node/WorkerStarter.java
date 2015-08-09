package mapreduce.node;

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
            node.addJob("qwe", (byte) 1,"example1.jar","TestClass");
        }
        catch (Exception e){
            SysLogger.getInstance().warning("Can not start a worker");
            e.printStackTrace();
        }
    }
}
