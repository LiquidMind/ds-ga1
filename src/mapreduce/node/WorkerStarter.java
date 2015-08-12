package mapreduce.node;

import mapreduce.utils.MapReduce;

import java.util.ArrayList;

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
//            node.addJob("wordcount", MapReduce.TYPE_MAPPER, "\\tasks\\example1.jar", "WordCount", "\\tasks\\nycrime2014.csv");
//            node.addJob("wordcount", MapReduce.TYPE_MAPPER, "\\tasks\\example1.jar", "WordCount", "\\tasks\\word_count_small.txt", null);
            //ArrayList<String> peers=new ArrayList<String>();
            //peers.add("localhost:21001");

            //node.addJob("wordcount", MapReduce.TYPE_REDUCER, "\\tasks\\example1.jar", "WordCount", "\\tasks\\wordcount_shuffled_0", peers, 0);
            //node.addJob("wordcount", MapReduce.TYPE_REDUCER, "\\tasks\\example1.jar", "WordCount", "\\tasks\\wordcount_shuffled_0", peers, 1);
        }
        catch (Exception e){
            SysLogger.getInstance().warning("Can not start a worker");
            e.printStackTrace();
        }
    }
}
