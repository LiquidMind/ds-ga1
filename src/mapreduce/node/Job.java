package mapreduce.node;

import mapreduce.utils.MapReduce;
import mapreduce.utils.OutputCollector;
import mapreduce.utils.ReducerCollector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URL;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ListIterator;
import java.util.TreeMap;

/**
 * Created by Aidar on 10.08.2015.
 */
public class Job extends Thread {
    public static final byte STATE_DONE=0;
    public static final byte STATE_INPROGRESS=1;
    public static final byte STATE_FAILED=2;
    public static final byte STATE_STOPPED=3;

    private int id;
    private String jobname;
    private byte state;
    private byte type;
    private String filename;
    private MapReduce mapReduce;
    private ArrayList<String> peerList;

    public Job(String jobname, byte type, MapReduce mapReduce, String filename) {
        super(jobname);
        this.jobname=jobname;
        this.type=type;
        this.mapReduce=mapReduce;
        this.filename=filename;
        state=STATE_STOPPED;
        peerList=null;
        id=0;
    }

    public Job(String jobname, byte type, MapReduce mapReduce, String filename, ArrayList<String> peerList, int id) {
        super(jobname);
        this.jobname=jobname;
        this.type=type;
        this.mapReduce=mapReduce;
        this.filename=filename;
        state=STATE_STOPPED;
        this.peerList=peerList;
        this.id=id;
    }

    public byte getJobState(){
        return state;
    }

    /**
     * Entry point
     */
    public void run(){
        state=STATE_INPROGRESS;
        try {
            if (type==MapReduce.TYPE_MAPPER){
                //todo this is the issue - we only instantiate same type of collectors
                OutputCollector<String, Integer> collector=new OutputCollector<String, Integer>();
                //file is already downloaded from DFS
                //todo read file string by string - need to setup code for this
                try {
                    BufferedReader br = new BufferedReader(new FileReader(filename));
                    String line;
                    while ((line = br.readLine()) != null) {
                        // process the line.
                        mapReduce.map(filename,line,collector);
                    }
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                collector.spill("../tasks/"+jobname+"_shuffled_",2);
            }
            if (type==MapReduce.TYPE_REDUCER){
                //traverse all mappers
                ReducerCollector<String, Integer> collector=new ReducerCollector<String, Integer>();

                TreeMap<String, ArrayList<Integer>> map=new TreeMap<String, ArrayList<Integer>>();
                //traverse each mapper
                for (String peerAddress: peerList){
                    //get relative treemap from each mapper

                    //connect to peer
                    URL host=new URL("http://"+peerAddress);
                    int peerPort=host.getPort()==-1 ? 21001 : host.getPort();

                    Registry registry = LocateRegistry.getRegistry(host.getHost(), peerPort);
                    WorkerNodeInterface peer = (WorkerNodeInterface) registry.lookup("mpnode");

                    //todo fix 1 to a logical number
                    TreeMap inputData=peer.getJobResults(jobname, id);
                    //join values by keys
                    for (Object key: inputData.keySet()){
                        if (map.containsKey(key)){
                            map.get(key).addAll((ArrayList<Integer>) inputData.get(key));
                        }
                        else{
                            map.put((String) key, (ArrayList<Integer>) inputData.get(key));
                        }
                    }
                }
                //assume here we have all keys loaded

                //traverse all keys
                for (String key: map.keySet()){
                    // process the line.
                    if (map.get(key)!=null){
                        mapReduce.reduce(key, map.get(key).iterator(), collector);
                    }
                }

                collector.spill("../tasks/"+jobname+"_reduced_"+id);
            }
            state=STATE_DONE;
        }
        catch (Exception e){
            state=STATE_FAILED;
            e.printStackTrace();
        }
    }
}
