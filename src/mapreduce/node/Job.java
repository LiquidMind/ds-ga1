package mapreduce.node;

import mapreduce.dfs.DFSClient;
import mapreduce.dfs.IncorrectLogFileException;
import mapreduce.dfs.Logger;
import mapreduce.utils.MapReduce;
import mapreduce.utils.OutputCollector;
import mapreduce.utils.ReducerCollector;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map.Entry;
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
    private String[] peerList;
    private int numberOfReducers;
    private HashMap<String, Integer> finishedMappers;

    public Job(String jobname, byte type, MapReduce mapReduce, String filename) {
        super(jobname);
        this.jobname=jobname;
        this.type=type;
        this.mapReduce=mapReduce;
        this.filename=filename;
        state=STATE_STOPPED;
        peerList=null;
        id=0;
        // TODO:
        this.numberOfReducers = 0;
        this.finishedMappers = null;
    }

    public Job(String jobname, byte type, MapReduce mapReduce, String filename, String[] peerList, int id, int numberOfReducers, HashMap<String, Integer> finishedMappers) {
        super(jobname);
        this.jobname=jobname;
        this.type=type;
        this.mapReduce=mapReduce;
        this.filename=filename;
        state=STATE_STOPPED;
        this.peerList=peerList;
        this.id=id;
        this.numberOfReducers = numberOfReducers;
        this.finishedMappers = finishedMappers;
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
                collector.spill("../tasks/"+jobname+"_shuffled_", numberOfReducers);
            }
            if (type==MapReduce.TYPE_REDUCER){
                //traverse all mappers
                ReducerCollector<String, Integer> collector=new ReducerCollector<String, Integer>();

                TreeMap<String, ArrayList<Integer>> map=new TreeMap<String, ArrayList<Integer>>();
                //traverse each finished mapper
                for (String jobUUID: finishedMappers.keySet()){
                  
                    //get relative treemap from each mapper

                    //connect to peer
                    String[] parts = peerList[finishedMappers.get(jobUUID)].split(":");
                    String peerHost = parts[1];
                    int peerPort = Integer.parseInt(parts[2]);

                    Registry registry = LocateRegistry.getRegistry(peerHost, peerPort);
                    WorkerNodeInterface peer = (WorkerNodeInterface) registry.lookup("mpnode");

                    //todo fix 1 to a logical number
                    TreeMap inputData=peer.getJobResults(jobUUID, id);
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

                String localPathToData = "..\\tasks\\" + jobname + "_reduced_" + id + ".dat";
                String remotePathToData = filename + "\\reduced_" + id + ".dat";
                collector.spill(localPathToData);
                
                Logger logger = null;
                try {
                  logger = new Logger(0, "..\\log\\Job.log");
                } catch (IncorrectLogFileException e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
                }

                DFSClient dfs = DFSClient.getInstance();
                try {
                  dfs.init("localhost", 20000, logger);
                } catch (Exception e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
                }
                
                try {
                  dfs.deleteFile(remotePathToData);
                  dfs.uploadFile(localPathToData, remotePathToData);
                } catch (Exception e) {
                  // TODO Auto-generated catch block
                  e.printStackTrace();
                }
            }
            state=STATE_DONE;
        }
        catch (Exception e){
            state=STATE_FAILED;
            e.printStackTrace();
        }
    }
}
