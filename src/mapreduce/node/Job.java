package mapreduce.node;

import mapreduce.utils.MapReduce;
import mapreduce.utils.OutputCollector;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by Aidar on 10.08.2015.
 */
public class Job extends Thread {
    public static final byte STATE_DONE=0;
    public static final byte STATE_INPROGRESS=1;
    public static final byte STATE_FAILED=2;
    public static final byte STATE_STOPPED=3;

    private String jobname;
    private byte state;
    private byte type;
    private String filename;
    private MapReduce mapReduce;

    public Job(String jobname, byte type, MapReduce mapReduce, String filename) {
        super(jobname);
        this.jobname=jobname;
        this.type=type;
        this.mapReduce=mapReduce;
        this.filename=filename;
        state=STATE_STOPPED;
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
                //todo download the file from DFS
                //todo read file string by string - need to setup code for this
                try {
                    BufferedReader br = new BufferedReader(new FileReader(filename));
                    String line;
                    while ((line = br.readLine()) != null) {
                        // process the line.
                        mapReduce.map(filename,line,collector);
                    }
                    OutputCollector<String, Integer> collector1=collector;
                }
                catch (Exception e){
                    e.printStackTrace();
                }
//                collector.
            }
            if (type==MapReduce.TYPE_REDUCER){
                mapReduce.reduce();
            }
            state=STATE_DONE;
        }
        catch (Exception e){
            state=STATE_FAILED;
            e.printStackTrace();
        }
    }
}
