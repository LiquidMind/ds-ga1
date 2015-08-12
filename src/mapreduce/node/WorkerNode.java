package mapreduce.node;

import mapreduce.dfs.DFSClient;
import mapreduce.dfs.IncorrectLogFileException;
import mapreduce.dfs.Logger;
import mapreduce.utils.MapReduce;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by Aidar on 09.08.2015.
 */
public class WorkerNode extends RMIServer implements WorkerNodeInterface {
    private HashMap<String,Job> jobList;
    /**
     * Constructor
     *
     * @param port
     * @param serviceName
     * @throws java.rmi.RemoteException
     */
    public WorkerNode(int port, String serviceName) throws RemoteException {
        super(port, serviceName);
        jobList=new HashMap<String, Job>();
    }

    @Override
    public void addJob(String jobName, byte type, String pathToJar, String className, String pathToData, ArrayList<String> peers, int parNumber) throws RemoteException{
        SysLogger.getInstance().info("Job "+jobName+" started");
        
        Logger logger = null;
        try {
          logger = new Logger(0, "..\\log\\WorkerNode.log");
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
        
        String localPathToJar = "..\\tasks\\" + jobName + ".jar";
        File jarFile = new File(localPathToJar);
        String localPathToData = "..\\tasks\\" + jobName + ".dat";
        File dataFile = new File(localPathToData);
        try {
          dfs.downloadFile(pathToJar, localPathToJar);
          dfs.downloadFile(pathToData, localPathToData);
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
        try {
            URL url = new URL("file:///" + jarFile.getAbsolutePath());
            URLClassLoader classLoader=new URLClassLoader(new URL[]{url});
            //todo make this
            Class c=classLoader.loadClass(className);

            MapReduce jobObject=(MapReduce) c.newInstance();
            Job job=new Job(jobName, type, jobObject, dataFile.getAbsolutePath(),peers,parNumber);
            jobList.put(jobName,job);

            job.start(); //start new thread
            //return control
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte getJobState(String jobName) {
        return jobList.get(jobName).getJobState();
    }

    @Override
    public TreeMap getJobResults(String jobName, Integer numberOfPart) {
        File file = new File("../tasks/"+jobName+"_shuffled_"+numberOfPart);
        FileInputStream f = null;
        try {
            f = new FileInputStream(file);
            ObjectInputStream s = new ObjectInputStream(f);
            TreeMap<String, Object> map = (TreeMap<String, Object>) s.readObject();
            s.close();
            return map;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
