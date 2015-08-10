package mapreduce.node;

import mapreduce.utils.MapReduce;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.rmi.RemoteException;
import java.util.HashMap;

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
    public void addJob(String jobName, byte type, String pathToJar, String className, String filename) throws RemoteException{
        SysLogger.getInstance().info("Job "+jobName+" started");
        try {
            URL url = new URL("file:///"+pathToJar);
            URLClassLoader classLoader=new URLClassLoader(new URL[]{url});
            //todo make this
            Class c=classLoader.loadClass(className);

            MapReduce jobObject=(MapReduce) c.newInstance();
            Job job=new Job(jobName, type, jobObject, filename);
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
}
