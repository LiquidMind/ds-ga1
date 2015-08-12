package mapreduce.node;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * Created by Aidar on 09.08.2015.
 */
public interface WorkerNodeInterface extends Remote {
    /**
     * Start a new job
     * @param jobName
     * @param type
     * @param pathToJar
     * @param className
     * @param filename
     * @throws RemoteException
     */
    public void addJob(String jobName, byte type, String pathToJar, String className, String filename, String[] peers, int partNumber, int numberOfReducers, HashMap<String, Integer> finishedMappers) throws RemoteException;

    /**
     * Get job state
     * @param jobName
     * @return
     */
    public byte getJobState(String jobName) throws RemoteException;

    /**
     *
     * @param jobName
     * @param numberOfPart (number of reducers)
     * @return
     */
    public TreeMap getJobResults(String jobName, Integer numberOfPart) throws RemoteException; //
}
