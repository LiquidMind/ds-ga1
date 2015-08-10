package mapreduce.node;

import java.rmi.Remote;
import java.rmi.RemoteException;

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
    public void addJob(String jobName, byte type, String pathToJar, String className, String filename) throws RemoteException;

    /**
     * Get job state
     * @param jobName
     * @return
     */
    public byte getJobState(String jobName) throws RemoteException;
}
