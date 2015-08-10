package mapreduce.node;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by Aidar on 09.08.2015.
 */
public interface WorkerNodeInterface extends Remote {
    public static final byte STATE_DONE=1;
    public static final byte STATE_INPROGRESS=1;
    public static final byte STATE_FAILED=2;

    /**
     * Start a new job
     * @param jobName
     * @param type
     * @param pathToJar
     * @param className
     * @throws RemoteException
     */
    public void addJob(String jobName, byte type, String pathToJar, String className) throws RemoteException;

    /**
     * Get job state
     * @param jobName
     * @return
     */
    public byte getJobState(String jobName);
}
