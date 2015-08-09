package mapreduce.node;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by Aidar on 09.08.2015.
 */
public interface NodeInterface  extends Remote {
    public void addJob(String jobName, String pathToJar, String className);
}
