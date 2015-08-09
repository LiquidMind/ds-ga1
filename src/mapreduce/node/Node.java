package mapreduce.node;

import java.rmi.RemoteException;

/**
 * Created by Aidar on 09.08.2015.
 */
public class Node extends RMIServer implements NodeInterface{
    /**
     * Constructor
     *
     * @param port
     * @param serviceName
     * @throws java.rmi.RemoteException
     */
    public Node(int port, String serviceName) throws RemoteException {
        super(port, serviceName);
    }

    @Override
    public void addJob(String jobName, String pathToJar, String className) {

    }
//    public static void start
}
