package mapreduce.node;

import java.rmi.RemoteException;

/**
 * Created by Aidar on 09.08.2015.
 */
public class WorkerNode extends RMIServer implements WorkerNodeInterface {
    /**
     * Constructor
     *
     * @param port
     * @param serviceName
     * @throws java.rmi.RemoteException
     */
    public WorkerNode(int port, String serviceName) throws RemoteException {
        super(port, serviceName);
    }

    @Override
    public void addJob(String jobName, byte type, String pathToJar, String className) throws RemoteException{
        SysLogger.getInstance().info("Job "+jobName+" started");
    }
}
