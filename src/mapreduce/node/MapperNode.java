package mapreduce.node;

import java.rmi.RemoteException;

/**
 * Created by Aidar on 09.08.2015.
 */
public class MapperNode extends Node {
    /**
     * Constructor
     *
     * @param port
     * @param serviceName
     * @throws java.rmi.RemoteException
     */
    public MapperNode(int port, String serviceName) throws RemoteException {
        super(port, serviceName);
    }

}
