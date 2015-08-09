package mapreduce.node;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Created by Aidar on 17.07.2015.
 */
public class RMIServer extends UnicastRemoteObject {
    public int port;
    public String serviceName;

    /**
     * Constructor
     * @param port
     * @param serviceName
     * @throws java.rmi.RemoteException
     */
    public RMIServer(int port, String serviceName) throws RemoteException {
        super();
        this.port=port;
        this.serviceName=serviceName;
    }

    /**
     * This method begins listening on a port
     * thanks to stackoverflow and https://github.com/sorenchiron
     */
    public void startRMI(){
        //setting the property to be able to run on remote machine
        SysLogger.getInstance().info("Starting RMI");
        try {
            LocateRegistry.createRegistry(port).rebind(serviceName, this);
            SysLogger.getInstance().info("RMI server listening on port "+port);
        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Stops the listening of the master server
     */
    public void stopRMI(){
        SysLogger.getInstance().info("Stopping RMI");
        try {
            LocateRegistry.getRegistry(port).unbind(serviceName);
            SysLogger.getInstance().info("RMI server is stopped");
        }catch (Exception e)
        {
            SysLogger.getInstance().warning(e.getMessage());
            e.printStackTrace();
        }
    }
}
