package mapreduce.node;

import mapreduce.utils.Mapper;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
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
        try {
            URL url = new URL("file:///"+pathToJar);
            URLClassLoader classLoader=new URLClassLoader(new URL[]{url});
            //todo make this
            Class c=classLoader.loadClass(className);
//            Class c= TestClass.class;

            Object jobObject=c.newInstance();
            if (jobObject instanceof Mapper){
                Mapper m=(Mapper) jobObject;
                m.map();
            }
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
}
