package mapreduce.dfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;


public class DFSClient {
  private static Logger logger;
  
  static NameNodeInterface nameNode = null;
  static RemoteFileInterface remoteFile = null;
  static DataNodeInterface[] dataNodes = null;
  
  static HashSet<String> dataNodesAddresses;
  
  private static Scanner userInput = new Scanner(System.in);
  private static String message;
  
  private DFSClient() {}

  private static class SingletonHolder {  
     public static final DFSClient instance = new DFSClient();  
  }  
  
  public static DFSClient getInstance()  {  
     return SingletonHolder.instance;  
  }   
  
  public void init(String nameNodeHost, int nameNodePort, Logger externalLogger) throws RemoteException, NotBoundException, UninitializedLoggerException, NumberFormatException, MalformedURLException {
    logger = externalLogger;
    
    Registry registry = LocateRegistry.getRegistry(nameNodeHost, nameNodePort);
    nameNode = (NameNodeInterface) registry.lookup("NameNode");
    remoteFile = (RemoteFileInterface) registry.lookup("RemoteFile");
    
    //nameNode = (NameNodeInterface) Naming.lookup("NameNode");
    //remoteFile = (RemoteFileInterface) Naming.lookup("RemoteFile");
    //DataNodeInterface dataNode = (DataNodeInterface) Naming.lookup("DataNode");
    
    dataNodesAddresses = nameNode.getDataNodesAddresses();
    log(0, "There are " + dataNodesAddresses.size() + " data nodes:\n");
    dataNodes = new DataNodeInterface[dataNodesAddresses.size()];
    for (String s : dataNodesAddresses) {
      String[] parts = s.split(":");
      log(0, "ID: " + parts[0] + ", host: " + parts[1] + ", port: " + parts[2] + "\n");
      dataNodes[Integer.parseInt(parts[0]) - 1] = (DataNodeInterface) Naming.lookup("//" + parts[1] + ":" + parts[2] + "/DataNode"); // "//host:port/name"
    }
  }
  
  public RemoteFileInterface getFile(String path) throws RemoteException, IOException {
    return nameNode.getFile(path);
  }
  
  public long getHddQuota() throws RemoteException {
    return nameNode.getHddQuota();
  }
  
  public void makeDirectory (String path) throws RemoteException, IOException, UninitializedLoggerException {
    RemoteFileInterface folder = nameNode.getFile(path);
    if (folder.exists()) {
      log(0, "Name \"" + folder.remoteToString() + "\" already exists\n");
    } else {
      // create directory
      if (folder.mkdir()) {
        log(0, "Directory \"" + folder.remoteToString() + "\" has being created\n");
      } else {
        log(0, "Directory \"" + folder.remoteToString() + "\" can't be created\n");
      }
    }
  }
  
  public void removeDirectory (String path) throws RemoteException, IOException, UninitializedLoggerException, SQLException {
    RemoteFileInterface folder = nameNode.getFile(path);
    
    if (folder.getVirtualPath().equals("\\")) {
      log(0, "You can't remove root of the DFS.\n");
      log(0, "To clear everything use: init\n");
    } else if (!folder.exists()) {
      log(0, "Directory \"" + folder.remoteToString() + "\" doesn't exist\n");
    } else if (!folder.isDirectory()) {
      log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
    } else {
      switch (folder.isEmpty()) {
        case -1:
          // this is not possible, because we've filtered it earlier
          log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
          break;
        case 0:
          // directory is not empty, ask about recursive delete
          log(0, "Directory \"" + folder.remoteToString() + "\" is not empty. Delete recursively?\n");
          System.out.print(Logger.dateFormatter.format(System.currentTimeMillis()) + " >> Yes or No >> ");
          if (userInput.hasNextLine()) {
            message = userInput.nextLine();
          } else {
            message = "n";
          }
          if (message.trim().toLowerCase().equals("y") || message.trim().toLowerCase().equals("yes")) {
            if (folder.delete(true)) {
              log(0, "Directory \"" + folder.remoteToString() + "\" and its content was removed\n");
            } else {
              log(0, "Directory \"" + folder.remoteToString() + "\" can't be removed\n");
            }
          } else {
            log(0, "Directory \"" + folder.remoteToString() + "\" was not deleted\n");
          }
          break;
        case 1:
          if (folder.delete(false)) {
            log(0, "Directory \"" + folder.remoteToString() + "\" was removed\n");
          } else {
            log(0, "Directory \"" + folder.remoteToString() + "\" can't be removed\n");
          }
          break;
      }
    }
  }
  
  public void deleteFile(String path) throws RemoteException, IOException, UninitializedLoggerException, SQLException {
    if (path == null || path.equals("")) {
      throw new RuntimeException("path in deleteFile function is null or empty");
    }
    RemoteFileInterface file = nameNode.getFile(path);
    if (!file.exists()) {
      log(0, "File \"" + file.remoteToString() + "\" doesn't exist\n");
    } else if (!file.isFile()) {
      log(0, "\"" + file.remoteToString() + "\" is not a file\n");
    } else {
      if (file.delete(true)) {
        log(0, "File \"" + file.remoteToString() + "\" was deleted\n");
      } else {
        log(0, "File \"" + file.remoteToString() + "\" can't be deleted\n");
      }
    }
  }
  
  public void uploadFile(String local, String remote) throws RemoteException, IOException, UninitializedLoggerException, SQLException {
    RemoteFileInterface remoteFI = nameNode.getFile(remote);
    
    if (remote == null || remote.equals("")) {
      throw new RuntimeException("remote in uploadFile function is null or empty");
    }
    
    boolean error = false;
    if (remoteFI.exists()) {
      log(0, "Name \"" + remoteFI.remoteToString() + "\" is already used\n");
      error = true;
    } 
    File localF = new File(local);
    if (!localF.exists()) {
      log(0, "File \"" + localF + "\" doesn't exist\n");
      error = true;
    }
    if (!localF.isFile()) {
      log(0, "\"" + localF + "\" is not a file\n");
      error = true;
    }
    
    byte[] buffer = new byte[65536];
    byte[] chunk = null;
    List<int[]> chunks = new ArrayList<int[]>(); 
    int hash;
    int dataNodeId;
    int chunkLength;
    if (!error) {
      FileInputStream fis = new FileInputStream(localF);
      for (int i = 0; (chunkLength = fis.read(buffer)) != -1; i++) {
        log(0, "chunkLength: " + chunkLength + ", buffer.length: " + buffer.length + "\n");
        chunk = Arrays.copyOfRange(buffer, 0, chunkLength);
        hash = Arrays.hashCode(chunk);
        // trick that not to get negative values here
        dataNodeId = (hash % nameNode.numberOfDataNodes() + nameNode.numberOfDataNodes()) % nameNode.numberOfDataNodes();
        if (dataNodes[dataNodeId].saveChunk(chunk)) {
          int[] chunkData = {hash, dataNodeId};
          chunks.add(chunkData);
        } else {
          log(0, "Can't save chunk " + hash + " at node " + (dataNodeId + 1));
        }
      }
      fis.close();
      
      // save chunk list to name node
      remoteFI.saveChunksList(chunks);
      log(0, "File was uploaded from \"" + localF.getCanonicalPath() + "\" to " + remoteFI.remoteToString() + "\n");
    }
  }
  
  public boolean downloadFile (String remote, String local) throws RemoteException, IOException, UninitializedLoggerException, SQLException {
    RemoteFileInterface remoteFI = nameNode.getFile(remote);
    
    File localF = new File(local);
    
    boolean error = false;
    if (localF.exists()) {
      log(0, "Name \"" + localF.getCanonicalPath() + "\" is already used\n");
      error = true;
    } 
    
    if (!remoteFI.exists()) {
      log(0, "File \"" + remoteFI.remoteToString() + "\" doesn't exist\n");
      error = true;
    }
    if (!remoteFI.isFile()) {
      log(0, "\"" + remoteFI.remoteToString() + "\" is not a file\n");
      error = true;
    }
    
    byte[] buffer = null;
    List<int[]> chunks = remoteFI.getChunksList(); 
    
    if (!error) {
      FileOutputStream fos = new FileOutputStream(localF);
      
      for (int[] chunk : chunks) {
        buffer = dataNodes[chunk[1]].getChunk(chunk[0]);
        if (buffer != null) {
          log(0, "buffer.length: " + buffer.length + "\n");
        } else {
          log(0, "Can't read chunk " + chunk[0] + " from node " + chunk[1]);
        }
        fos.write(buffer);
      }
      fos.close();
      
      log(0, "File was saved from \"" + remoteFI.remoteToString() + "\" to " + localF.getCanonicalPath() + "\n");
    }
    
    return error;
  }
  
  public void log(int logLevel, String message) throws UninitializedLoggerException {
    if (logger == null) {
      throw new UninitializedLoggerException();
    }
    logger.log(logLevel, message);
  }  
}
