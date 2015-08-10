import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.LinkedTransferQueue;
import java.util.regex.Pattern;

import mapreduce.node.*;
import mapreduce.utils.MapReduce;


public class MasterNode {
  //flag to notify that client should keep working
  static boolean keepWorking = true;
  
  // scanner to read user input
  private static Scanner userInput = new Scanner(System.in);
  // message that was read from the user
  private static String message = "";
  // processed message 
  private static String msg = "";
  
  // reference to Logger class
  private static Logger logger;
  
  // current directory within the DFS
  private static String currentFolder = "\\";
  
  static HashSet<String> dataNodesAddresses;
  
  static NameNodeInterface nameNode = null;
  static RemoteFileInterface remoteFile = null;
  static DataNodeInterface[] dataNodes = null;
  
  static String workerNodesFileName;
  static HashSet<String> workerNodesAddresses = new HashSet<String>();;
  static WorkerNodeInterface[] workerNodes = null;
  
  static String nameNodeHost;
  static int nameNodePort;
  
  public static void main(String[] args) throws UninitializedLoggerException, IOException, SQLException {

    try {
      // init everything to log messages
      if (args.length > 1) {
        initLogger(0, args[0]); // 1 - logLevel, 2 - logFilename
      } else {
        initLogger(0, null); // 1 - logLevel, 2 - logFilename
      }
      
      if (args.length < 4) {
        throw new IncompleteArgumentListException();
      }
    
      // init security manager
      if (System.getSecurityManager() == null) {
        System.setSecurityManager(new SecurityManager());
      }
  
      nameNodeHost = args[1];
      nameNodePort = Integer.parseInt(args[3]);
      log(0, "Name node port is: " + nameNodePort + "\n");
      
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
      
      // name of the file with the list of expected worker nodes from the program parameters
      workerNodesFileName = args[2];
      
      // check if file exists
      File f = new File(workerNodesFileName);
      if(!f.exists() || f.isDirectory()) {
        throw new IncorrectWorkerNodesFileNameFileException();
      }
      
      // read list of of expected data nodes to connect to
      log(0, "WorkerNodes' addresses to connect to are:\n");
  
      List<String> lines = Files.readAllLines(Paths.get(workerNodesFileName), Charset.defaultCharset());
      for (String line : lines) {
        log(0, line + "\n");
        workerNodesAddresses.add(line.trim());
      }
      if (workerNodesAddresses.isEmpty()) {
        throw new EmptyWorkerNodesFileException();
      }
      
      log(0, "There are " + workerNodesAddresses.size() + " worker nodes:\n");
      workerNodes = new WorkerNodeInterface[workerNodesAddresses.size()];
      for (String s : workerNodesAddresses) {
        String[] parts = s.split(":");
        log(0, "ID: " + parts[0] + ", host: " + parts[1] + ", port: " + parts[2] + "\n");
        workerNodes[Integer.parseInt(parts[0]) - 1] = (WorkerNodeInterface) Naming.lookup("//" + parts[1] + ":" + parts[2] + "/mpnode"); // "//host:port/name"
      }
      
      userInput  = new Scanner(System.in);
      
      log(0, "Type your message and press <Enter>\n");
      log(0, "Type \"exit\" and press <Enter> for exit\n");
      
      while(keepWorking){
        System.out.print(Logger.dateFormatter.format(System.currentTimeMillis()) + " >> ");
        if (userInput.hasNextLine()) {
          message = userInput.nextLine();
        } else {
          message = "exit";
        }
        
        String[] splitArray = message.trim().split("\\s+");
        msg = splitArray[0].toLowerCase();
  
        if (msg.equals("exit")) {
          // notify to stop thread and close connection
          keepWorking = false;
          
        } else if (msg.equals("quota")) {
          // print DFS HDD quota
          log(0, "HDD Quota: " + nameNode.getHddQuota() + " bytes\n");
          
        } else if (msg.equals("ls")) {
          RemoteFileInterface folder = null;
          
          // print content of the current directory
          if (splitArray.length == 1) {
            folder = nameNode.getFile(currentFolder);
          } else if (splitArray.length == 2) {
            // print content of the directory in parameter
            String path = normalizePath(splitArray[1]);
            
            folder = nameNode.getFile(path);
            if (!folder.exists()) {
              log(0, "Directory \"" + folder.remoteToString() + "\" doesn't exist\n");
              folder = null;
            } else if (!folder.isDirectory()) {
              log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
              folder = null;
            }
          } else {
            log(0, "Use: ls [directory_name]");
          }
          
          if (folder != null) {
            RemoteFileInterface[] listOfFiles = folder.listFiles();
            String pattern = folder.remoteToString() + (folder.remoteToString().equals("\\") ? "" : "\\"); 
            if (listOfFiles.length == 0) {
              log(0, "Folder \"" + folder.remoteToString() + "\" is empty\n");
            } else {
              log(0, "Folder \"" + folder.remoteToString() + "\" contains:\n");
              for (int i = 0; i < listOfFiles.length; i++) {
                log(0, "  " + listOfFiles[i].remoteToString().replaceFirst(Pattern.quote(pattern), "") + "\n");
              }
            }
          }
          
        } else if (msg.equals("pwd")) {
          // print working directory
          RemoteFileInterface folder = nameNode.getFile(currentFolder);
          log(0, "Current working directory is: \"" + folder.remoteToString() + "\"\n");
          
        } else if (msg.equals("cd")) {
          // change working directory
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: cd <directory name>");
          } else {
            String path = normalizePath(splitArray[1]);
            
            RemoteFileInterface folder = nameNode.getFile(path);
            if (!folder.exists()) {
              log(0, "Directory \"" + folder.remoteToString() + "\" doesn't exist\n");
            } else if (!folder.isDirectory()) {
              log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
            } else {
              log(0, "Working directory has being changed to: \"" + folder.remoteToString() + "\"\n");
              currentFolder = folder.remoteToString();
            }
          }
          
        } else if (msg.equals("mkdir")) {
          // create new directory
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: mkdir <directory name>");
          } else {
            String path = normalizePath(splitArray[1]);
            
            makeDirectory(path);
          }
          
        } else if (msg.equals("rmdir")) {
          // change working directory
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: rmdir <directory name>");
          } else {
            String path = normalizePath(splitArray[1]);
            
            removeDirectory(path);
          }
          
        } else if (msg.equals("upload")) {
          // upload file from local fs to dfs
          if (splitArray.length < 3 || splitArray.length > 3) {
            log(0, "Use: upload <local name> <remote name>\n");
          } else {
            String local = splitArray[1];
            String remote = normalizePath(splitArray[2]);
            
            uploadFile(local, remote);
          }
          
        } else if (msg.equals("download")) {
          // download file from dfs to local fs
          if (splitArray.length < 3 || splitArray.length > 3) {
            log(0, "Use: download <remote name> <local name>\n");
          } else {
            String remote = normalizePath(splitArray[1]);
            String local = splitArray[2];
            
            downloadFile(remote, local);
          }
          
        } else if (msg.equals("delete")) {
          // delete file from dfs
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: delete <file name>");
          } else {
            String path = normalizePath(splitArray[1]);
            
            deleteFile(path);
          }
          
        } else if (msg.equals("init")) {
            RemoteFileInterface folder = nameNode.getFile("\\");

            // this will clear everything
            log(0, "This will clear everything within DFS. Are you sure?\n");
            System.out.print(Logger.dateFormatter.format(System.currentTimeMillis()) + " >> Yes or No >> ");
            if (userInput.hasNextLine()) {
              message = userInput.nextLine();
            } else {
              message = "n";
            }
            if (message.trim().toLowerCase().equals("y") || message.trim().toLowerCase().equals("yes")) {
              folder.delete(true);
              log(0, "Everything was removed from DFS\n");
              // print DFS HDD quota
              log(0, "HDD Quota: " + nameNode.getHddQuota() + " bytes\n");
            } else {
              log(0, "Init was not performed\n");
            }
        } else if (msg.equals("mapreduce")) {
          // run mapreduce tasks on available workers
          if (splitArray.length < 7 || splitArray.length > 7) {
            log(0, "Use: mapreduce <job name> <mappers count> <reducers count> <jar with task> <class name> <data directory> <output file>\n");
          } else {
            String jobName = splitArray[0];
            int mCount = Integer.parseInt(splitArray[1]);
            int rCount = Integer.parseInt(splitArray[2]);
            String pathToJar = normalizePath(splitArray[3]);
            String className = splitArray[4];
            String pathToData = normalizePath(splitArray[5]);
            String pathToResult = normalizePath(splitArray[6]);
            
            boolean jobNotFinished = true;
            
            int i = 0;
            int j = 0;
            int k = 0;
            
            // HashMap<mapperId: from 0 to mCount, nodeId: from 0 to workerNodes.length>
            HashMap<Integer, Integer> workingMappers = new HashMap<Integer, Integer>();
            LinkedTransferQueue<Integer> unassignedMappers = new LinkedTransferQueue<Integer>();
            for (i = 0; i < mCount; i++) {
              unassignedMappers.add(i);
            }
            HashMap<Integer, Integer> finishedMappers = new HashMap<Integer, Integer>();
            
            // HashMap<reducerId: from 0 to rCount, nodeId: from 0 to workerNodes.length>
            HashMap<Integer, Integer> workingReducers = new HashMap<Integer, Integer>();
            LinkedTransferQueue<Integer> unassignedReducers = new LinkedTransferQueue<Integer>();
            for (i = 0; i < rCount; i++) {
              unassignedReducers.add(i);
            }
            HashMap<Integer, Integer> finishedReducers = new HashMap<Integer, Integer>();
            
            HashSet<Integer> failedWorkers = new HashSet<Integer>();
            
            // worker node to start assigning tasks from
            i = randomWithRange(0, workerNodes.length - 1);

            for (j = 0; j < rCount && k < workerNodes.length; j++) {
              try {
                workerNodes[i].addJob(jobName, MapReduce.TYPE_REDUCER, pathToJar, className);
                k = 0;
              } catch (Exception e) {
                log(0, "Exception while executing mapper job no. " + j);
                e.printStackTrace();
                j--;
                k++;
              }
              i = (i + 1) / workerNodes.length;
            }
            
            // while we have incomplete map tasks
            while (finishedMappers.size() < mCount && failedWorkers.size() < workerNodes.length) {
              if (failedWorkers.contains(i)) {
                // if current worker failed move to next
                i = (i + 1) / workerNodes.length;
                continue;
              }
              // while we have unassigned map tasks
              while (unassignedMappers.size() > 0 && failedWorkers.size() < workerNodes.length) {
                j = unassignedMappers.poll();
                try {
                  workerNodes[i].addJob(jobName, MapReduce.TYPE_MAPPER, pathToJar, className);
                  workingMappers.put(j, i); // add map task to the list of working mappers
                } catch (Exception e) {
                  log(0, "Exception while executing mapper job no. " + j);
                  e.printStackTrace();
                  unassignedMappers.add(j); // return map back to the list of unassigned map tasks
                  failedWorkers.add(i); // add worker to the list of failed workers
                }
                // move to next worker
                i = (i + 1) / workerNodes.length;
              }
              
              // iterate through all tasks and get their status
              Iterator<Entry<Integer, Integer>> it = workingMappers.entrySet().iterator();
              while (it.hasNext()) {
                Map.Entry<Integer, Integer> pair = it.next();
                int taskId = pair.getKey();
                int workerId = pair.getValue();
                int jobState = workerNodes[workerId].getJobState(jobName);
                
                switch (jobState) {
                  case Job.STATE_DONE:
                    log(0, "Mapper task no. " + taskId + " has been finished\n");
                    log(0, "Added it to the list of finished tasks\n");
                    workingMappers.remove(taskId);
                    finishedMappers.put(taskId, workerId);
                    break;
                  case Job.STATE_INPROGRESS:
                    // do nothing
                    break;
                  case Job.STATE_FAILED:
                    log(0, "Mapper task no. " + taskId + " has failed on worker no. " + workerId + "\n");
                    log(0, "Added it to the list of failed workers\n");
                    workingMappers.remove(taskId);
                    unassignedMappers.add(taskId);
                    failedWorkers.add(workerId);
                    break;  
                  case Job.STATE_STOPPED:
                    // do nothing
                    break;
                  default:
                    throw new UnknownJobStateException();  
                }
                it.remove(); // avoids a ConcurrentModificationException
              }
            }
            
            if (k == workerNodes.length) {
              log(0, "There are no available worker nodes to add map jobs");
            }
            
            if (k == workerNodes.length) {
              log(0, "There are no available worker nodes to add reduce jobs");
            }
            
            while (jobNotFinished) {
              //workerNodes[0].addJob(jobName, type, pathToJar, className);
              jobNotFinished = false;
            }
          }
        } else if (msg.equals("")) {
          // just do nothing
          
        } else {
          // print program usage
          log(0, "Usage: \n");
          log(0, "  exit - exit the program\n");
          log(0, "  help - shows possible program parameters\n");
          log(0, "  quota - shows HDD quota for the DFS\n");
          log(0, "  ls [directory_name] - list directory contents\n");
          log(0, "  cd <directory_name> - change directory\n");
          log(0, "  mkdir <directory_name> - create directory\n");
          log(0, "  rmdir <directory_name> - remove directory\n");
          log(0, "  upload <local name> <remote name> - upload file from local to remote fs\n");
          log(0, "  download <remote name> <local name> - download file from remote to local fs\n");
          log(0, "  delete <file name> - delete file from remote fs\n");
          log(0, "  init - clear everything within dfs\n");
          log(0, "  \n");
          log(0, "  to be done...\n");
        }
      }
  
      log(0, "Client was shutted down");
      
    } catch (IncompleteArgumentListException e) {
      log(0, "Use: MasterNode logFileName NameNodeAddress workNodesFileName");
    } catch (IncorrectLogFileException e) {
      log(0, "File to store log files can't be created or inaccessible.");
    } catch (RemoteException e) {
      log(0, "RemoteException was thrown.");
      e.printStackTrace();
    } catch (NotBoundException e) {
      log(0, "Some RMI object is not bound.");
      e.printStackTrace();
    } catch (MalformedURLException e) {
      log(0, "Malformed URI was used to lookup RMI object.");
      e.printStackTrace();
    } catch (IncorrectWorkerNodesFileNameFileException e) {
      log(0, "File with list of worker nodes doesn't exist or inaccessible.");
    } catch (EmptyWorkerNodesFileException e) {
      log(0, "File with list of worker nodes is empty.");
    }       
  }
  
  static int randomWithRange(int min, int max) {
     int range = (max - min) + 1;
     return (int)(Math.random() * range) + min;
  }
  
  public static void initLogger(int logLevel, String logFilename) throws IncorrectLogFileException {
    logger = new Logger(logLevel, logFilename);
  }
  
  public static void log(int logLevel, String message) throws UninitializedLoggerException {
    if (logger == null) {
      throw new UninitializedLoggerException();
    }
    logger.log(logLevel, message);
  }
  
  static String normalizePath(String path) {
    if (path.startsWith("\\")) {
      return path;
    } else {
      return currentFolder + "\\" + path;
    }
  }
  
  static void makeDirectory (String path) throws RemoteException, IOException, UninitializedLoggerException {
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
  
  static void removeDirectory (String path) throws RemoteException, IOException, UninitializedLoggerException, SQLException {
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
  
  static void deleteFile(String path) throws RemoteException, IOException, UninitializedLoggerException, SQLException {
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
  
  static void uploadFile(String local, String remote) throws RemoteException, IOException, UninitializedLoggerException, SQLException {
    RemoteFileInterface remoteFI = nameNode.getFile(remote);
    
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
  
  static void downloadFile (String remote, String local) throws RemoteException, IOException, UninitializedLoggerException, SQLException {
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
  }  
}
