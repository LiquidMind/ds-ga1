package mapreduce.node;

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
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.LinkedTransferQueue;
import java.util.regex.Pattern;

import mapreduce.node.*;
import mapreduce.utils.MapReduce;
import mapreduce.dfs.*;

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
  
  static String workerNodesFileName;
  static HashSet<String> workerNodesAddresses = new HashSet<String>();
  static WorkerNodeInterface[] workerNodes = null;
  
  static String nameNodeHost;
  static int nameNodePort;
  
  static DFSClient dfs = DFSClient.getInstance();
  
  static int jobTrackerTimeout = 1000; // milliseconds  
  
  public static void main(String[] args) throws UninitializedLoggerException, IOException, SQLException, UnknownJobStateException, InterruptedException {

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
      /*if (System.getSecurityManager() == null) {
        System.setSecurityManager(new SecurityManager());
      }*/
  
      nameNodeHost = args[1];
      nameNodePort = Integer.parseInt(args[3]);
      log(0, "Name node port is: " + nameNodePort + "\n");
      
      // TODO: Add dfs client here
      dfs.init(nameNodeHost, nameNodePort, logger);
      
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
          log(0, "HDD Quota: " + dfs.getHddQuota() + " bytes\n");
          
        } else if (msg.equals("ls")) {
          RemoteFileInterface folder = null;
          
          // print content of the current directory
          if (splitArray.length == 1) {
            folder = dfs.getFile(currentFolder);
          } else if (splitArray.length == 2) {
            // print content of the directory in parameter
            String path = normalizePath(splitArray[1]);
            
            folder = dfs.getFile(path);
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
          RemoteFileInterface folder = dfs.getFile(currentFolder);
          log(0, "Current working directory is: \"" + folder.remoteToString() + "\"\n");
          
        } else if (msg.equals("cd")) {
          // change working directory
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: cd <directory name>");
          } else {
            String path = normalizePath(splitArray[1]);
            
            RemoteFileInterface folder = dfs.getFile(path);
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
            
            dfs.makeDirectory(path);
          }
          
        } else if (msg.equals("rmdir")) {
          // change working directory
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: rmdir <directory name>");
          } else {
            String path = normalizePath(splitArray[1]);
            
            dfs.removeDirectory(path);
          }
          
        } else if (msg.equals("upload")) {
          // upload file from local fs to dfs
          if (splitArray.length < 3 || splitArray.length > 3) {
            log(0, "Use: upload <local name> <remote name>\n");
          } else {
            String local = splitArray[1];
            String remote = normalizePath(splitArray[2]);
            
            dfs.uploadFile(local, remote);
          }
          
        } else if (msg.equals("download")) {
          // download file from dfs to local fs
          if (splitArray.length < 3 || splitArray.length > 3) {
            log(0, "Use: download <remote name> <local name>\n");
          } else {
            String remote = normalizePath(splitArray[1]);
            String local = splitArray[2];
            
            dfs.downloadFile(remote, local);
          }
          
        } else if (msg.equals("delete")) {
          // delete file from dfs
          if (splitArray.length < 2 || splitArray.length > 2) {
            log(0, "Use: delete <file name>");
          } else {
            String path = normalizePath(splitArray[1]);
            
            dfs.deleteFile(path);
          }
          
        } else if (msg.equals("init")) {
            RemoteFileInterface folder = dfs.getFile("\\");

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
              log(0, "HDD Quota: " + dfs.getHddQuota() + " bytes\n");
            } else {
              log(0, "Init was not performed\n");
            }
        } else if (msg.equals("test")) {
          String reg = "[^a-zA-Z']+";
          String str = "One, two, three, four, five, s'ix, seven, eight, nine, ten!";
          String[] res = str.split(reg);
          log(0, "res.length: " + res.length + "\n");
          log(0, Arrays.deepToString(res) + "\n");
        } else if (msg.equals("mapreduce")) {
          // run mapreduce tasks on available workers
          if (splitArray.length < 8 || splitArray.length > 8) {
            log(0, "Use: mapreduce <job name> <mappers count> <reducers count> <jar with task> <class name> <data directory> <output file>\n");
          } else {
            String jobName = splitArray[1];
            int mCount = Integer.parseInt(splitArray[2]);
            int rCount = Integer.parseInt(splitArray[3]);
            String pathToJar = normalizePath(splitArray[4]);
            String className = splitArray[5];
            String pathToData = normalizePath(splitArray[6]);
            String pathToResults = normalizePath(splitArray[7]);
            
            String[] peers = new String[workerNodesAddresses.size()];
            workerNodesAddresses.toArray(peers);
            
            boolean jobNotFinished = true;
            
            int i = 0;
            String jm = null;
            int jr = 0;
            int k = 0;
            
            // HashMap<mapperId: path to data, nodeId: from 0 to workerNodes.length>
            HashMap<String, Integer> workingMappers = new HashMap<String, Integer>();
            LinkedTransferQueue<String> unassignedMappers = new LinkedTransferQueue<String>();
            /*
            for (i = 0; i < mCount; i++) {
              unassignedMappers.add(i);
            }
            */
            HashMap<String, Integer> finishedMappers = new HashMap<String, Integer>();
            // HashMap<UUID of the task, path to data>
            HashMap<String, String> mapperTasks = new HashMap<String, String>();
            
            // HashMap<reducerId: from 0 to rCount, nodeId: from 0 to workerNodes.length>
            HashMap<String, Integer> workingReducers = new HashMap<String, Integer>();
            LinkedTransferQueue<Integer> unassignedReducers = new LinkedTransferQueue<Integer>();
            for (i = 0; i < rCount; i++) {
              unassignedReducers.add(i);
            }
            HashMap<String, Integer> finishedReducers = new HashMap<String, Integer>();
            // HashMap<UUID of the task, reducerId: from 0 to rCount>
            HashMap<String, Integer> reducerTasks = new HashMap<String, Integer>();
            
            HashSet<Integer> failedWorkers = new HashSet<Integer>();
            
            RemoteFileInterface folder = null;
            
            // get content of the directory in parameter
            String path = normalizePath(pathToData);
            
            folder = dfs.getFile(path);
            if (!folder.exists()) {
              log(0, "Directory \"" + folder.remoteToString() + "\" doesn't exist\n");
              folder = null;
            } else if (!folder.isDirectory()) {
              log(0, "\"" + folder.remoteToString() + "\" is not a directory\n");
              folder = null;
            }
            
            if (folder != null) {
              RemoteFileInterface[] listOfFiles = folder.listFiles();
              String pattern = folder.remoteToString() + (folder.remoteToString().equals("\\") ? "" : "\\"); 
              if (listOfFiles.length == 0) {
                log(0, "Folder \"" + folder.remoteToString() + "\" is empty\n");
              } else {
                log(0, "Folder \"" + folder.remoteToString() + "\" contains:\n");
                for (i = 0; i < listOfFiles.length; i++) {
                  log(0, "  " + listOfFiles[i].remoteToString().replaceFirst(Pattern.quote(pattern), "") + "\n");
                  if (listOfFiles[i].isFile()) {
                    unassignedMappers.add(listOfFiles[i].remoteToString());
                    log(0, "    " + listOfFiles[i].remoteToString() + " was added as map task\n");
                  }
                }
              }
            }
            
            
            // worker node to start assigning tasks from
            i = randomWithRange(0, workerNodes.length - 1);

            /*
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
              i = (i + 1) % workerNodes.length;
            }
            */
            
            // while we have incomplete reduce tasks
            while ((unassignedReducers.size() > 0 || workingReducers.size() > 0) && failedWorkers.size() < workerNodes.length) {
              log(0, "tw: " + workerNodes.length + ", i: " + i + ", m: " + mCount + ", r: " + rCount + ", wr: " + workingReducers.size() + ", fr: " + finishedReducers.size() + ", ur: " + unassignedReducers.size() + ", fw: " + failedWorkers.size() + "\n");
              
              // while we have incomplete map tasks
              while ((unassignedMappers.size() > 0 || workingMappers.size() > 0) && failedWorkers.size() < workerNodes.length) {
                log(0, "tw: " + workerNodes.length + ", i: " + i + ", m: " + mCount + ", r: " + rCount + ", wm: " + workingMappers.size() + ", fm: " + finishedMappers.size() + ", um: " + unassignedMappers.size() + ", fw: " + failedWorkers.size() + "\n");
                
                // while we have unassigned map tasks and number of assigned tasks leess than mCount
                while (unassignedMappers.size() > 0 && workingMappers.size() < mCount && failedWorkers.size() < workerNodes.length) {
                  if (failedWorkers.contains(i)) {
                    // if current worker failed move to next
                    i = (i + 1) % workerNodes.length;
                    continue;
                  }
                  
                  jm = unassignedMappers.poll();
                  try {
                    // TODO:
                    String jobUUID = UUID.randomUUID().toString();
                    workerNodes[i].addJob(jobUUID, MapReduce.TYPE_MAPPER, pathToJar, className, jm, null, 0, rCount, null);
                    mapperTasks.put(jobUUID, jm);
                    workingMappers.put(jobUUID, i); // add map task to the list of working mappers
                    log(0, "Added map task " + jm + " to the worker no. " + i + "\n");
                  } catch (Exception e) {
                    log(0, "Exception while executing mapper job no. " + jm + "\n");
                    e.printStackTrace();
                    unassignedMappers.add(jm); // return map back to the list of unassigned map tasks
                    failedWorkers.add(i); // add worker to the list of failed workers
                  }
                  // move to next worker
                  i = (i + 1) % workerNodes.length;
                }
                
                // iterate through all tasks and get their status
                Iterator<Entry<String, Integer>> itm = workingMappers.entrySet().iterator();
                while (itm.hasNext()) {
                  Map.Entry<String, Integer> pair = itm.next();
                  String mTaskId = pair.getKey();
                  int workerId = pair.getValue();
                  int jobState = workerNodes[workerId].getJobState(mTaskId);
                  
                  switch (jobState) {
                    case Job.STATE_DONE:
                      log(0, "Mapper task no. " + mTaskId + " has been finished\n");
                      log(0, "Added it to the list of finished tasks\n");
                      itm.remove(); //workingMappers.remove(taskId);
                      finishedMappers.put(mTaskId, workerId);
                      break;
                    case Job.STATE_INPROGRESS:
                      // do nothing
                      break;
                    case Job.STATE_FAILED:
                      log(0, "Mapper task no. " + mTaskId + " has failed on worker no. " + workerId + "\n");
                      log(0, "Added it to the list of failed workers\n");
                      itm.remove(); //workingMappers.remove(mTaskId);
                      unassignedMappers.add(mapperTasks.get(mTaskId));
                      failedWorkers.add(workerId);
                      break;  
                    case Job.STATE_STOPPED:
                      // do nothing
                      break;
                    default:
                      throw new UnknownJobStateException();
                  }
                  //it.remove(); // avoids a ConcurrentModificationException
                }
                
                Thread.sleep(jobTrackerTimeout);
              }
              if (failedWorkers.size() < workerNodes.length) {
                log(0, "All map tasks are finished\n");
              }
              
              // while we have unassigned reducers tasks and number of assigned tasks less than rCount
              // all map tasks should be finished before running this
              while (unassignedMappers.size() == 0 
                      && workingMappers.size() == 0 
                      && unassignedReducers.size() > 0 
                      && workingReducers.size() < rCount 
                      && failedWorkers.size() < workerNodes.length) {
                if (failedWorkers.contains(i)) {
                  // if current worker failed move to next
                  i = (i + 1) % workerNodes.length;
                  continue;
                }
                
                jr = unassignedReducers.poll();
                try {
                  // TODO:
                  String jobUUID = UUID.randomUUID().toString();
                  workerNodes[i].addJob(jobUUID, MapReduce.TYPE_REDUCER, pathToJar, className, pathToResults, peers, jr, rCount, finishedMappers);
                  reducerTasks.put(jobUUID, jr);
                  workingReducers.put(jobUUID, i); // add map task to the list of working mappers
                  log(0, "Added reduce task " + jr + " to the worker no. " + i + "\n");
                } catch (Exception e) {
                  log(0, "Exception while executing reducer job no. " + jr + "\n");
                  e.printStackTrace();
                  unassignedReducers.add(jr); // return map back to the list of unassigned map tasks
                  failedWorkers.add(i); // add worker to the list of failed workers
                }
                // move to next worker
                i = (i + 1) % workerNodes.length;
              }
              
              // iterate through all reduce tasks and get their status
              Iterator<Entry<String, Integer>> itr = workingReducers.entrySet().iterator();
              while (itr.hasNext()) {
                Map.Entry<String, Integer> pair = itr.next();
                String rTaskId = pair.getKey();
                int workerId = pair.getValue();
                int jobState = workerNodes[workerId].getJobState(rTaskId);
                
                switch (jobState) {
                  case Job.STATE_DONE:
                    log(0, "Reducer task no. " + rTaskId + " has been finished\n");
                    log(0, "Added it to the list of finished tasks\n");
                    itr.remove(); //workingReducers.remove(rTaskId);
                    finishedReducers.put(rTaskId, workerId);
                    break;
                  case Job.STATE_INPROGRESS:
                    // do nothing
                    break;
                  case Job.STATE_FAILED:
                    log(0, "Reducer task no. " + rTaskId + " has failed on worker no. " + workerId + "\n");
                    log(0, "Added it to the list of failed workers\n");
                    itr.remove(); //workingMappers.remove(taskId);
                    unassignedReducers.add(reducerTasks.get(rTaskId));
                    failedWorkers.add(workerId);
                    break;  
                  case Job.STATE_STOPPED:
                    // do nothing
                    break;
                  default:
                    throw new UnknownJobStateException();
                }
                //it.remove(); // avoids a ConcurrentModificationException
              }
              
              Thread.sleep(jobTrackerTimeout);
            }
            
            if (failedWorkers.size() == workerNodes.length) {
              log(0, "Error: There are no available worker nodes to add jobs! Restart job completely!\n");
            } else {
              log(0, "All reduce tasks have been finished\n");
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
}
