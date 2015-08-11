package mapreduce.utils;

import mapreduce.node.SysLogger;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by Aidar on 10.08.2015.
 */
public class OutputCollector<KEYTYPE, VALTYPE> {
    private Map<KEYTYPE, ArrayList<VALTYPE>> map;

    public OutputCollector() {
        this.map = new TreeMap<KEYTYPE, ArrayList<VALTYPE>>();
    }

    public void collect(KEYTYPE key, VALTYPE val){
        if (!map.containsKey(key)){
            map.put(key, new ArrayList<VALTYPE>());
        }
        //todo track whether the value in hash map changes
        map.get(key).add(val);
    }

    public void spill(String baseFilename, int numberOfReducers){
        //create a list for each of the reducer
        ArrayList<TreeMap<KEYTYPE, ArrayList<VALTYPE>>> reducerMaps = new ArrayList<TreeMap<KEYTYPE, ArrayList<VALTYPE>>>();

        //init array
        for (int i=0;i<numberOfReducers;i++){
            reducerMaps.add(new TreeMap<KEYTYPE, ArrayList<VALTYPE>>());
        }

        Set<KEYTYPE> keys=map.keySet();
        for (KEYTYPE key: keys){
            //get the list
            ArrayList<VALTYPE> list = map.get(key);
            //serialize it into the file
            //get the id of server
            MessageDigest mDigest = null;
            try {
                mDigest = MessageDigest.getInstance("SHA1");
                byte[] digest = mDigest.digest(key.toString().getBytes());
                //get id
                int serverNumber=Math.abs(digest[digest.length-1]%numberOfReducers);
                //todo store to the array
                reducerMaps.get(serverNumber).put(key, map.get(key));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

        }
        //todo store each array to separate file
        for (int i=0;i<numberOfReducers;i++){
            File file = new File(baseFilename+i);
            FileOutputStream f = null;
            try {
                f = new FileOutputStream(file);
                ObjectOutputStream s = new ObjectOutputStream(f);
                s.writeObject(reducerMaps.get(i));
                s.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
