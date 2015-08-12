package mapreduce.utils;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Created by Aidar on 10.08.2015.
 */
public class ReducerCollector<KEYTYPE, VALTYPE>{
    protected Map<KEYTYPE, VALTYPE> map;

    public ReducerCollector() {

        this.map = new TreeMap<KEYTYPE, VALTYPE>();
    }

    public void collect(KEYTYPE key, VALTYPE val){
        if (!map.containsKey(key)){
            map.put(key, val);
        }
    }

    public void spill(String filename){
        //create a list for each of the reducer
        Set<KEYTYPE> keys=map.keySet();

        //store file
        File file = new File(filename);
        FileOutputStream f = null;
        try {
            f = new FileOutputStream(file);
            BufferedWriter writer = null;
            writer = new BufferedWriter(new FileWriter(file));
            for (KEYTYPE key: keys){
                writer.write(key.toString()+": \t"+map.get(key).toString()+"\n");
            }
            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
