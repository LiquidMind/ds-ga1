package mapreduce.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

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
}
