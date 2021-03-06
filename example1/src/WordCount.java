import mapreduce.utils.MapReduce;
import mapreduce.utils.OutputCollector;
import mapreduce.utils.ReducerCollector;

import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by Aidar on 09.08.2015.
 */
public class WordCount implements MapReduce {
    @Override
    public void map(String key, String value, OutputCollector collector) {
//        System.out.println("Test map works!");
        String line = value.toString();
//        StringTokenizer tokenizer = new StringTokenizer(line, ",", false);
        
        String reg = "[^a-zA-Z'-]+";
        
        String[] res = line.split(reg);
        String word;
        for (int i = 0; i < res.length; i++) {
          word = res[i].trim().toLowerCase();
          if (word.length() > 0) {
            collector.collect(word, 1);
          }
        }
        /*
        StringTokenizer tokenizer = new StringTokenizer(line, " ", false);
        String word="";
        while (tokenizer.hasMoreTokens()) {
            word=tokenizer.nextToken();
            collector.collect(word, 1);
        }
        */
    }

    @Override
    public void reduce(String key, Iterator values, ReducerCollector collector) {
        //System.out.println("Test reduce works!");
        Integer sum=0;
        while (values.hasNext()) {
            sum += (Integer) values.next();
        }
        collector.collect(key, sum);
    }

    /**
     * Plumber
     */
    public void prepare(){

    }
}
