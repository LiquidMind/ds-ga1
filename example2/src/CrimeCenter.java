import mapreduce.utils.MapReduce;
import mapreduce.utils.OutputCollector;
import mapreduce.utils.ReducerCollector;

import java.nio.DoubleBuffer;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by Aidar on 11.08.2015.
 * This calculates the average coordinates of all crimes that are related to drugs - find a Drug center of the UK :)
 */
public class CrimeCenter implements MapReduce {
    @Override
   /* public void map(String key, String value, OutputCollector collector) {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, ",", false);

        int i=1;
        while (tokenizer.hasMoreTokens()) {
            String token=tokenizer.nextToken();
            Double lat = .0, lng = .0;
            if (i==5){
                lng=Double.parseDouble(token);
            }
            if (i==6){
                lat=Double.parseDouble(token);
            }
            if (i==10 && token.trim().toLowerCase().equals("drugs")){
                collector.collect("lng", lng.toString());
                collector.collect("lat", lat.toString());
            }
            i++;
        }
    }*/

    public void map(String key, String value, OutputCollector collector) {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, ",", false);

        int i=1;
        while (tokenizer.hasMoreTokens()) {
            String token=tokenizer.nextToken();
            String date="";
            if (i==2){
                date=token;
            }

            if (i==11 && token=="Investigation complete"){
                collector.collect(date, 1);
                return;
            }
            i++;
        }
    }

    @Override
    /*public void reduce(String key, Iterator values, ReducerCollector collector) {
        Double sum=0.0;
        double count=0.0;
        while (values.hasNext()) {
            sum += (Double) values.next();
            count++;
        }
        if (count>0) {
            collector.collect(key, sum/count);
        }
    }*/

    public void reduce(String key, Iterator values, ReducerCollector collector) {
        Integer sum=0;
        while (values.hasNext()) {
            sum += (Integer) values.next();
        }

        collector.collect(key, sum);
    }
}
