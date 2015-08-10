package mapreduce.utils;

/**
 * Created by Aidar on 09.08.2015.
 */
public interface MapReduce {
    /**
     * Constants provided
     */
    public static final byte TYPE_MAPPER=0;
    public static final byte TYPE_REDUCER=1;

    public void map(String key, String value, OutputCollector collector);
    public void reduce();
}
