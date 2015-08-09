import mapreduce.utils.MapReduce;

/**
 * Created by Aidar on 09.08.2015.
 */
public class TestMapReduce implements MapReduce {
    @Override
    public void map() {
        System.out.println("Test map works!");
    }

    @Override
    public void reduce() {
        System.out.println("Test reduce works!");
    }

    /**
     * Plumber
     */
    public void prepare(){

    }
}
