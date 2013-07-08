package hu.sztaki.ilab.bigdata.spam.aggregators;

/**
 *
 * @author garzo
 */
public interface Aggregator {

    public void reset();
    public void add(double feature);
    public double[] aggregate();
    public String[] getLabels();

}
