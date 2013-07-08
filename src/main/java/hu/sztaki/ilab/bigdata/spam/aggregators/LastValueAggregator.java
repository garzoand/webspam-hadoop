/**
 * LastValueAggregator.java
 * Returns the last value added to aggregator. Used for max mp and hp features.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.aggregators;

/**
 *
 * @author garzo
 */
public final class LastValueAggregator implements Aggregator {

    private double lastValue = 0;
    
    public LastValueAggregator() {
        reset();
    }
    
    @Override
    public void reset() {
        this.lastValue = 0;
    }

    @Override
    public void add(double feature) {
        this.lastValue = feature;
    }

    @Override
    public double[] aggregate() {
        double[] result = new double[1];
        result[0] = this.lastValue;
        return result;
    }

    @Override
    public String[] getLabels() {
        String[] labels = new String[1];
        labels[0] = "";
        return labels;
    }
    
}
