/**
 * SubAggregator
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.aggregators;


/**
 *
 * @author garzo
 */
public class SumAggregator implements Aggregator {

    protected double sum;

    public SumAggregator() {
        reset();
    }

    public final void reset() {
        this.sum = 0;
    }

    public void add(double feature) {
        this.sum += feature;
    }

    public double[] aggregate() {
        double[] result = new double[1];
        result[0] = this.sum;
        return result;
    }

    public String[] getLabels() {
        String[] labels = new String[1];
        labels[0] = "_sum";
        return labels;
    }

}
