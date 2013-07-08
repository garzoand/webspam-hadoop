/**
 * ExistenceAggregator
 * Return 1 is any component is grater than zero.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.aggregators;

/**
 *
 * @author garzo
 */
public final class ExistenceAggregator implements Aggregator {

    private double value = 0;

    public ExistenceAggregator() {
        reset();
    }

    public void reset() {
        this.value = 0;
    }

    public void add(double feature) {
        if (feature > 0.0)
            this.value = 1.0;
    }

    public double[] aggregate() {
        double[] result = new double[1];
        result[0] = this.value;
        return result;        
    }

    public String[] getLabels() {
        String[] labels = new String[1];
        labels[0] = "";
        return labels;
    }

}
