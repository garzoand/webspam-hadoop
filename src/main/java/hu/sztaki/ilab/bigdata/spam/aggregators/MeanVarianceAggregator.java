/**
 * MeanVarianceAggregator.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.aggregators;

import hu.sztaki.ilab.bigdata.common.utils.IncrementalMeanCalculator;

/**
 *
 * @author garzo
 */
public final class MeanVarianceAggregator implements Aggregator {

    private IncrementalMeanCalculator calculator = new IncrementalMeanCalculator();

    public void reset() {
        calculator.reset();
    }

    public void add(double feature) {
        this.calculator.add(feature);
    }

    public double[] aggregate() {
        double[] result = new double[2];
        result[0] = calculator.getMean();
        result[1] = calculator.getStDev();
        return result;
    }

    public String[] getLabels() {
        String[] labels = new String[2];
        labels[0] = "_avg";
        labels[1] = "_stdev";
        return labels;
    }
   
}
