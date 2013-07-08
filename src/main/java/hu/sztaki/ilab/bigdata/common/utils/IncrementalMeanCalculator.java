/**
 * IncrementalMeanCalculator.java
 * Calculates mean, variance and standard deviation incrementally.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.utils;

/**
 *
 * @author garzo
 */
public class IncrementalMeanCalculator {

    int num = 0;
    double mean = 0.0;
    double variance = 0.0;

    public IncrementalMeanCalculator() {

    }

    // adds new value and recalculated mean and variance
    public void add(double value) {
        num++;
        double delta = value - mean;
        mean += delta / num;
        variance += delta * (value - mean);
    }

    public double getMean() {
        return mean;
    }

    public double getVariance() {
        if (num > 1) {
            return variance / (num - 1);
        } else {
            return variance;
        }
    }

    public double getStDev() {
        return Math.sqrt(getVariance());
    }

    public void reset() {
        num = 0;
        mean = 0;
        variance = 0;
    }
    
    public int getNum() {
        return num;
    }

}
