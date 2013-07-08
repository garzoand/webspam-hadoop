package hu.sztaki.ilab.bigdata.spam.termweighting;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author garzo
 */
public interface ITermWeightingStrategy {

    public void setup(Configuration conf);
    public double calculate(long termFreq, long documentFreq, long documentSize);

}
