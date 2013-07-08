/**
 * BM25TermWeighting.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.termweighting;

import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author garzo
 */
public class BM25TermWeighting implements ITermWeightingStrategy {

    protected double collectionSize;
    protected double avgDocumentSize;
    private final double K1 = 1.0f;
    private final double B = 0.5f;
    
    public void setup(Configuration conf) {
        setCollectionSize(conf.getLong(SpamConfigNames.CONF_TERMW_COLLECTIONSIZE,
                SpamConfigNames.DEFAULT_TERMW_COLLECTIONSIZE));
        setAvgDocumentSize(conf.getLong(SpamConfigNames.CONF_TERMW_AVGDOCSIZE,
                SpamConfigNames.DEFAULT_TERMW_AVGDOCSIZE));
    }
    
    public void setCollectionSize(long collectionSize) {
        this.collectionSize = (double)collectionSize;
    }

    public void setAvgDocumentSize(long avgDocumentSize) {
        this.avgDocumentSize = (double)avgDocumentSize;
    }    

    public double calculate(long termFreq, long documentFreq, long documentSize) {
        double N = collectionSize;
        double n = documentFreq;
        double w = Math.log(1.0 / ((n + 0.5) / (N - n + 0.5)));
        double tf = termFreq;
        double K = K1 * ((1.0 - B) +
                B * (double)documentSize / avgDocumentSize);               
        return w * (K1 + 1.0) * tf / (K + tf);
    }
    
}
