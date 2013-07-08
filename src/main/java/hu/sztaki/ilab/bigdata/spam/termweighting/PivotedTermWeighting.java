/**
 * PivotedTermWeighting.java
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
public class PivotedTermWeighting implements ITermWeightingStrategy {

    protected double collectionSize;
    protected double avgDocumentSize;
    private final double S = 0.8f;

    public PivotedTermWeighting() {
        
    }

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
        double tf = (1.0 + Math.log((double)termFreq)) /
                (1.0 - S + S * (double)documentSize / avgDocumentSize);
        double idf = 1.0;
        if (documentFreq > 0) {
            idf = Math.log(collectionSize / (double)documentFreq);
        }
        return tf * idf;
    }

}
