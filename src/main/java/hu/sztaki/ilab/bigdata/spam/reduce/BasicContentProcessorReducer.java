/**
 * BasicContentProcessorReducer.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.reduce;

import hu.sztaki.ilab.bigdata.spam.aggregators.LastValueAggregator;
import hu.sztaki.ilab.bigdata.spam.aggregators.MeanVarianceAggregator;
import hu.sztaki.ilab.bigdata.spam.enums.FeatureNames;
import hu.sztaki.ilab.bigdata.spam.enums.features.BasicContentFeatures;

/**
 *
 * @author garzo
 */
public class BasicContentProcessorReducer extends AbstractContentProcessorReducer {

    protected static void setupAggregators(AbstractContentProcessorReducer reducer) {
        for (BasicContentFeatures feature : BasicContentFeatures.values()) {
            
            /* avg and stddev */
            reducer.addFeatureAggregator(feature.index(),
                    FeatureNames.getFeatureName(feature.index()),
                    new MeanVarianceAggregator());
            
            /* max mp */
            reducer.addFeatureAggregator(feature.index_maxmp(),
                    FeatureNames.getFeatureName(feature.index_maxmp()),
                    new LastValueAggregator());

            /* hp */
            reducer.addFeatureAggregator(feature.index_hp(),
                    FeatureNames.getFeatureName(feature.index_hp()),
                    new LastValueAggregator());
        }
    }
    
    @Override
    protected void setFeatureAggregators() {
        setupAggregators(this);
    }
    
}
