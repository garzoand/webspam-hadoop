/**
 * HostSummaryReducer.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.reduce;

import hu.sztaki.ilab.bigdata.spam.aggregators.MeanVarianceAggregator;
import hu.sztaki.ilab.bigdata.spam.aggregators.SumAggregator;
import hu.sztaki.ilab.bigdata.spam.enums.FeatureNames;
import hu.sztaki.ilab.bigdata.spam.enums.features.HostSummaryFeatures;

/**
 *
 * @author garzo
 */
public class HostSummaryReducer extends AbstractContentProcessorReducer {
   
    protected static void setupAggregators(AbstractContentProcessorReducer reducer) {
        reducer.addFeatureAggregator(HostSummaryFeatures.HomePageNum.index(),
                FeatureNames.getFeatureName(HostSummaryFeatures.HomePageNum.index()),
                new SumAggregator());

        reducer.addFeatureAggregator(HostSummaryFeatures.OverallWordcount.index(),
                FeatureNames.getFeatureName(HostSummaryFeatures.OverallWordcount.index()),
                new SumAggregator());

        reducer.addFeatureAggregator(HostSummaryFeatures.Wordcount.index(),
                FeatureNames.getFeatureName(HostSummaryFeatures.Wordcount.index()),
                new MeanVarianceAggregator());        

        reducer.addFeatureAggregator(HostSummaryFeatures.OrigContentSize.index(),
                FeatureNames.getFeatureName(HostSummaryFeatures.OrigContentSize.index()),
                new SumAggregator());
    }
    
    @Override
    protected void setFeatureAggregators() {
        setupAggregators(this);
    }

}
