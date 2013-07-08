/**
 * HostSummaryReducer.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.reduce;

import hu.sztaki.ilab.bigdata.spam.aggregators.MeanVarianceAggregator;
import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import hu.sztaki.ilab.bigdata.spam.enums.FeatureNames;
import hu.sztaki.ilab.bigdata.spam.enums.features.CorpusPrecisionRecallFeatures;
import hu.sztaki.ilab.bigdata.spam.enums.features.IPrecisionRecallFeature;
import hu.sztaki.ilab.bigdata.spam.enums.features.QueryPrecisionRecallFeatures;

import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;

/**
 * Base reducer class for Corpus/Query precision/recall features
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 * 
 */
public class PrecisionRecallReducer extends AbstractContentProcessorReducer {

    @Override
    protected void setFeatureAggregators() {
        Configuration conf = getConf();
        String featureType = conf.get(SpamConfigNames.CONF_PREC_RECALL_FEATURE_TYPE);
        setupAggregators(this, featureType);
    }

    protected static void setupAggregators(AbstractContentProcessorReducer reducer,
            String featureType) {

        if (CorpusPrecisionRecallFeatures.FEATURE_TYPE_CORPUS.equals(featureType)) {
            addFeatures(reducer, CorpusPrecisionRecallFeatures.class);
        }
        else {
            addFeatures(reducer, QueryPrecisionRecallFeatures.class);
        }
    }

    private static <E extends Enum<E> & IPrecisionRecallFeature> void addFeatures(
            AbstractContentProcessorReducer reducer, Class<E> featureType) {

        for (E feature : EnumSet.allOf(featureType)) {
            reducer.addFeatureAggregator(feature.index(),
                    FeatureNames.getFeatureName(feature.index()), new MeanVarianceAggregator());
        }
    }

}
