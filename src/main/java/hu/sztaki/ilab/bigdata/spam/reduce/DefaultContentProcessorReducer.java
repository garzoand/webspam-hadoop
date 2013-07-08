/**
 * DefaultContentProcessorReducer.java
 * Basic content and query/corpus prec/recall features
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.reduce;

import hu.sztaki.ilab.bigdata.spam.enums.features.QueryPrecisionRecallFeatures;

/**
 *
 * @author garzo
 */
public class DefaultContentProcessorReducer extends AbstractContentProcessorReducer {

    @Override
    protected void setFeatureAggregators() {                
        BasicContentProcessorReducer.setupAggregators(this);
        PrecisionRecallReducer.setupAggregators(this, QueryPrecisionRecallFeatures.FEATURE_TYPE_QUERY);
        HostSummaryReducer.setupAggregators(this);
    }

}
