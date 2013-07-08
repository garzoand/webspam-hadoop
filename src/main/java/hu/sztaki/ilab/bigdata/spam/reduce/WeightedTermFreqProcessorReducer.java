/**
 * WeightedTermFreqProcessorReducer.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.reduce;

import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryStore;
import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;
import hu.sztaki.ilab.bigdata.spam.aggregators.SumAggregator;
import hu.sztaki.ilab.bigdata.spam.constants.Constants;
import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import hu.sztaki.ilab.bigdata.spam.termweighting.ITermWeightingStrategy;
import java.io.IOException;
import java.util.Map;

/**
 *
 * @author garzo
 */
public class WeightedTermFreqProcessorReducer extends AbstractContentProcessorReducer {
    
    protected ITermWeightingStrategy termWeighter = null;

    @Override
    protected void setup(Context context) throws IOException {
        super.setup(context);
        try {
            String className = context.getConfiguration()
                    .get(SpamConfigNames.CONF_TERMW_STRATEGY, SpamConfigNames.DEFAULT_TERMW_STRATEGY);
            Class termWeighterClass = Class.forName(className);
            termWeighter = (ITermWeightingStrategy)termWeighterClass.newInstance();
            termWeighter.setup(context.getConfiguration());
            LOG.info("Term weighter class was instantiated: " + className);
        } catch (Exception ex) {
            LOG.error("Exception: " + ex.getMessage());
        }
    }

    @Override
    protected void setFeatureAggregators() {
        for (int i = 0; i < getDictionary().getWordsNum(); i++) {
            addFeatureAggregator(i, getDictionary().getWord(i), new SumAggregator());
        }
        addFeatureAggregator(getDictionary().getWordsNum(), "SPAM_length", new SumAggregator());
        addFeatureFilter("SPAM_length_sum");
    }

    @Override
    protected void postFeatureProcessing(Map<String, Double> featureMap) {
        long documentLength = ((Double)(featureMap.get("SPAM_length_sum"))).longValue();
        DictionaryStore dictionary = getDictionary();
        for (String featureName : featureMap.keySet()) {
            if (!featureName.equals("SPAM_length_sum")) {
                long df = dictionary.getDocumentFrequency(dictionary.getWordId(featureName.replaceFirst("_sum", "")));
                long tf = featureMap.get(featureName).longValue();
                double weighted_tf = 0;
                if (tf > 0) {
                    weighted_tf = termWeighter.calculate(tf, df, documentLength);
                }
                featureMap.put(featureName, weighted_tf);
            }
        }
    }

    @Override
    protected FeatureOutputRecord createFeatureOutputRecord(Map<String, Double> featureMap) {
        FeatureOutputRecord record = new FeatureOutputRecord();
        for (String featureName : featureMap.keySet()) {
            double featureValue = featureMap.get(featureName);
            if (!isFilteredFeature(featureName) && featureValue > 0.0) {
                String fn = featureName.replace("_sum", "");
                String fid = ((Integer)(getDictionary().getWordId(fn))).toString();
                record.addFeature(fid, featureValue);
            }
        }
        return record;
    }

}
