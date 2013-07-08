/**
 * FeatureNames.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.enums;

import hu.sztaki.ilab.bigdata.spam.enums.features.BasicContentFeatures;
import hu.sztaki.ilab.bigdata.spam.enums.features.CorpusPrecisionRecallFeatures;
import hu.sztaki.ilab.bigdata.spam.enums.features.HostSummaryFeatures;
import hu.sztaki.ilab.bigdata.spam.enums.features.IFeature;
import hu.sztaki.ilab.bigdata.spam.enums.features.QueryPrecisionRecallFeatures;

import java.util.HashMap;

/**
 * @author garzo, Bendig Loránd <lbendig@ilab.sztaki.hu>
 * 
 */
public class FeatureNames {

    private FeatureNames() {
        throw new AssertionError("shouldn't be instantiated");
    }
    
    private static HashMap<Integer, String> featureNameMap = new HashMap<Integer, String>();
    
    static {
        
        /* basic content feature names */
        for (IFeature feature : BasicContentFeatures.values()) {
            addFeatureNames(feature);
        }
        
        /* Query prec/recall features */
        for (IFeature feature : QueryPrecisionRecallFeatures.values()) {
            addFeatureNames(feature);
        }
        
        /* Corpus prec/recall features */
        for (IFeature feature : CorpusPrecisionRecallFeatures.values()) {
            addFeatureNames(feature);
        }
        
        /* Host summary features */
        for (IFeature feature : HostSummaryFeatures.values()) {
            addFeatureNames(feature);
        }

    }
    
    private static void addFeatureNames(IFeature feature) {
        featureNameMap.put(feature.index(), feature.featureName());
        featureNameMap.put(feature.index_maxmp(), feature.featureName() + "_mp");
        featureNameMap.put(feature.index_hp(), feature.featureName() + "_hp");
    }
    
    public static String getFeatureName(int featureID) {
        String name = featureNameMap.get(featureID);
        if (name == null)
            return "N/A";
        return name;
    }

}
