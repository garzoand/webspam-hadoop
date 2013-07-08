package hu.sztaki.ilab.bigdata.spam.enums.features;

import hu.sztaki.ilab.bigdata.spam.enums.ItemSets;

/**
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 * 
 */
public enum QueryPrecisionRecallFeatures implements IPrecisionRecallFeature {

    /** Query precision/recall */
    QUERY_PRECISION_100(
            QueryPrecisionRecallFeatures.QUERY_PRECISION_BASE_INDEX,
            "query_prec",
            ItemSets.K_FREQUENCY_100.getSize()),

    QUERY_RECALL_100(
            QueryPrecisionRecallFeatures.QUERY_RECALL_BASE_INDEX,
            "query_recall",
            ItemSets.K_FREQUENCY_100.getSize()),

    QUERY_PRECISION_200(
            QueryPrecisionRecallFeatures.QUERY_PRECISION_BASE_INDEX,
            "query_prec",
            ItemSets.K_FREQUENCY_200.getSize()),

    QUERY_RECALL_200(
            QueryPrecisionRecallFeatures.QUERY_RECALL_BASE_INDEX,
            "query_recall",
            ItemSets.K_FREQUENCY_200.getSize()),

    QUERY_PRECISION_500(
            QueryPrecisionRecallFeatures.QUERY_PRECISION_BASE_INDEX,
            "query_prec",
            ItemSets.K_FREQUENCY_500.getSize()),

    QUERY_RECALL_500(
            QueryPrecisionRecallFeatures.QUERY_RECALL_BASE_INDEX,
            "query_recall",
            ItemSets.K_FREQUENCY_500.getSize()),

    QUERY_PRECISION_1000(
            QueryPrecisionRecallFeatures.QUERY_PRECISION_BASE_INDEX,
            "query_prec",
            ItemSets.K_FREQUENCY_1000.getSize()),

    QUERY_RECALL_1000(
            QueryPrecisionRecallFeatures.QUERY_RECALL_BASE_INDEX,
            "query_recall",
            ItemSets.K_FREQUENCY_1000.getSize()),

    // For unit testing
    QUERY_PRECISION_8(
            QueryPrecisionRecallFeatures.QUERY_PRECISION_BASE_INDEX,
            "query_prec",
            ItemSets.K_FREQUENCY_8.getSize()),

    QUERY_RECALL_8(
            QueryPrecisionRecallFeatures.QUERY_RECALL_BASE_INDEX,
            "query_recall", ItemSets.K_FREQUENCY_8.getSize());


    public static final String FEATURE_TYPE_QUERY = "query";
    public static final int QUERY_PRECISION_BASE_INDEX = 200000;
    public static final int QUERY_RECALL_BASE_INDEX = 200001;

    private final int index;
    private final int freq;
    private final String featureName;

    private QueryPrecisionRecallFeatures(int index, String featureName, int freq) {
        this.index = index;
        this.featureName = featureName;
        this.freq = freq;
    }

    public int index() {
        return this.index + freq;
    }

    public int index_maxmp() {
        return this.index + freq + 1;
    }

    public int index_hp() {
        return this.index + freq + 2;
    }

    public String featureName() {
        return this.featureName + "_" + freq;
    }

    public int getPrecisionBaseIndex() {
        return QUERY_PRECISION_BASE_INDEX;
    }

    public int getRecallBaseIndex() {
        return QUERY_RECALL_BASE_INDEX;
    }
}
