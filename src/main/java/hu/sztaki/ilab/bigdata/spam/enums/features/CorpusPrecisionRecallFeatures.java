package hu.sztaki.ilab.bigdata.spam.enums.features;

import hu.sztaki.ilab.bigdata.spam.enums.ItemSets;

/**
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 * 
 */
public enum CorpusPrecisionRecallFeatures implements IPrecisionRecallFeature {

    /** Query precision/recall */
    CORPUS_PRECISION_100(
            CorpusPrecisionRecallFeatures.CORPUS_PRECISION_BASE_INDEX,
            "content_prec",
            ItemSets.K_FREQUENCY_100.getSize()),

    CORPUS_RECALL_100(
            CorpusPrecisionRecallFeatures.CORPUS_RECALL_BASE_INDEX,
            "content_recall",
            ItemSets.K_FREQUENCY_100.getSize()),

    CORPUS_PRECISION_200(
            CorpusPrecisionRecallFeatures.CORPUS_PRECISION_BASE_INDEX,
            "content_prec",
            ItemSets.K_FREQUENCY_200.getSize()),

    CORPUS_RECALL_200(
            CorpusPrecisionRecallFeatures.CORPUS_RECALL_BASE_INDEX,
            "content_recall",
            ItemSets.K_FREQUENCY_200.getSize()),

    CORPUS_PRECISION_500(
            CorpusPrecisionRecallFeatures.CORPUS_PRECISION_BASE_INDEX,
            "content_prec",
            ItemSets.K_FREQUENCY_500.getSize()),

    CORPUS_RECALL_500(
            CorpusPrecisionRecallFeatures.CORPUS_RECALL_BASE_INDEX,
            "content_recall",
            ItemSets.K_FREQUENCY_500.getSize()),

    CORPUS_PRECISION_1000(
            CorpusPrecisionRecallFeatures.CORPUS_PRECISION_BASE_INDEX,
            "content_prec",
            ItemSets.K_FREQUENCY_1000.getSize()),

    CORPUS_RECALL_1000(
            CorpusPrecisionRecallFeatures.CORPUS_RECALL_BASE_INDEX,
            "content_recall",
            ItemSets.K_FREQUENCY_1000.getSize()),

    // For unit testing
    CORPUS_PRECISION_8(
            CorpusPrecisionRecallFeatures.CORPUS_PRECISION_BASE_INDEX,
            "content_prec",
            ItemSets.K_FREQUENCY_8.getSize()),

    CORPUS_RECALL_8(
            CorpusPrecisionRecallFeatures.CORPUS_RECALL_BASE_INDEX,
            "content_recall", ItemSets.K_FREQUENCY_8.getSize());

    public static final String FEATURE_TYPE_CORPUS = "corpus";
    public static final int CORPUS_PRECISION_BASE_INDEX = 300000;
    public static final int CORPUS_RECALL_BASE_INDEX = 300001;

    private final int index;
    private final int freq;
    private final String featureName;

    private CorpusPrecisionRecallFeatures(int index, String featureName, int freq) {
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
        return CORPUS_PRECISION_BASE_INDEX;
    }

    public int getRecallBaseIndex() {
        return CORPUS_RECALL_BASE_INDEX;
    }
}
