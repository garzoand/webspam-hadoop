package hu.sztaki.ilab.bigdata.spam.features.content;

import hu.sztaki.ilab.bigdata.common.dictionary.FileDictionaryStore;
import hu.sztaki.ilab.bigdata.spam.MockParserResult;
import hu.sztaki.ilab.bigdata.spam.enums.features.QueryPrecisionRecallFeatures;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for Query precision/recall features.
 * Corpus feature testing would be the same
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 * 
 */
public class PrecisionRecallCalculatorTest {

    private MockParserResult parserResult = null;
    private FileDictionaryStore dictionary = null;
    private int k = 8;

    @Before
    public void init() {

        parserResult = new MockParserResult.MockParserBuilder("http://host1.com")
                .title("money is free enterprise")
                .content("cheap academic camera degree education").build();

        // init dictionary
        Map<String, Integer> dictItem = new HashMap<String, Integer>(k);
        dictItem.put("and", 0);
        dictItem.put("free", 1);
        dictItem.put("have", 2);
        dictItem.put("music", 3);
        dictItem.put("cheap", 4);
        dictItem.put("wiki", 5);
        dictItem.put("money", 6);
        dictItem.put("camera", 7);
        dictItem.put("xxx", 8);
        dictItem.put("download", 9);

        String[] words = { "and", "free", "have", "music", "cheap", "wiki", "money", "camera",
                "xxx", "download" };
        long[] DF = { 1232, 6565, 67, 99567, 121034, 4545, 8378, 23232, 8324, 7883 };
        dictionary = new FileDictionaryStore(dictItem, words, DF);

    }

    @Test
    public void testQueryPrecisionRecall() {

        PrecisionRecallCalculator<QueryPrecisionRecallFeatures> calc = 
            new PrecisionRecallCalculator<QueryPrecisionRecallFeatures>(
                k, QueryPrecisionRecallFeatures.class) {
        };

        calc.setDictionary(dictionary);
        calc.processContent(parserResult.getParseResult(), null);

        @SuppressWarnings("unchecked")
        Map<Integer, Double> features = calc.getFeatures();

        double precision = features.get(QueryPrecisionRecallFeatures.QUERY_PRECISION_BASE_INDEX + k);
        double recall = features.get(QueryPrecisionRecallFeatures.QUERY_RECALL_BASE_INDEX + k);

        Assert.assertEquals("Precision calculation failed", 0.65, precision);
        Assert.assertEquals("Recall calculation failed", 0.375, recall);

    }
}
