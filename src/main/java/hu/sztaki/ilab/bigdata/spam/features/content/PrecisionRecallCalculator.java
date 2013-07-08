package hu.sztaki.ilab.bigdata.spam.features.content;

import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryStore;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.spam.enums.features.IFeature;
import hu.sztaki.ilab.bigdata.spam.enums.features.IPrecisionRecallFeature;

import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base class for calculating Corpus/Query precision recall features
 * 
 * <pre>
 * Q = Frequent terms in the query log/corpus
 * T = Terms in the page
 * 
 * Precision = (|Q/\T|)/|T|
 * Recall = (|Q/\T|)/|Q|
 * </pre>
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 *
 */
public abstract class PrecisionRecallCalculator<E extends Enum<E> & IPrecisionRecallFeature> extends AbstractContentFeatureCalculator {

    private static final Log LOG = LogFactory.getLog(PrecisionRecallCalculator.class);
    
    private Class<E> precRecallType;
    
    private DictionaryStore dictionary = null;
    
    /* Number of tokens in the document content */ 
    private int tokenContentCount = 0;
    
    /* Number of tokens in the document title */ 
    private int tokenTitleCount = 0;
    
    /*Number of considered frequent query terms */
    private int kFrequentNum = 0; //see Constants.K_FREQUENCY_XXX
    
    /* Number of frequent query terms found in the document content*/
    private int frequentContentTerms = 0;
    
    /* Number of frequent query terms found in the document title*/
    private int frequentTitleTerms = 0;

    public PrecisionRecallCalculator(int kFrequentNum, Class<E> precRecallType) {
        this.kFrequentNum = kFrequentNum;
        this.precRecallType = precRecallType;
    }

    @Override
    public boolean processContent(ParseResult result, HomePageMetaData meta) {
        super.processContent(result, meta);
        
        if (dictionary == null) {
            LOG.error("FATAL: dictionary not set!");
            return false;
        }
        
        //limit size of the dictionary
        dictionary = dictionary.getDictionaryStoreRange(kFrequentNum);
        
        processTokens(result.getTokenizedContent(), false);
        processTokens(result.getTitle(), true);
        
        IFeature queryPrecision = null;
        IFeature queryRecall = null;
        
        for (E feature : EnumSet.allOf(precRecallType)) {
            if (feature.index() ==  (feature.getPrecisionBaseIndex() + kFrequentNum)) {
                queryPrecision = feature;
            }
            else if (feature.index() == (feature.getRecallBaseIndex() + kFrequentNum)) {
                queryRecall = feature;
            }
        }
        
        addFeature(queryPrecision, getPrecision());
        addFeature(queryRecall, getRecall());
        return true;
    }
    
    private void processTokens(String data, boolean isTitle) {

        ITokenStream stream = 
            new PorterStemmerFilter(
            new LowerCaseFilter(
            new WordFilter(
            new StringTokenizerStream(data))));

        Token token = null;

        while ((token = stream.next()) != null) {
            if (dictionary.containsWord(token.getValue())) {
                if (isTitle) {
                    frequentTitleTerms++;
                }
                else {
                    frequentContentTerms++;
                }
            }
            if (isTitle) {
                tokenTitleCount++;
            }
            else {
                tokenContentCount++;
            }
        }
    }
    
    /**
     * @return - precision of title and content
     */
    private double getPrecision() {
        return (frequentContentTerms / (double)tokenContentCount) + (frequentTitleTerms / (double)tokenTitleCount);
    }
    
    /**
     * @return - recall of title and content
     */
    private double getRecall() {
        return (frequentContentTerms / (double)kFrequentNum) + (frequentTitleTerms / (double)kFrequentNum);
    }

    public void setDictionary(DictionaryStore dictionary) {
        this.dictionary = dictionary;
    }
    
}
