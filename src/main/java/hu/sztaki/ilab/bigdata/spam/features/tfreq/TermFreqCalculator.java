/**
 * DocumentFreqCalculator.java
 * Feature calculator class for host based document frequency calculation
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.features.tfreq;

import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryStore;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.spam.constants.Constants;
import hu.sztaki.ilab.bigdata.spam.features.content.AbstractContentFeatureCalculator;

/**
 *
 * @author garzo
 */
public class TermFreqCalculator extends AbstractContentFeatureCalculator {

    private DictionaryStore dictionary = null;
    private int[] foundWords = new int[Constants.MaxTopWord];

    public void setDictionary(DictionaryStore dictionary) {
        this.dictionary = dictionary;
    }

    public boolean processContent(ParseResult result, HomePageMetaData meta) {
        super.processContent(result, meta);

        if (dictionary == null) {
            LOG.error("FATAL: dictionary not set!");
            return false;
        }
        for (int i = 0; i < Constants.MaxTopWord; ++i)
            foundWords[i] = 0;

        ITokenStream stream =                 
                new LowerCaseFilter(
                new WordFilter(
                new StringTokenizerStream(result.getTokenizedContent())));
        if (this.stemmer != null) {
            stemmer.setInput(stream);
            stream = stemmer;
        }
        
        Token token = null;

        long counter = 0;
        while ((token = stream.next()) != null) {
            if (dictionary.containsWord(token.getValue())) {
                int wordID = dictionary.getWordId(token.getValue());
                foundWords[wordID]++;
            }
            counter++;
        }

        for (int i = 0; i < Constants.MaxTopWord; i++) {
            if (foundWords[i] != 0) {                
                addRawFeature(i, foundWords[i]);
            }
        }
        addRawFeature(Constants.MaxTopWord, counter);
        
        return true;
    }
   
}
