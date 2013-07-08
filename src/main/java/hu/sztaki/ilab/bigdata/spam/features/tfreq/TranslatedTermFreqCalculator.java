/**
 * TranslatedFreqCalculator.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.features.tfreq;

import hu.sztaki.ilab.bigdata.common.dictionary.MultiLangDictionary;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.spam.features.content.AbstractContentFeatureCalculator;
import java.util.List;

/**
 *
 * @author garzo
 */
public class TranslatedTermFreqCalculator extends AbstractContentFeatureCalculator {
    
    private MultiLangDictionary dictionary = null;

    public void setDictionary(MultiLangDictionary dictionary) {
        this.dictionary = dictionary;       
    }    
    
    @Override
    public boolean processContent(ParseResult result, HomePageMetaData meta) {
        super.processContent(result, meta);

        if (dictionary == null || stemmer == null) {
            LOG.error("FATAL: dictionary not set!");
            return false;
        }
        
        double[] foundWords = new double[dictionary.getWordsNum()];
        for (int i = 0; i < dictionary.getWordsNum(); ++i)
            foundWords[i] = 0.0;

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
        while ((token = stemmer.next()) != null) {
            if (token.getValue().length() == 0)
                continue;
            if (dictionary.isOriginalWord(token.getValue())) {
                foundWords[dictionary.getWordId(token.getValue())] += 1.0;
            } else if (dictionary.isTranslatedWord(token.getValue())) {
                List<String> translations =
                        dictionary.reverseTranslate(token.getValue());
                for (String word : translations) {
                    foundWords[dictionary.getWordId(word)] += (double)1 / (double)translations.size();
                }
            }
            counter++;
        }

        for (int i = 0; i < dictionary.getWordsNum(); i++) {
            if (foundWords[i] != 0) {
                addRawFeature(i, foundWords[i]);
            }
        }
        addRawFeature(dictionary.getWordsNum(), counter);
        
        return true;
    }

}
