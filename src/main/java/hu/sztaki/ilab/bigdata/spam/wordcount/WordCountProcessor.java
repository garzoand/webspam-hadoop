/**
 * WordCountProcessor.java
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount;

import hu.sztaki.ilab.bigdata.common.enums.EmitMode;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.*;
import hu.sztaki.ilab.bigdata.common.tokenize.strategy.ICUBasedTokenizingStrategy;
import hu.sztaki.ilab.bigdata.common.tokenize.strategy.ITokenizingStrategy;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author garzo
 */
public class WordCountProcessor {

    private IWordCountStrategy strategy = null;
    private char prefixChar;
    protected StopWordFilter stopWordFilter = null;
    protected ITokenFilter stemmer = null;
    protected ITokenizingStrategy tokenizer = new ICUBasedTokenizingStrategy();
    private static final Log LOG = LogFactory.getLog(WordCountProcessor.class);
    private int minLength = 2;
    
    public WordCountProcessor() {

    }

    public WordCountProcessor(IWordCountStrategy strategy) {
        setWordcountStrategy(strategy);
    }

    public void setPrefixChar(char c) {
        this.prefixChar = c;
    }

    public void setStemmer(ITokenFilter stemmer) {
        this.stemmer = stemmer;
    }
    
    public void setMinLength(int length) {
        this.minLength = length;
    }

    public char getPrefixChar() {
        return this.prefixChar;
    }

    public final void setWordcountStrategy(IWordCountStrategy strategy) {
        this.strategy = strategy;
    }

    public final IWordCountStrategy getWordcountStrategy() {
        return this.strategy;
    }

    public void setStopWordFilter(StopWordFilter stopWordFilter) {
        this.stopWordFilter = stopWordFilter;
    }

    protected ITokenStream buildTextStream(String input) {
        ITokenFilter filter = new LowerCaseFilter(new WordFilter(
                new StringTokenizerStream(tokenizer, input)));
        MinLengthFilter minLengthFilter = new MinLengthFilter(filter);
        minLengthFilter.setMinLength(minLength);
        filter = minLengthFilter;        
        
        if (stemmer != null) {
            stemmer.setInput(filter);            
            filter = stemmer;
        }        
        if (stopWordFilter != null) {
            stopWordFilter.setStream(filter);
            filter = stopWordFilter;            
        }
        return filter;
    }

    public void process(String input, DictionaryTrie trie) {
        strategy.countWords(buildTextStream(input), trie);
    }

    public EmitMode getEmitMode() {
        return strategy.getEmitMode();
    }

}
