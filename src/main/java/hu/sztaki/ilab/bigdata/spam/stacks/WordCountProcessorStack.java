/**
 * WordCountProcessorStack.java
 * Processor stack for mappers dealing with word count.
 * Word count processor stack always emits words occured in the content as an
 * intermediate key. This class is suitable for counting page based document frequencies
 * for instance. If you would like to calculate host based DFs, use ContentProcessorStack.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.stacks;

import hu.sztaki.ilab.bigdata.common.parser.DefaultContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.parser.IContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.parser.strategy.JerichoHTMLParsingStrategy;
import hu.sztaki.ilab.bigdata.common.trie.EmittableDictionaryTrie;
import hu.sztaki.ilab.bigdata.spam.wordcount.WordCountProcessor;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author garzo
 */
public class WordCountProcessorStack {

    private List<WordCountProcessor> processors = null;
    protected IContentParsingStrategy parsingStrategy = null;
    private static final Log LOG = LogFactory.getLog(WordCountProcessorStack.class);

    public WordCountProcessorStack() {
        this(new DefaultContentParsingStrategy(new JerichoHTMLParsingStrategy()));
    }

    public WordCountProcessorStack(IContentParsingStrategy strategy) {
        this.parsingStrategy = strategy;
        this.processors = new ArrayList<WordCountProcessor>();
    }

    public void addProcessor(WordCountProcessor processor) {
        processors.add(processor);
    }

    public void compute(String input, EmittableDictionaryTrie trie) {
        ParseResult result = new ParseResult();
        parsingStrategy.ParseContent(input, result);
        for (WordCountProcessor processor : processors) {
            // clearing result trie
            trie.clear();

            // sets prefix character of intermediate key
            trie.setPrefixChar(processor.getPrefixChar());

            // calculating word count features according to selected strategy
            processor.process(result.getTokenizedContent(), trie);

            // emit data to reducers
            trie.setEmitMode(processor.getEmitMode());
            trie.write();
        }
    }

}
