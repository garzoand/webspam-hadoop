/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount.query;

import hu.sztaki.ilab.bigdata.common.enums.EmitMode;
import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.DuplicatedTokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.ITokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;
import hu.sztaki.ilab.bigdata.spam.wordcount.WordCountProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author garzo
 */
public class QueryWordCountProcessor extends WordCountProcessor {

    protected QueryWordCountStrategy queryStrategy = null;
    protected List<String> queries = new ArrayList<String>();

    public QueryWordCountProcessor(QueryWordCountStrategy strategy) {
        setQueryWordCountStrategy(strategy);
    }

    public final void setQueryWordCountStrategy(QueryWordCountStrategy strategy) {
        this.queryStrategy = strategy;
    }

    protected ITokenStream buildQueryTextStream(String query) {
        ITokenFilter filter = new PorterStemmerFilter(new DuplicatedTokenFilter(
                new LowerCaseFilter(new WordFilter(new StringTokenizerStream(query)))));
        if (stopWordFilter != null) {
            stopWordFilter.setStream(filter);
            return stopWordFilter;
        }
        return filter;
    }

    public void addQuery(String query) {
        ITokenStream stream = buildQueryTextStream(query);
        StringBuilder builder = new StringBuilder();
        Token token = null;
        boolean first = true;
        while ((token = stream.next()) != null) {
            if (first) {
                first = false;
            } else {
                builder.append(" ");
            }
            builder.append(token.getValue());
        }
        queries.add(builder.toString());
        queryStrategy.addQuery(builder.toString());
    }

    public void setQueries(List<String> queries) {
        for (String query : queries) {
            addQuery(query);
        }
    }

    @Override
    public EmitMode getEmitMode() {
        return queryStrategy.getEmitMode();
    }

    @Override
    public void process(String input, DictionaryTrie trie) {
        queryStrategy.countWords(buildTextStream(input), trie);
    }

}
