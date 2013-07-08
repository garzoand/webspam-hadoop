/**
 * QueryWordCountStrategy.java
 * Base class of query word DF counters.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount.query;

import hu.sztaki.ilab.bigdata.common.enums.EmitMode;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;
import hu.sztaki.ilab.bigdata.spam.wordcount.IWordCountStrategy;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author garzo
 */
public abstract class QueryWordCountStrategy implements IWordCountStrategy {

    protected List<String> queries = new ArrayList<String>();

    public void setQueryList(List<String> queries) {
        this.queries = queries;
    }

    public void addQuery(String query) {
        queries.add(query);
    }

    public EmitMode getEmitMode() {
        return EmitMode.EMIT_ONE;
    }

    // do the initial work with query words, e.g. calculating N-grams
    protected abstract void initialize(DictionaryTrie queryTrie);
    
}
