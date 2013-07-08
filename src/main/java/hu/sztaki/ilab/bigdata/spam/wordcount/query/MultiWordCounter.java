/**
 * MultiWordCounter.java
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount.query;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author garzo
 */
public abstract class MultiWordCounter extends QueryWordCountStrategy {

    public void countWords(ITokenStream stream, DictionaryTrie trie) {
        initialize(trie);
        Token token = null;
        List<StringBuilder> builderList = new LinkedList<StringBuilder>();
        while ((token = stream.next()) != null) {
            if (!trie.contains(token.getValue())) {
                builderList.clear();
            } else {
                trie.increaseLabel(token.getValue(), 1);
                StringBuilder builder = new StringBuilder().append(token.getValue());
                for (StringBuilder b : builderList.toArray(new StringBuilder[builderList.size()])) {
                    b.append(" ").append(token.getValue());
                    if (!trie.increaseLabel(b.toString(), 1)) {
                        builderList.remove(b);
                    }
                }
                builderList.add(builder);
            }
        }
    }

}
