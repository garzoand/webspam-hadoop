/**
 * NGramDFCounter.java
 * Calculated DF based on NGram created form words of query
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount.query;

import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

/**
 *
 * @author garzo
 */
public class NGramDFCounter extends MultiWordCounter {

    @Override
    protected void initialize(DictionaryTrie queryTrie) {
        queryTrie.clear();
        for (String query : queries) {
            String[] words = query.split(" ");
            for (int i = 0; i < words.length; i++) {
                StringBuilder builder = new StringBuilder();
                for (int j = i; j < words.length; j++) {                    
                    if (j > i)
                        builder.append(" ");
                    builder.append(words[j]);
                    queryTrie.insert(builder.toString(), 0);
                }
            }
        }
    }

}
