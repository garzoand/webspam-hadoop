/**
 * PowerSetDFCounter.java
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount.query;

import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author garzo
 */
public class PowerSetDFCounter extends MultiWordCounter {

    @Override
    protected void initialize(DictionaryTrie queryTrie) {
        queryTrie.clear();
        for (String query : queries) {
            String[] words = query.split(" ");
            Set<String> set = new TreeSet<String>();
            for (int i = 0; i < words.length; i++) {
                set.add(words[i]);
                queryTrie.insert(words[i], 0);
                for (String s : set.toArray(new String[set.size()])) {
                    StringBuilder builder = new StringBuilder();
                    builder.append(s).append(" ").append(words[i]);
                    queryTrie.insert(builder.toString(), 0);
                    set.add(builder.toString());
                }                
            }
        }
    }

}
