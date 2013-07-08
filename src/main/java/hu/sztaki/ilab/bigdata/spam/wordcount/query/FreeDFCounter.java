/**
 * FreeDFCounter.java
 * Class for counting free document frequencies.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount.query;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author garzo
 */
public class FreeDFCounter extends QueryWordCountStrategy {

    @Override
    protected void initialize(DictionaryTrie queryTrie) {
        queryTrie.clear();
        for (String query : queries) {
            for (String s : query.split(" ")) {
                queryTrie.insert(s, 0);
            }
        }
    }

    public void countWords(ITokenStream stream, DictionaryTrie trie) {
        initialize(trie);
        Token token = null;
        List<String> foundTokens = new ArrayList<String>();
        while ((token = stream.next()) != null) {
            if (trie.contains(token.getValue())) {
                trie.setLabel(token.getValue(), 1);
            }
        }

        for (String query : queries) {
            List<String> words = new ArrayList();
            String[] queryWords = query.split(" ");
            Arrays.sort(queryWords);
            for (String word : queryWords) {
                if (trie.getLabel(word) > 0) {
                    words.add(word);
                }
            }
            if (words.size() < 2)
                continue;
            StringBuilder builder = new StringBuilder().append(words.get(0));
            for (int i = 1; i < words.size(); i++) {
                builder.append(" ").append(words.get(i));
                trie.insert(builder.toString(), 1);
            }            
        }
       
    }

}
