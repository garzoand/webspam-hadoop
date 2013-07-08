/**
 * CountJustOnceStrategy.java
 * Counts each token once.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount;

import hu.sztaki.ilab.bigdata.common.enums.EmitMode;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

/**
 *
 * @author garzo
 */
public class CountJustOnceStrategy implements IWordCountStrategy {

    public void countWords(ITokenStream stream, DictionaryTrie trie) {
        Token token = null;
        while ((token = stream.next()) != null) {
            if (!trie.contains(token.getValue())) {
                trie.insert(token.getValue(), 1);
            }
        }
    }

    public EmitMode getEmitMode() {
        return EmitMode.EMIT_LABEL;
    }

}
