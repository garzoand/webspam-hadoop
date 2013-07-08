/**
 * DuplicatedTokenFilter.java
 * Removes duplicated tokens from a token stream.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

/**
 *
 * @author garzo
 */
public class DuplicatedTokenFilter extends IdentityTokenFilter {

    private DictionaryTrie trie = new DictionaryTrie();

    public DuplicatedTokenFilter(ITokenStream stream) {
        this.inputStream = stream;
    }

    @Override
    public Token next() {
        Token token = null;
        if (inputStream != null) {
            boolean done = false;
            while (!done) {
                token = inputStream.next();
                if (token == null) {
                    done = true;
                } else {
                    if (!trie.contains(token.getValue())) {
                        trie.insert(token.getValue(), 0);
                        done = true;
                    }
                }
            }
            return token;
        } else {
            return null;
        }
    }

}
