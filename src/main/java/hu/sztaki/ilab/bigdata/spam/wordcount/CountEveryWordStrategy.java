/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.wordcount;

import hu.sztaki.ilab.bigdata.common.enums.EmitMode;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author garzo
 */
public class CountEveryWordStrategy implements IWordCountStrategy {
    
    private static final Log LOG = LogFactory.getLog(CountEveryWordStrategy.class);
   
    @Override
    public void countWords(ITokenStream stream, DictionaryTrie trie) {
        Token token = null;
        while ((token = stream.next()) != null) {
            if (token.getValue().length() > 30)
                continue;
            if (!trie.increaseLabel(token.getValue(), 1)) {
                // token hasn't been found in trie
                trie.insert(token.getValue(), 1);
            }
        }
    }

    @Override
    public EmitMode getEmitMode() {
        return EmitMode.EMIT_LABEL;
    }

}
