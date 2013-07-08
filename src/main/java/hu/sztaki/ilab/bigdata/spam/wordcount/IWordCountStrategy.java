package hu.sztaki.ilab.bigdata.spam.wordcount;

import hu.sztaki.ilab.bigdata.common.enums.EmitMode;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

/**
 *
 * @author garzo
 */
public interface IWordCountStrategy {

    public void countWords(ITokenStream stream, DictionaryTrie trie);
    public EmitMode getEmitMode();

}
