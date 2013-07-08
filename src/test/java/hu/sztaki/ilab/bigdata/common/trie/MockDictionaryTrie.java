/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.trie;

import hu.sztaki.ilab.bigdata.common.enums.EmitMode;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;
import hu.sztaki.ilab.bigdata.common.trie.EmittableDictionaryTrie;

import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author garzo
 */
public class MockDictionaryTrie extends EmittableDictionaryTrie {

    private Map<String, Integer> content = null;

    public MockDictionaryTrie() {
        content = new TreeMap<String, Integer>();
    }

    public long getContent(String input) {
        if (content.keySet().contains(input)) {
            return content.get(input);
        } else {
            return 0;
        }
    }

    public Map getContent() {
        return this.content;
    }

    @Override
    protected void emit(String input, DictionaryTrie trie) {
        if (emitMode == EmitMode.EMIT_LABEL) {
            content.put(prefixChar + input, (Integer)trie.label);
        } else if (emitMode == EmitMode.EMIT_ONE) {
            content.put(prefixChar + input, 1);
        }
    }

}
