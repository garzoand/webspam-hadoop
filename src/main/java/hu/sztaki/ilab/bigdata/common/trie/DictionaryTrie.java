/**
 * Simple implementation of Trie data structure for effective dictionary storing.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.trie;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author garzo
 */
public class DictionaryTrie {

    protected Map<Character, DictionaryTrie> paths = new HashMap<Character, DictionaryTrie>();
    protected boolean isWord = false;
    protected int label = 0;

    public DictionaryTrie() {
        
    }

    // returns null is trie don't contains word given
    public int getLabel(String word) {
        DictionaryTrie node = findNode(word);
        if (node != null) {
            return node.label;
        } else {
            return 0;
        }
    }

    public void setLabel(String word, int label) {
        DictionaryTrie node = findNode(word);
        if (node != null) {
            node.label = label;
        }
    }

    public void clear() {
        this.label = 0;
        this.isWord = false;
        for (DictionaryTrie trie : paths.values()) {
            trie.clear();
        }
        paths.clear();
    }

    public void insert(String word, int label) {
        if (word == null) {
            return;
        }
        char[] chars = word.toCharArray();
        DictionaryTrie current = this;
        for (char c : chars) {
            if (!current.paths.containsKey(c)) {
                DictionaryTrie trie = new DictionaryTrie();
                current.paths.put(c, trie);
                current = trie;
            } else {
                current = (DictionaryTrie)current.paths.get(c);
            }
        }
        current.isWord = true;
        current.label = label;
    }

    public boolean contains(String word) {
        return (findNode(word) != null);
    }

    // returns false if item doesn't exist in trie
    public boolean increaseLabel(String word, int diff) {
        DictionaryTrie trie = findNode(word);
        if (trie != null) {
            trie.label += diff;
            return true;
        } else {
            return false;
        }
    }

    protected DictionaryTrie findNode(String word) {
        if (word == null) {
            return null;
        }
        char[] chars = word.toCharArray();
        DictionaryTrie current = this;
        for (char c : chars) {
            if (current.paths.containsKey(c)) {
                current = (DictionaryTrie)current.paths.get(c);
            } else {
                return null;
            }
        }
        if (current.isWord) {
            return current;
        } else {
            return null;
        }
    }

    protected void emit(String node, DictionaryTrie trie) {
        StringBuilder builder = new StringBuilder().append(node).append("\t")
                .append(trie.label);
        System.out.println(builder.toString());
    }

    protected void processNode(String prefix, DictionaryTrie trie) {
        if (trie.isWord && trie.label > 0) {
            emit(prefix, trie);
        }        
        /*Object[] array = trie.paths.keySet().toArray();
        Arrays.sort(array);
        for (int i = 0; i < array.length; i++) {
            processNode(prefix + array[i], trie.paths.get((Character)array[i]));
        }*/
        for (Character c : trie.paths.keySet()) {
            processNode(prefix + c, trie.paths.get(c));
        }
    }

    public void write() {
        processNode("", this);
    }
        
}
