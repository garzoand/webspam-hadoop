/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.bigdata.common.trie;

import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class DictionaryTrieTest extends TestCase {
    
    public DictionaryTrieTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testDictionaryTrie() {
        DictionaryTrie trie = new DictionaryTrie();
        trie.insert("apple", 0);
        trie.insert("appear", 7);
        trie.insert("pear", 1);
        trie.insert("orange", 2);
        trie.insert("banana", 3);

        assertEquals(trie.contains("apple"), true);
        assertEquals(trie.getLabel("appear"), 7);
        assertEquals(trie.contains("grapes"), false);

        trie.setLabel("banana", trie.getLabel("banana") + 1);
        assertEquals(trie.getLabel("banana"), 4);

        System.out.println("============================");
        trie.write();

        trie.clear();
        assertEquals(trie.contains("apple"), false);
        assertEquals(trie.contains("pear"), false);
        assertEquals(trie.contains("orange"), false);
        assertEquals(trie.contains("banana"), false);

        trie.insert("appear", 2);
        assertEquals(trie.getLabel("appear"), 2);

        trie.increaseLabel("appear", 1);
        assertEquals(trie.getLabel("appear"), 3);

        System.out.println("============================");
        trie.write();
    }

}
