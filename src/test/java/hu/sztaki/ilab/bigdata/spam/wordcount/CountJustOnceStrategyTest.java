/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.bigdata.spam.wordcount;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.TokenRepeatingStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class CountJustOnceStrategyTest extends TestCase {
    
    public CountJustOnceStrategyTest(String testName) {
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

    public void testCountJustOnce() {
        ITokenStream stream = new TokenRepeatingStream(new Token("apple"), 10);
        CountJustOnceStrategy wordCounter = new CountJustOnceStrategy();
        DictionaryTrie trie = new DictionaryTrie();

        wordCounter.countWords(stream, trie);
        assertEquals(trie.getLabel("apple"), 1);
    }

}
