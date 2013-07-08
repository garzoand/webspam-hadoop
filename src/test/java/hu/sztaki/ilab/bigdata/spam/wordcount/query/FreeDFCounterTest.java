package hu.sztaki.ilab.bigdata.spam.wordcount.query;

import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class FreeDFCounterTest extends TestCase {
    
    private final String query1 = "apple is fruit or vegetable";
    private final String query2 = "good red vegetable";
    private final FreeDFCounter dfCounter = new FreeDFCounter();
    private final StringTokenizerStream stream = new StringTokenizerStream();
    private final DictionaryTrie trie = new DictionaryTrie();

    public FreeDFCounterTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        List<String> queryList = new ArrayList<String>();
        queryList.add(query1);
        queryList.add(query2);
        dfCounter.setQueryList(queryList);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testCountWords() {
        String input1 = "apple and orange are fruit";
        stream.setInput(input1);
        dfCounter.countWords(stream, trie);
        assertEquals(trie.getLabel("apple"), 1);
        assertEquals(trie.getLabel("orange"), 0);
        assertEquals(trie.getLabel("fruit"), 1);
        assertEquals(trie.getLabel("vegetable"), 0);
        assertEquals(trie.getLabel("apple fruit"), 1);
        assertEquals(trie.getLabel("apple vegatable"), 0);
        trie.write();

        trie.clear();
        System.out.println();
        String input2 = "apple red is";
        dfCounter.initialize(trie);
        stream.setInput(input2);
        dfCounter.countWords(stream, trie);
        assertEquals(trie.getLabel("apple"), 1);
        assertEquals(trie.getLabel("is"), 1);
        assertEquals(trie.getLabel("orange"), 0);
        assertEquals(trie.getLabel("fruit"), 0);
        assertEquals(trie.getLabel("vegetable"), 0);
        assertEquals(trie.getLabel("apple is"), 1);
        trie.write();
    }

}
