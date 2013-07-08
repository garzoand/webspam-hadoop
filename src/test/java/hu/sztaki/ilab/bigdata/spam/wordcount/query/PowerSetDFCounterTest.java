package hu.sztaki.ilab.bigdata.spam.wordcount.query;

import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class PowerSetDFCounterTest extends TestCase {
    
    private final String[] queries = {
        "Y N P",
        "H L K"
    };

    private final String input = "Y N P P X X X N P X Y L H H L";
    private final PowerSetDFCounter dfCounter = new PowerSetDFCounter();
    private final DictionaryTrie trie = new DictionaryTrie();
    private ITokenStream stream;

    public PowerSetDFCounterTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        stream = new WordFilter(new StringTokenizerStream(input));
        for (String query : queries) {
            dfCounter.addQuery(query);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testPowersetDFCounter() {
        dfCounter.countWords(stream, trie);

        assertEquals(trie.contains("Y"), true);
        assertEquals(trie.contains("N"), true);
        assertEquals(trie.contains("P"), true);
        assertEquals(trie.contains("Y N"), true);
        assertEquals(trie.contains("Y P"), true);
        assertEquals(trie.contains("N P"), true);
        assertEquals(trie.contains("Y N P"), true);

        /*assertEquals(trie.contains("2 4"), true);
        assertEquals(trie.contains("2 3"), true);
        assertEquals(trie.contains("1 2 4"), true);*/

        assertEquals(trie.getLabel("Y"), 2);
        assertEquals(trie.getLabel("Y N P"), 1);
        assertEquals(trie.getLabel("N P"), 2);
        assertEquals(trie.getLabel("P"), 3);

        assertEquals(trie.getLabel("H"), 2);
        // assertEquals(trie.getLabel("H H"), 0);

        trie.write();
    }

}
