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
public class NGramDFCounterTest extends TestCase {
    
    private final String[] queries = {
        "A B C",
        "D E F G"
    };

    private final String input = "A D X X A B Y Y Z D E F G X X F G";
    private final NGramDFCounter dfCounter = new NGramDFCounter();
    private final DictionaryTrie trie = new DictionaryTrie();
    private ITokenStream stream;

    public NGramDFCounterTest(String testName) {
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

    public void testNGramDFCounter() {
        dfCounter.countWords(stream, trie);

        assertEquals(trie.getLabel("A"), 2);
        assertEquals(trie.getLabel("A B"), 1);
        assertEquals(trie.getLabel("A B C"), 0);                

        assertEquals(trie.getLabel("D"), 2);
        assertEquals(trie.getLabel("D E"), 1);
        assertEquals(trie.getLabel("D E F"), 1);
        assertEquals(trie.getLabel("D E F G"), 1);
        assertEquals(trie.getLabel("E F"), 1);
        assertEquals(trie.getLabel("F G"), 2);
    }

}
