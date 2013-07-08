package hu.sztaki.ilab.bigdata.spam.wordcount;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.TokenRepeatingStream;
import hu.sztaki.ilab.bigdata.common.trie.DictionaryTrie;

import java.util.Map;
import java.util.TreeMap;

import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class CountEveryWordStrategyTest extends TestCase {
    
    private Map<String, Integer> result = new TreeMap<String, Integer>();
    private ITokenStream stream = null;

    public CountEveryWordStrategyTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        String input = "apple apple orange banana orange apple";
        stream = new StringTokenizerStream(input);
        result.put("apple", 3);
        result.put("orange", 2);
        result.put("banana", 1);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testCountEveryWord() {
        CountEveryWordStrategy wordCounter = new CountEveryWordStrategy();
        DictionaryTrie trie = new DictionaryTrie();

        wordCounter.countWords(stream, trie);
        for (String s : result.keySet()) {
            assertEquals(trie.getLabel(s), (int)result.get(s));
        }

        trie.clear();
        ITokenStream stream2 = new TokenRepeatingStream(new Token("apple"), 3);
        wordCounter.countWords(stream2, trie);
        assertEquals(trie.getLabel("apple"), 3);
    }

}
