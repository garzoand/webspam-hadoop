/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.bigdata.spam.mapper;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class WordCountProcessorStackTest extends TestCase {
    
    private static final String input 
            = "<html><title>C B A</title><body> X X Y <p>X A B</p> X Y <a>C B A</a></body></html>";
    private static String[] queries = { "A B C", "B A", "K U" };

    public WordCountProcessorStackTest(String testName) {
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

    public void testCompute() {
    /*    MockDictionaryTrie trie = new MockDictionaryTrie();
        FreeDFCounter freeDFCounter = new FreeDFCounter();

        QueryWordCountProcessor processor = new QueryWordCountProcessor(freeDFCounter);
        processor.setPrefixChar('F');
        processor.setQueries(Arrays.asList(queries));

        WordCountProcessorStack stack = new WordCountProcessorStack();
        stack.addProcessor(processor);

        stack.compute(input, trie);
        
        assertEquals(trie.getContent("Fa"), 1);
        assertEquals(trie.getContent("Fb"), 1);
        assertEquals(trie.getContent("Fb c a"), 1);
        assertEquals(trie.getContent("Fc b"), 1);
        assertEquals(trie.getContent("Fk"), 0);
        assertEquals(trie.getContent("Fu"), 0);
        assertEquals(trie.getContent("Fk u"), 0);

        stack = new WordCountProcessorStack();
        processor = new QueryWordCountProcessor(new NGramDFCounter());
        processor.setPrefixChar('D');
        processor.setQueries(Arrays.asList(queries));
        stack.addProcessor(processor);
        stack.compute(input, trie);
        
        assertEquals(trie.getContent("Da"), 1);
        assertEquals(trie.getContent("Db"), 1);
        assertEquals(trie.getContent("Da b c"), 0);
        assertEquals(trie.getContent("Da b"), 1);
        assertEquals(trie.getContent("Dc a"), 0);
        assertEquals(trie.getContent("Da c"), 0);
        assertEquals(trie.getContent("Db a"), 1);

        stack = new WordCountProcessorStack();
        processor = new QueryWordCountProcessor(new PowerSetDFCounter());
        processor.setPrefixChar('S');
        processor.setQueries(Arrays.asList(queries));
        stack.addProcessor(processor);
        stack.compute(input, trie);

        assertEquals(trie.getContent("Sa"), 1);
        assertEquals(trie.getContent("Sb"), 1);
        assertEquals(trie.getContent("Sa b c"), 0);
        assertEquals(trie.getContent("Sa b"), 1);
        assertEquals(trie.getContent("Sa c"), 0);*/
    }

   public void testComputeOnRealHtml() throws FileNotFoundException, IOException {
        /*String filePath = "/home/garzo/doc/proba.html";
        byte[] buffer = new byte[(int) new File(filePath).length()];
        BufferedInputStream f = null;
        try {
            f = new BufferedInputStream(new FileInputStream(filePath));
            f.read(buffer);
        } finally {
            if (f != null)
                try { f.close(); } catch (IOException ignored) { }
        }

        String inputStr = new String(buffer);
        List<String> queries = new ArrayList<String>();
        queries.add("Hacker News");
        queries.add("Python Ruby Rails");
        
        FreeDFCounter freeDFCounter = new FreeDFCounter();
        QueryWordCountProcessor processor = new QueryWordCountProcessor(freeDFCounter);
        processor.setPrefixChar('F');
        processor.setQueries(queries);

        WordCountProcessorStack stack = new WordCountProcessorStack();
        stack.addProcessor(processor);

        MockDictionaryTrie trie = new MockDictionaryTrie();
        stack.compute(inputStr, trie);

        Map<String, Integer> map = trie.getContent();
        System.out.println("Words: " + map.keySet().size());
        for (String word : map.keySet()) {
            System.out.println(word + "\t" + map.get(word));
        }*/
        
    }

}
