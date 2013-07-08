/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.sztaki.ilab.bigdata.common.dictionary;

import hu.sztaki.ilab.bigdata.common.tokenize.filter.IdentityTokenFilter;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;

/**
 *
 * @author garzo
 */
public class MultiLangDictionaryTest extends TestCase {
    
    private MultiLangDictionary dictionary = null;
    public static final String input = 
            "x1\ty1\t1000\n" +
            "x1\ty2\t1000\n" +
            "x2\ty2\t1002\n" +
            "x3\ty1\t1003\n" +
            "x4\ty4\t1003\n" +
            "x5\t\t1009\n";
    
    public MultiLangDictionaryTest() {
        
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        dictionary = new MultiLangDictionary(new IdentityTokenFilter(), new IdentityTokenFilter());
        InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(input.getBytes()));
        dictionary.readFromFile(reader);
    }
    
    /**
     * Test of isOriginalWord method, of class MultiLangDictionary.
     */
    @Test
    public void testIsOriginalWord() {
        assertEquals(true, dictionary.isOriginalWord(("x1")));
        assertEquals(false, dictionary.isOriginalWord(("y1")));
    }

    /**
     * Test of isTranslatedWord method, of class MultiLangDictionary.
     */
    @Test
    public void testIsTranslatedWord() {
        assertEquals(false, dictionary.isTranslatedWord("x1"));
        assertEquals(true, dictionary.isTranslatedWord("y1"));
    }

    /**
     * Test of translate method, of class MultiLangDictionary.
     */
    @Test
    public void testTranslate() {
        List<String> result = dictionary.translate("x1");
        assertEquals(true, result.contains("y1"));
        assertEquals(true, result.contains("y2"));
        assertEquals(false, result.contains("y3"));
    }

    /**
     * Test of reverseTranslate method, of class MultiLangDictionary.
     */
    @Test
    public void testReverseTranslate() {
        List<String> result = dictionary.reverseTranslate("y1");
        assertEquals(true, result.contains("x1"));
        assertEquals(true, result.contains("x3"));
    }

    /**
     * Test of getID method, of class MultiLangDictionary.
     */
    @Test
    public void testGetID() {
        assertEquals(0, dictionary.getWordId("x1"));
        assertEquals(1, dictionary.getWordId("x2"));
        assertEquals(2, dictionary.getWordId("x3"));
    }

    /**
     * Test of getWord method, of class MultiLangDictionary.
     */
    @Test
    public void testGetWord() {
        assertEquals("x1", dictionary.getWord(0));
        assertEquals("x2", dictionary.getWord(1));
        assertEquals("x3", dictionary.getWord(2));
        assertEquals("x5", dictionary.getWord(4));
    }

    /**
     * Test of getWordsNum method, of class MultiLangDictionary.
     */
    @Test
    public void testGetWordsNum() {
        assertEquals(5, dictionary.getWordsNum());
    }

    @Test
    public void testGetDF() {
        assertEquals(1000, dictionary.getDocumentFrequency(0));
        assertEquals(1002, dictionary.getDocumentFrequency(1));
        assertEquals(1003, dictionary.getDocumentFrequency(2));
        assertEquals(1009, dictionary.getDocumentFrequency(4));
    }         
    
    @Test
    public void emptyStringTest() {
        List<String> result = dictionary.reverseTranslate("");
        assertEquals(0, result.size());
    }

}
