/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package hu.sztaki.ilab.bigdata.spam.features.tfreq;

import hu.sztaki.ilab.bigdata.common.dictionary.MultiLangDictionary;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.IdentityTokenFilter;
import hu.sztaki.ilab.bigdata.spam.MockParserResult;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.Map;

import junit.framework.TestCase;

import org.junit.Test;

/**
 *
 * @author garzo
 */
public class TranslatedTermFreqCalculatorTest extends TestCase {
    
    private final int testPageNum = 1;
    private MockParserResult[] parserResults = new MockParserResult[testPageNum];
    private Map[] features = new Map[testPageNum];
    private final TranslatedTermFreqCalculator calculator = new TranslatedTermFreqCalculator();
    
    public static final String dictionaryInput = 
            "x1\ty1\t1000\n" +
            "x1\ty2\t1000\n" +
            "x2\ty2\t1002\n" +
            "x3\ty1\t1003\n" +
            "x4\ty4\t1003\n" +
            "x5\t\t1009\n";    
    
    public TranslatedTermFreqCalculatorTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        MultiLangDictionary dictionary = new MultiLangDictionary(new IdentityTokenFilter(), new IdentityTokenFilter());       
        InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(dictionaryInput.getBytes()));
        dictionary.readFromFile(reader);
        calculator.setDictionary(dictionary);
        calculator.setStemmer(new IdentityTokenFilter());
        
        MockParserResult page1 = new MockParserResult.MockParserBuilder("http://host1.com")
                .title("")
                .content("y4 y1 y1 y2 x4")
                .build();
        parserResults[0] = page1;        
       
        for (int i = 0; i < parserResults.length; i++) {
            calculator.processContent(parserResults[i].getParseResult(),
                    parserResults[i].getMetaData());
            features[i] = calculator.getFeatures();
        }        
    }
    
    @Test
    public void testTranslatedTermFreqCalculator() {
        assertEquals(1.5, features[0].get(0));
        assertEquals(0.5, features[0].get(1));
        assertEquals(1.0, features[0].get(2));
        assertEquals(2.0, features[0].get(3));        
    }
}
