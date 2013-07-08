package hu.sztaki.ilab.bigdata.common.stemming;

import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class PorterStemmerFilterTest extends TestCase {
    
    public PorterStemmerFilterTest(String testName) {
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

    public void testPorterStemmer() {
        String input = "John does walked apples has two";
        String[] result = { "john", "doe", "walk", "appl", "ha", "two" };
        PorterStemmerFilter filter = new PorterStemmerFilter(new LowerCaseFilter(
                new WordFilter(new StringTokenizerStream(input))));
        Token token = null;
        int counter = 0;
        while ((token = filter.next()) != null) {
            assertEquals(token.getValue(), result[counter]);
            counter++;
        }
    }

}
