package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.ITokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.StopWordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class StopWordFilterTest extends TestCase {
    
    private StopWordFilter stopWordFilter = new StopWordFilter();

    public StopWordFilterTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        stopWordFilter.loadFromString("the a an more than because");
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testStopWordFilter() {
        String inputText = "The D more D D D because than D";
        ITokenFilter stream = new PorterStemmerFilter(new LowerCaseFilter(
                new WordFilter(new StringTokenizerStream(inputText))));
        stopWordFilter.setStream(stream);

        Token token = null;
        int counter = 0;
        while ((token = stopWordFilter.next()) != null) {
            assertEquals(token.getValue(), "d");
            counter++;
        }
        assertEquals(counter, 5);        
    }

}
