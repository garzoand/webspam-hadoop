package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class LowerCaseFilterTest extends TestCase {
    
    public LowerCaseFilterTest(String testName) {
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

    public void testLowerCaseFilter() {
        String input = "  ALma   KoRTE";
        LowerCaseFilter filter = new LowerCaseFilter(
                new WordFilter(new StringTokenizerStream(input)));
        Token token = null;

        token = filter.next();
        assertEquals(token.getValue(), "alma");

        token = filter.next();
        assertEquals(token.getValue(), "korte");
    }

}
