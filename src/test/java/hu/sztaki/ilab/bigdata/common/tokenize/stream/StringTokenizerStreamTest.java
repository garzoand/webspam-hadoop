package hu.sztaki.ilab.bigdata.common.tokenize.stream;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.TokenType;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class StringTokenizerStreamTest extends TestCase {
    
    private final StringTokenizerStream stream = new StringTokenizerStream();
    private final String input = "token1 token'2   Token3\ttoken--4 5token 6token6";
    private final String[] result 
            = { "token1", "token", "Token3", "token", "5token", "6token6" };

    public StringTokenizerStreamTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        stream.setInput(input);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testStringTokenizerStream() {
        int counter = 0;
        Token token;
        while ((token = stream.next()) != null) {
            if (token.getType() == TokenType.WORD) {
                System.out.println(token.getValue());
                assertTrue(counter < result.length);
                assertEquals(token.getValue(), result[counter]);
                counter++;
            }
        }
    }
    
}
