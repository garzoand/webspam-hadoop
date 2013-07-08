/**
 * TokenRepeatingStreamTest.java
 * JUnit test class for TokenRepeatingStream class.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */

package hu.sztaki.ilab.bigdata.common.tokenize.stream;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.TokenType;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.TokenRepeatingStream;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class TokenRepeatingStreamTest extends TestCase {
    
    private Token token = null;

    public TokenRepeatingStreamTest(String testName) {
        super(testName);
    }

    @Override
    protected void setUp() throws Exception {
        token = new Token("token", TokenType.WORD);
    }

    @Override
    protected void tearDown() throws Exception {

    }

    /**
     * Test of next method, of class TokenRepeatingStream.
     */
    public void testNext() {
        TokenRepeatingStream stream1 = new TokenRepeatingStream(token, 4);
        TokenRepeatingStream stream2 = new TokenRepeatingStream(token, 0);
        
        int counter = 0;
        Token t;
        while ((t = stream1.next()) != null) {
            counter++;
            assertEquals(token.getValue(), "token");
        }
        assertEquals(counter, 4);

        counter = 0;
        while (stream2.next() != null)
            fail();
    }


}
