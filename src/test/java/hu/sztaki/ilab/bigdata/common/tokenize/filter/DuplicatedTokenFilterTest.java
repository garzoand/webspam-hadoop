/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.DuplicatedTokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.TokenRepeatingStream;
import junit.framework.TestCase;

/**
 *
 * @author garzo
 */
public class DuplicatedTokenFilterTest extends TestCase {
    
    public DuplicatedTokenFilterTest(String testName) {
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

    public void testDuplicatedTokenFilter() {
        TokenRepeatingStream repeatingStream = new TokenRepeatingStream(new Token("apple"), 10);
        DuplicatedTokenFilter filter = new DuplicatedTokenFilter(repeatingStream);

        int counter = 0;
        Token token = null;
        while ((token = filter.next()) != null) {
            counter++;
            assertEquals(token.getValue(), "apple");
        }
        assertEquals(counter, 1);

        String input = "apple pear pear apple grapes apple";
        filter = new DuplicatedTokenFilter(new WordFilter(new StringTokenizerStream(input)));
        counter = 0;
        while ((token = filter.next()) != null) {
            counter++;
        }
        assertEquals(counter, 3);
        
    }

}
