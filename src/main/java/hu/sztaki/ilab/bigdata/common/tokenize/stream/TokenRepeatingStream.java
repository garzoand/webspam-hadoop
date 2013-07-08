/**
 * TokenRepeatingStream.java
 * Token stream which outputs the same token a specified number of times.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tokenize.stream;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;

/**
 *
 * @author garzo
 */
public class TokenRepeatingStream implements ITokenStream {

    private int numberOfTimes = 0;
    private Token token = null;

    public TokenRepeatingStream() {
        
    }

    public TokenRepeatingStream(Token token, int numberOfTimes) {
        this.numberOfTimes = numberOfTimes;
        this.token = token;
    }

    public void setToken(Token token) {
        this.token = token;
    }

    public void setNumberOfTimes(int num) {
        this.numberOfTimes = num;
    }

    public void reset() {
        this.numberOfTimes = 0;
    }

    public Token next() {
        if (numberOfTimes > 0) {
            numberOfTimes--;
            return token;
        } else {
            return null;
        }
    }

    public void close() {
        
    }

}
