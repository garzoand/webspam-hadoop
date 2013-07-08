/**
 * MinLengthFilter.java
 * Removes too short words from string stream. Default minimum length is 0
 * (does not filter anything).
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;

/**
 *
 * @author garzo
 */
public class MinLengthFilter extends IdentityTokenFilter {
    
    private int minLength = 0;
    
    public MinLengthFilter(ITokenStream stream) {
        this.inputStream = stream;
    }
    
    public void setMinLength(int length) {
        this.minLength = length;
    }
    
    @Override
    public Token next() {
        Token token = null;
        if (inputStream != null) {
            boolean done = false;
            while (!done) {
                token = inputStream.next();
                if (token == null) {
                    done = true;
                } else {
                    if (token.getValue().length() > this.minLength) {                        
                        done = true;
                    }
                }
            }
            return token;
        } else {
            return null;
        }
    }        
    
}
