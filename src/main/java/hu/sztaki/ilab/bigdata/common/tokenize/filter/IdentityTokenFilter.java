/**
 * IdentityTokenFilter.java
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;

/**
 *
 * @author garzo
 */
public class IdentityTokenFilter implements ITokenFilter {

    protected ITokenStream inputStream = null;

    public IdentityTokenFilter() {
        
    }

    public void setInput(ITokenStream input) {
        this.inputStream = input;
    }

    public void reset() {
        if (inputStream != null) {
            inputStream.reset();
        }
    }

    public Token next() {
        if (inputStream != null) {
            return inputStream.next();
        } else {
            return null;
        }
    }

    public void close() {
        if (inputStream != null) {
            inputStream.close();
        }
    }

}
