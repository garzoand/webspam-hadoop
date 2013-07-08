/**
 * LowerCaseFilter.java
 * Token filter for lowercasing input
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
public class LowerCaseFilter extends IdentityTokenFilter {

    public LowerCaseFilter(ITokenStream stream) {
        this.inputStream = stream;
    }

    @Override
    public Token next() {
        if (inputStream != null) {
            Token token = inputStream.next();
            if (token != null) {
                token.setValue(token.getValue().toLowerCase());
            }
            return token;
        } else {
            return null;
        }
    }
    
}
