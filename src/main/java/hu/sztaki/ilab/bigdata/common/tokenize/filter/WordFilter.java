/**
 * WordFilter.java
 * Filter all tokens but WORD type tokens.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tokenize.filter;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.TokenType;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;

/**
 *
 * @author garzo
 */
public class WordFilter extends IdentityTokenFilter {

    public WordFilter(ITokenStream stream) {
        this.inputStream = stream;
    }

    @Override
    public Token next() {
        if (inputStream != null) {
            Token token = null;
            do {
                token = inputStream.next();
            } while (token != null && token.getType() != TokenType.WORD);
            return token;
        } else {
            return null;
        }
    }

}
