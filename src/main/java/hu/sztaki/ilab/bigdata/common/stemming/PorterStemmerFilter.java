/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.stemming;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.IdentityTokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;

/**
 *
 * @author garzo
 */
public class PorterStemmerFilter extends IdentityTokenFilter {

    private PorterStemmer stemmer = new PorterStemmer();

    public PorterStemmerFilter() {
    
    }
    
    public PorterStemmerFilter(ITokenStream stream) {
        this.inputStream = stream;
    }
    
    @Override
    public Token next() {
        if (inputStream != null) {
            Token token = inputStream.next();
            if (token == null)
                return null;
            stemmer.add(token.getValue().toCharArray(), token.getValue().length());
            stemmer.stem();
            token.setValue(stemmer.toString());
            return token;
        } else {
            return null;
        }
    }

}
