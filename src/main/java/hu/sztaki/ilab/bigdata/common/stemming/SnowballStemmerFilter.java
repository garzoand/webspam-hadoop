/**
 * SnowballStemmerFilter.java
 * String stream filter for snowball stemming
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.stemming;

import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.IdentityTokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.tartarus.snowball.SnowballStemmer;

/**
 *
 * @author garzo
 */
public class SnowballStemmerFilter extends IdentityTokenFilter{
    
    private SnowballStemmer stemmer = null;
    private static final Log LOG = LogFactory.getLog(SnowballStemmerFilter.class);
    
    public SnowballStemmerFilter(ITokenStream stream, SnowballStemmer stemmer) {
        this.inputStream = stream;
        this.stemmer = stemmer;
    }    
    
    public SnowballStemmerFilter(ITokenStream stream, String language) 
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
	Class stemClass = Class.forName("org.tartarus.snowball.ext." +
					language + "Stemmer");
        this.stemmer = (SnowballStemmer) stemClass.newInstance();
        LOG.info("Instantiated stemmer class: " + this.stemmer.getClass().getName());
    }
    
    public SnowballStemmerFilter(String language) 
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
	Class stemClass = Class.forName("org.tartarus.snowball.ext." +
					language + "Stemmer");        
        this.stemmer = (SnowballStemmer) stemClass.newInstance();
        LOG.info("Instantiated stemmer class: " + this.stemmer.getClass().getName());
    }       
    
    @Override
    public Token next() {
        if (inputStream != null) {
            Token token = inputStream.next();
            if (token == null)
                return null;
            stemmer.setCurrent(token.getValue());            
            token.setValue(stemmer.getCurrent());
            return token;
        } else {
            return null;
        }
    }
        
}
