/**
 * TokenizedContentParsingStrategy.java
 * Expects text file with tokenized content. Suitable for processing files generated
 * by the (modified) parse_repos.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.parser;

import hu.sztaki.ilab.bigdata.common.parser.IContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.parser.strategy.IHTMLParsingStrategy;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author garzo
 */
public class TokenizedContentParsingStrategy implements IContentParsingStrategy {

    private static final Log LOG = LogFactory.getLog(TokenizedContentParsingStrategy.class);
    
    @Override
    public void setHtmlParsingStrategy(IHTMLParsingStrategy strategy) {
        
    }

    @Override
    public void ParseContent(String content, ParseResult result) {        
        String[] data = content.split("\\|");
        if (data.length < 3)
            return;
        result.setOriginalContent(content);
        result.setTitle(data[1]);
        result.setTokenizedContent(
                new StringBuilder(data[1]).append(" ").append(data[2]).toString());
    }
    
}
