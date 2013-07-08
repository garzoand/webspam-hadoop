/**
 * IdentityContentParsingStrategy.java
 * Leaves input text unchanged
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.parser;

import hu.sztaki.ilab.bigdata.common.parser.strategy.IHTMLParsingStrategy;

/**
 *
 * @author garzo
 */
public class IdentityContentParsingStrategy implements IContentParsingStrategy {

    public void setHtmlParsingStrategy(IHTMLParsingStrategy strategy) {
        
    }

    public void ParseContent(String content, ParseResult result) {
        result.setOriginalContent(content);
        result.setTitle("");
        result.setTokenizedContent(content);
    }

}
