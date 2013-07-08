package hu.sztaki.ilab.bigdata.common.parser;

import hu.sztaki.ilab.bigdata.common.parser.strategy.IHTMLParsingStrategy;

/**
 *
 * @author garzo
 */
public interface IContentParsingStrategy {

    public void setHtmlParsingStrategy(IHTMLParsingStrategy strategy);
    public void ParseContent(String content, ParseResult result);

}
