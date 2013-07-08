/**
 * DefaultContentParsingStrategy.java
 * Default content parsing strategy simply calls HTML parser strategy given by parameter.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.parser;

import hu.sztaki.ilab.bigdata.common.parser.strategy.IHTMLParsingStrategy;
import hu.sztaki.ilab.bigdata.common.parser.strategy.JerichoHTMLParsingStrategy;

/**
 *
 * @author garzo
 */
public class DefaultContentParsingStrategy implements IContentParsingStrategy {

    protected IHTMLParsingStrategy strategy;

    public DefaultContentParsingStrategy() {
        strategy = new JerichoHTMLParsingStrategy();
    }

    public DefaultContentParsingStrategy(IHTMLParsingStrategy strategy) {
        this.strategy = strategy;
    }

    public void setHtmlParsingStrategy(IHTMLParsingStrategy strategy) {
        this.strategy = strategy;
    }

    public void ParseContent(String content, ParseResult result) {
        strategy.parseHTML(content, result);
    }

}
