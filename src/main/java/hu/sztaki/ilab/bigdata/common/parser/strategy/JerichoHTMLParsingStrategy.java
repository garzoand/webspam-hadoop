/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.parser.strategy;

import hu.sztaki.ilab.bigdata.common.parser.OutLink;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;

import java.util.List;

import net.htmlparser.jericho.Config;
import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.LoggerProvider;
import net.htmlparser.jericho.Source;

/**
 *
 * @author garzo
 */
public class JerichoHTMLParsingStrategy implements IHTMLParsingStrategy {

    private Source source = null;

    
    public JerichoHTMLParsingStrategy() {
        //disable verbose logging
        Config.LoggerProvider=LoggerProvider.DISABLED;
    }
    
    public void parseHTML(String html, ParseResult result) {
        source = new Source(html);
        
        StringBuilder builder = new StringBuilder();

        result.setOriginalContent(html);
        result.setTokenizedContent(source.getTextExtractor().toString());
        List<Element> titleElements = source.getAllElements(HTMLElementName.TITLE);
        List<Element> anchorElements = source.getAllElements(HTMLElementName.A);
        for (Element e : titleElements) {
            builder.append(e.getTextExtractor().toString());
        }
        result.setTitle(builder.toString());
        for (Element e : anchorElements) {
            OutLink outlink = new OutLink();
            String href=e.getAttributeValue("href");
            if (href==null) {
                continue;  
            } 
            // A element can contain other tags so need to extract the text from it:
            outlink.setAnchorText(e.getContent().getTextExtractor().toString());
            outlink.setTargetUrl(href);
            result.addOutLink(outlink);
        }
    }

}
