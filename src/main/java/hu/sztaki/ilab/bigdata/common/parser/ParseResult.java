/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.parser;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author garzo
 */
public class ParseResult {

    private String originalContent = null;
    private String tokenizedContent = null;
    private String title = null;
    private List<OutLink> outlinks = null;

    public ParseResult() {
        outlinks = new ArrayList<OutLink>();
    }

    public String getOriginalContent() {
        return this.originalContent;
    }

    public void setOriginalContent(String text) {
        this.originalContent = text;
    }

    public String getTokenizedContent() {
        return this.tokenizedContent;
    }

    public void setTokenizedContent(String text) {
        this.tokenizedContent = text;
    }

    public String getTitle() {
        return this.title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void addOutLink(OutLink link) {
        outlinks.add(link);
    }

    public List<OutLink> getOutLinks() {
        return this.outlinks;
    }

}
