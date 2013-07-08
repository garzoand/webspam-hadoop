/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.parser;

/**
 *
 * @author garzo
 */
public class OutLink {

    private String targetUrl;
    private String anchorText;

    public OutLink() {
        
    }

    public OutLink(String targetUrl, String anchorText) {
        this.targetUrl = targetUrl;
        this.anchorText = anchorText;
    }

    public String getTargetUrl() {
        return this.targetUrl;
    }

    public void setTargetUrl(String url) {
        this.targetUrl = url;
    }

    public String getAnchorText() {
        return this.anchorText;
    }

    public void setAnchorText(String anchor) {
        this.anchorText = anchor;
    }

}
