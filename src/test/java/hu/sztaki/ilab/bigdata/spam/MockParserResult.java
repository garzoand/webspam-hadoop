/**
 * MockParser.java
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam;

import hu.sztaki.ilab.bigdata.common.parser.OutLink;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;

/**
 *
 * @author garzo
 */
public class MockParserResult {

    ParseResult result;
    HomePageMetaData meta;

    public static class MockParserBuilder {
        ParseResult result = new ParseResult();
        private final String url;
        private final String hostName;
        private String path;

        public MockParserBuilder(String url) {
            this.url = url;
            this.hostName = url;
        }

        public MockParserBuilder path(String p) {
            this.path = p;
            return this;
        }

        public MockParserBuilder content(String content) {
            result.setTokenizedContent(content);
            result.setOriginalContent(content);
            return this;
        }

        public MockParserBuilder originalContent(String content) {
            result.setOriginalContent(content);
            return this;
        }

        public MockParserBuilder title(String title) {
            result.setTitle(title);
            return this;
        }

        public MockParserBuilder withOutlink(String url, String anchor) {
            OutLink link = new OutLink(url, anchor);
            result.addOutLink(link);
            return this;
        }

        public MockParserResult build() {
            return new MockParserResult(this);
        }
        
    }

    private MockParserResult(MockParserBuilder builder) {
        meta = new HomePageMetaData.Builder(builder.url, builder.hostName)
                .path(builder.path).build();
        result = builder.result;
    }

    public HomePageMetaData getMetaData() {
        return this.meta;
    }

    public ParseResult getParseResult() {
        return this.result;
    }
    
}
