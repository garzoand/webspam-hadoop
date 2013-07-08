/*
 * Content processing staregy is implemented differently for feature
 * generation methods (i.g. BasicContentFeatures)
*/
package hu.sztaki.ilab.bigdata.spam.features;

import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoStore;

import java.util.Map;

/**
 *
 * @author garzo
 */
public interface IContentProcessingStrategy<T> {

    public boolean needHTMLParsing();
    public boolean needTokenizedAndStemmed();
    public boolean processContent(ParseResult result, HomePageMetaData meta);
    public Map<String, Double> getFeatures();
    public void setHostInfoStore(HostInfoStore store);
}
