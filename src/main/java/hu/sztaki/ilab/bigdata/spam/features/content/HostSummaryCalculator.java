/**
 * HostSummaryCalculator.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.features.content;

import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.ITokenStream;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.spam.enums.features.HostSummaryFeatures;

/**
 *
 * @author garzo
 */
public class HostSummaryCalculator extends AbstractContentFeatureCalculator {

    public boolean processContent(ParseResult result, HomePageMetaData meta) {
        super.processContent(result, meta);

        ITokenStream stream =
                new PorterStemmerFilter(
                new LowerCaseFilter(
                new WordFilter(
                new StringTokenizerStream(result.getTokenizedContent()))));
        Token token = null;

        int wordcount = 0;
        while ((token = stream.next()) != null)
            wordcount++;

        addFeature(HostSummaryFeatures.Wordcount, wordcount);
        addFeature(HostSummaryFeatures.OverallWordcount, wordcount);
        addFeature(HostSummaryFeatures.HomePageNum, 1);
        addFeature(HostSummaryFeatures.OrigContentSize, result.getOriginalContent().length());
        return true;
    }

}
