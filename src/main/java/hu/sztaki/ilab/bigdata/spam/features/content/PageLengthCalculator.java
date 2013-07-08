/**
 * PageLengthCalculator.java
 * Calculates length of a page.
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

/**
 *
 * @author garzo
 */
public class PageLengthCalculator extends AbstractContentFeatureCalculator {

    public boolean processContent(ParseResult result, HomePageMetaData meta) {
        ITokenStream stream =
                new PorterStemmerFilter(
                new LowerCaseFilter(
                new WordFilter(
                new StringTokenizerStream(result.getTokenizedContent()))));
        Token token = null;

        int wordcount = 0;
        while ((token = stream.next()) != null)
            wordcount++;

        /*addFeature("f_length", result.getTokenizedContent().length());
        addFeature("f_wordcount", wordcount);
        addFeature("f_pagenum", 1);*/
        return true;
    }

}
