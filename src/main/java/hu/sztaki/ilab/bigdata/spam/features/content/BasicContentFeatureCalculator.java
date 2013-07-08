/**
 * Class for calculating basic content features.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.features.content;

import hu.sztaki.ilab.bigdata.common.parser.OutLink;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.ITokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.common.trigrams.Trigram;
import hu.sztaki.ilab.bigdata.common.trigrams.TrigramComparator;
import hu.sztaki.ilab.bigdata.spam.enums.features.BasicContentFeatures;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.Deflater;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author garzo
 */
public class BasicContentFeatureCalculator extends AbstractContentFeatureCalculator {

    private Map<String, Integer> wordFreq = new HashMap<String, Integer>();
    private Map<Trigram, Integer> trigrams = new TreeMap<Trigram, Integer>(new TrigramComparator());
    private Deflater deflater = new Deflater();
    private byte[] buf = new byte[10240];
    private static final Log LOG = LogFactory.getLog(BasicContentFeatureCalculator.class);

    @Override
    public boolean processContent(ParseResult result, HomePageMetaData meta) {
        super.processContent(result, meta);
        
        int wordcount = 0;
        int num_title_words = 0;
        Token token = null;
        String[] tri = new String[3];
        wordFreq.clear();
        trigrams.clear();

        // TODO: add support for stop word filter
        ITokenFilter contentFilter =
                new LowerCaseFilter(
                new WordFilter(
                new StringTokenizerStream(result.getTokenizedContent())));

        ITokenFilter titleFilter =
                new LowerCaseFilter(
                new WordFilter(
                new StringTokenizerStream(result.getTitle())));

        //title
        while ((token = titleFilter.next()) != null) {
            num_title_words++;
        }

        // content
        double avg_length = 0;
        double frac_visible = 0;
        while ((token = contentFilter.next()) != null) {
            avg_length += token.getValue().length();
            
            if (wordFreq.containsKey(token.getValue())) {
                wordFreq.put(token.getValue(), wordFreq.get(token.getValue()) + 1);
            } else {
                wordFreq.put(token.getValue(), 1);
            }
            tri[wordcount % 3] = token.getValue();
            wordcount++;
            if (wordcount > 2) {
                Trigram trigram = new Trigram(tri[wordcount % 3], tri[(wordcount + 1) % 3],
                        tri[(wordcount + 2) % 3]);
                if (trigrams.containsKey(trigram)) {
                    trigrams.put(trigram, trigrams.get(trigram) + 1);
                } else {
                    trigrams.put(trigram, 1);
                }
            }
        }
        if (wordcount > 0) {
            frac_visible = avg_length / result.getOriginalContent().length();
            avg_length /= wordcount;
        }

        // entropy and independent likelihood
        double entropy = 0.0;
        double indep_lh = 0.0;
        if (wordcount > 0) {
            for (String word : wordFreq.keySet()) {
                int freq = wordFreq.get(word);
                entropy +=  -freq / (double)wordcount * Math.log((double)freq / (double)wordcount);
            }
            for (Trigram trigram : trigrams.keySet()) {
                indep_lh += Math.log(trigrams.get(trigram) / ((double)wordcount - 2));
            }
            indep_lh /= -(wordcount - 2);
        }

        // anchor text
        double frac_anchor = 0.0;
        if (wordcount > 0)
        {
            for (OutLink link : result.getOutLinks()) {
                ITokenFilter filter =
                        new WordFilter(
                        new StringTokenizerStream(link.getAnchorText()));
                while ((token = filter.next()) != null) {
                    frac_anchor++;
                }
                frac_anchor /= wordcount;
            }
        }

        // compress rate
        double compress_rate = 0;
        try {
            deflater.setInput(result.getOriginalContent().getBytes("UTF-8"));
            deflater.finish();            
            int compressed_size = deflater.deflate(buf);
            if (deflater.getTotalIn() > 0) {
                compress_rate = (double)compressed_size / (double)deflater.getTotalIn();
            }
        } catch (Exception ex) {
            LOG.error("Exception: " + ex.getMessage());
        }

        addFeature(BasicContentFeatures.WordCount, wordcount);
        addFeature(BasicContentFeatures.NumTitleWords, num_title_words);
        addFeature(BasicContentFeatures.AvgLength, avg_length);
        addFeature(BasicContentFeatures.FracVisible, frac_visible);
        addFeature(BasicContentFeatures.FracAnchor, frac_anchor);
        addFeature(BasicContentFeatures.CompressRate, compress_rate);
        addFeature(BasicContentFeatures.Entropy, entropy);
        addFeature(BasicContentFeatures.IndepLikelihood, indep_lh);
        return true;
    }

}
