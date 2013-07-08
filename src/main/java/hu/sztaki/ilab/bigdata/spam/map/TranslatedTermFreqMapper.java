/**
 * TranslatedTermFreqMapper.java
 * Host based cross-language term frequency counter
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.map;

import hu.sztaki.ilab.bigdata.common.dictionary.MultiLangDictionary;
import hu.sztaki.ilab.bigdata.spam.features.tfreq.TranslatedTermFreqCalculator;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;

/**
 *
 * @author garzo
 */
public class TranslatedTermFreqMapper extends AbstractContentProcessorMapper {

    @Override
    protected void buildCalculators(ContentProcessorStack stack) {
        TranslatedTermFreqCalculator calculator = new TranslatedTermFreqCalculator();
        calculator.setDictionary((MultiLangDictionary)this.getDictionary());
        stack.addContentProcessor(calculator);
    }
        
}
