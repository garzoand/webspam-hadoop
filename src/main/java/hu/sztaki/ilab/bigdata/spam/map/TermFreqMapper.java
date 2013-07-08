/**
 * HostBasedDFMapper
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.map;

import hu.sztaki.ilab.bigdata.spam.features.tfreq.TermFreqCalculator;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;

/**
 *
 * @author garzo
 */
public class TermFreqMapper extends AbstractContentProcessorMapper {    
    
    @Override
    protected void buildCalculators(ContentProcessorStack stack) {
        TermFreqCalculator calculator = new TermFreqCalculator();
        calculator.setDictionary(this.getDictionary());
        stack.addContentProcessor(calculator);
    }

}
