/**
 * HostSummaryMapper.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.map;

import hu.sztaki.ilab.bigdata.spam.features.content.HostSummaryCalculator;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;

/**
 *
 * @author garzo
 */
public class HostSummaryMapper extends AbstractContentProcessorMapper {

    @Override
    protected void buildCalculators(ContentProcessorStack stack) {        
        stack.addContentProcessor(new HostSummaryCalculator());
    }

}
