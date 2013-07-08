/**
 * BasicContentFeatureMapper.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.map;

import hu.sztaki.ilab.bigdata.spam.features.content.BasicContentFeatureCalculator;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;

/**
 *
 * @author garzo
 */
public class BasicContentFeatureMapper extends AbstractContentProcessorMapper {

    @Override
    protected void buildCalculators(ContentProcessorStack stack) {
        stack.addContentProcessor(new BasicContentFeatureCalculator());
    }

}
