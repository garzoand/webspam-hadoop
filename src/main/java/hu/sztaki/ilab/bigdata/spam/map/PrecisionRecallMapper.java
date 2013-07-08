package hu.sztaki.ilab.bigdata.spam.map;

import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import hu.sztaki.ilab.bigdata.spam.enums.ItemSets;
import hu.sztaki.ilab.bigdata.spam.enums.features.CorpusPrecisionRecallFeatures;
import hu.sztaki.ilab.bigdata.spam.enums.features.QueryPrecisionRecallFeatures;
import hu.sztaki.ilab.bigdata.spam.features.content.PrecisionRecallCalculator;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;

import org.apache.hadoop.conf.Configuration;

/**
 * Base mapper class for Corpus/Query precision/recall features
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 * 
 */
public class PrecisionRecallMapper extends AbstractContentProcessorMapper {

    @Override
    protected void buildCalculators(ContentProcessorStack stack) {

        Configuration conf = getConf();
        String featureType = conf.get(SpamConfigNames.CONF_PREC_RECALL_FEATURE_TYPE);

        for (ItemSets is : ItemSets.values()) {

            PrecisionRecallCalculator<?> calc;
            if (CorpusPrecisionRecallFeatures.FEATURE_TYPE_CORPUS.equals(featureType)) {

                calc = new PrecisionRecallCalculator<CorpusPrecisionRecallFeatures>(is.getSize(),
                        CorpusPrecisionRecallFeatures.class) {
                };
            }
            else {
                calc = new PrecisionRecallCalculator<QueryPrecisionRecallFeatures>(is.getSize(),
                        QueryPrecisionRecallFeatures.class) {
                };
            }

            calc.setDictionary(this.getDictionary());
            stack.addContentProcessor(calc);
        }
    }

}
