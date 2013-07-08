/**
 * HostBasedDFReducer
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.reduce;

import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;
import hu.sztaki.ilab.bigdata.spam.aggregators.ExistenceAggregator;
import hu.sztaki.ilab.bigdata.spam.aggregators.MeanVarianceAggregator;
import hu.sztaki.ilab.bigdata.spam.constants.Constants;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;

/**
 *
 * @author garzo
 */
public class HostBasedDFReducer extends AbstractContentProcessorReducer {

    protected Map<String, Long> DF = new HashMap<String, Long>();

    @Override
    protected void setup(Context context) {
        try {
            super.setup(context);            
        } catch (IOException ex) {
            LOG.error("Unable to open dictionary file!");
        }        
    }

    @Override
    protected void setFeatureAggregators() {
        for (int i = 0; i < Constants.MaxTopWord; i++) {
            addFeatureAggregator(i, getDictionary().getWord(i), new ExistenceAggregator());
        }
        addFeatureAggregator(Constants.MaxTopWord, "SPAM_length", new MeanVarianceAggregator());
    }

    @Override
    protected void emit(Text hostID, FeatureOutputRecord record, Context context) {
        List<String> terms = record.getFeatureNames();
        for (int i = 0; i < terms.size(); i++) {
            long oldval = 0;
            if (DF.containsKey(terms.get(i))) {
                oldval = DF.get(terms.get(i));
            }
            Double value = (Double)(record.getFeatures().get(i));
            oldval += value.longValue();
            DF.put(terms.get(i), oldval);
        }
    }

    @Override
    protected void cleanup(Context context) {
        FeatureOutputRecord record = new FeatureOutputRecord();
        for (String term : DF.keySet()) {
            record.addFeature(term, DF.get(term));
        }
        try {
            context.write(new Text("DUMMY"), record);
            super.cleanup(context);
        } catch (Exception ex) {
            LOG.warn("Cleanup exception: " + ex.getMessage());
        }        
    }

}
