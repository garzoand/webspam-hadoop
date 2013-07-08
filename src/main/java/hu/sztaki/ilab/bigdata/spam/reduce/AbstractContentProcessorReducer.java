/**
 * AbstractContentProcessorReducer.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.reduce;

import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryBuilder;
import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryStore;
import hu.sztaki.ilab.bigdata.common.dictionary.MultiLangDictionary;
import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;
import hu.sztaki.ilab.bigdata.common.record.FeatureRecord;
import hu.sztaki.ilab.bigdata.spam.aggregators.Aggregator;
import hu.sztaki.ilab.bigdata.spam.constants.Constants;
import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoBuilder;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoRecord;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoStore;
import java.io.IOException;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author garzo
 */
public abstract class AbstractContentProcessorReducer
        extends Reducer<ImmutableBytesWritable, FeatureRecord, Text, FeatureOutputRecord> {

    protected static final Log LOG = LogFactory.getLog(AbstractContentProcessorReducer.class);

    private Map<Integer, Aggregator> aggregators;
    private Map<Integer, String> aggregatedFeatureNames;
    private Set<String> featureFilter;
    private HostInfoStore hostInfoStore = null;
    private DictionaryStore dictionary = null;
    private MultiLangDictionary multiLangDictionary = null;
    private Configuration conf;
    
    @Override
    protected void setup(Context context) throws IOException {
        this.conf = context.getConfiguration();
        aggregators = new HashMap<Integer, Aggregator>();
        aggregatedFeatureNames = new HashMap<Integer, String>();
        featureFilter = new HashSet<String>();
        
         /* dictionary */
        this.dictionary = DictionaryBuilder.buildDictionaryStore(conf);
        if (this.dictionary != null) {
            this.dictionary.initialize(conf);
        }        
                
        /* loads host info store */
        try {
            this.hostInfoStore = HostInfoBuilder.Build(conf);        
            this.hostInfoStore.initialize(conf);
        } catch (Exception ex) {
            LOG.warn("Unable to load host info data: " + ex.getMessage());
        }
        
        setFeatureAggregators();
    }
    
    @Override
    protected void cleanup(Context context) {
        if (hostInfoStore != null) {
            try {
                hostInfoStore.close();
            } catch (Exception ex) {
                LOG.warn("Error while closing hostinfo: " + ex.getMessage());
            }            
        }
    }

    protected void addFeatureAggregator(int featureID, String featureName, Aggregator aggregator) {
        aggregators.put(featureID, aggregator);
        aggregatedFeatureNames.put(featureID, featureName);
    }

    protected void addFeatureFilter(String featureName) {
        featureFilter.add(featureName);
    }

    protected boolean isFilteredFeature(String featureName) {
        return featureFilter.contains(featureName);
    }

    protected abstract void setFeatureAggregators();

    protected void emit(Text hostID, FeatureOutputRecord record, Context context)
            throws IOException, InterruptedException {
        
        /* pass label fetched from host info to output */
        boolean passLabel = context.getConfiguration().getBoolean(
                SpamConfigNames.CONF_HOST_INFO_PASSLABEL, 
                Boolean.parseBoolean(SpamConfigNames.DEFAULT_HOST_INFO_PASSLABEL));
        if (passLabel && hostInfoStore != null) {
            // TODO(garzo): don't fetch twice hostInfoRecord from host info!
            HostInfoRecord hinfo = hostInfoStore.getHostInfoRecord(hostID.toString());
            record.addFeature(Constants.LABEL_OUTPUT_FEATURE, hinfo.getLabel());
        }
        
        context.write(hostID, record);
    }

    protected void addToAggregator(int featureID, double featureValue, Aggregator aggregator) {
        aggregator.add(featureValue);
    }

    protected void postFeatureProcessing(Map<String, Double> featureMap) {
        
    }

    protected FeatureOutputRecord createFeatureOutputRecord(Map<String, Double> featureMap) {
        FeatureOutputRecord record = new FeatureOutputRecord();
        for (String featureName : featureMap.keySet()) {
            if (!isFilteredFeature(featureName)) {
                record.addFeature(featureName, featureMap.get(featureName));
            }
        }
        return record;
    }
    
    public HostInfoStore getHostInfoStore() {
        return this.hostInfoStore;
    }
    
    public DictionaryStore getDictionary() {
        return this.dictionary;
    }
    
    public MultiLangDictionary getMultiLangDictionary() {
        return this.multiLangDictionary;
    }
    
    protected Text determineOutputKey(ImmutableBytesWritable key) {
        if (hostInfoStore != null) {
            HostInfoRecord hinfo = hostInfoStore.getHostInfoRecordByID(Bytes.toLong(key.get()));
            if (hinfo != null) {
                return new Text(hinfo.getHostName());
            } else {
                return new Text(key.get());
            }
        } else {
            return new Text(key.get());
        }
    }

    @Override
    protected void reduce(ImmutableBytesWritable hostID, Iterable<FeatureRecord> records, Context context)
            throws IOException, InterruptedException {

        for (FeatureRecord features : records) {
            for (Integer featureID : features.getFeatures().keySet()) {
                Aggregator aggregator = aggregators.get(featureID);
                if (aggregator == null) {
                    LOG.warn("Aggregator wasn't set to feature " + featureID);
                } else {
                    addToAggregator(featureID, features.getFeatures().get(featureID),
                            aggregator);
                }
            }
        }

        Map<String, Double> featureMap = new TreeMap<String, Double>();
        for (Integer featureID : aggregators.keySet()) {
            Aggregator aggregator = aggregators.get(featureID);
            String featureName = aggregatedFeatureNames.get(featureID);
            double[] results = aggregator.aggregate();
            String labels[] = aggregator.getLabels();
            for (int i = 0; i < results.length; i++) {
                featureMap.put(featureName + labels[i], results[i]);
            }
            aggregator.reset();
        }

        postFeatureProcessing(featureMap);
        emit(determineOutputKey(hostID), createFeatureOutputRecord(featureMap), context);
    }
    
    protected Configuration getConf() {
        return conf;
    }

}
