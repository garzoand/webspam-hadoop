/**
 * AbstractContentProcessorMapper.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.map;

import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryBuilder;
import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryStore;
import hu.sztaki.ilab.bigdata.common.dictionary.FileDictionaryStore;
import hu.sztaki.ilab.bigdata.common.record.FeatureRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import hu.sztaki.ilab.bigdata.spam.enums.Counters.HOST_INFO_COUNTER;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoBuilder;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoRecord;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoStore;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author garzo
 */
public abstract class AbstractContentProcessorMapper extends
        Mapper<Text, HomePageContentRecord, ImmutableBytesWritable, FeatureRecord> {
    
    private static final Log LOG = LogFactory.getLog(AbstractContentProcessorMapper.class);

    public static enum Counters { RECORD_SKIPPED };

    private HostInfoStore hostInfoStore = null;
    private ContentProcessorStack processorStack = new ContentProcessorStack();
    private DictionaryStore dictionary = null;
    private boolean processOnly = false;
    private Configuration conf;
    
    /* data to emit */
    protected ImmutableBytesWritable key = new ImmutableBytesWritable();
    protected FeatureRecord featureRecord = new FeatureRecord();
    
    /* need to implement */
    protected abstract void buildCalculators(ContentProcessorStack stack);

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        this.conf = conf;
        
         /* Initializing dictionary */
        this.dictionary = DictionaryBuilder.buildDictionaryStore(conf);
        if (this.dictionary != null) {
            dictionary.initialize(conf);
        }

        /* Loading host info store */
        this.hostInfoStore = HostInfoBuilder.Build(conf);
        if (this.hostInfoStore != null) {
            try {
                hostInfoStore.initialize(conf);
                processorStack.setHostInfoStore(hostInfoStore);            
                processOnly = Boolean.parseBoolean(
                        conf.get(SpamConfigNames.CONF_HOST_INFO_PROCESSONLY,
                                SpamConfigNames.DEFAULT_HOST_INFO_PROCESSONLY));
                LOG.info("Process hosts which are included in host info only: " + processOnly);
            } catch (Exception ex) {
                LOG.warn("Unable to load host info : " + ex.getMessage());
            }
        }
        
        /* calls abstract virutal builder method which determines which calculators
         * will be executed during the feature extraction process.  */
        buildCalculators(processorStack);
    }
    
    @Override
    protected void cleanup(Context context) {
        if (hostInfoStore != null) {
            try {                
                hostInfoStore.close();
                LOG.info("Host info store closed.");
            } catch (Exception ex) {
                LOG.warn("Exception while closing host info store: " + ex.getMessage());
            }
        }
    }
    
    public DictionaryStore getDictionary() {
        return this.dictionary;
    }
            

    protected byte[] determineIntermediateKey(HomePageMetaData meta, Context context) {
        if (hostInfoStore == null) {
            return Bytes.toBytes(meta.getHostName());
        } else {
            context.getCounter(HOST_INFO_COUNTER.HOST_INFO_GET).increment(1);
            HostInfoRecord hinfo = hostInfoStore.getHostInfoRecord(meta.getHostName());
            if (hinfo != null) {
                return Bytes.toBytes(hinfo.getHostID());
            } else {
                context.getCounter(HOST_INFO_COUNTER.HOST_INFO_MISS).increment(1);
                return Bytes.toBytes(meta.getHostName());                
            }
        }
    }
    
    protected boolean needProcess(HomePageContentRecord record) {
        if (processOnly && hostInfoStore != null) {
            return hostInfoStore.getHostInfoRecord(record.getMetaData().getHostName()) != null;
        }
        return true;
    }

    @Override
    protected void map(Text rowKey, HomePageContentRecord record, Context context) {
        try {
            if (!needProcess(record)) {
                context.getCounter(Counters.RECORD_SKIPPED).increment(1);
                return;
            }            
            featureRecord.setFeatures(processorStack.execute(record));
            key.set(determineIntermediateKey(record.getMetaData(), context));
            context.write(key, featureRecord);
        } catch (Exception ex) {
            LOG.error("Exception: " + ex.getMessage());
        }
    }
    
    protected Configuration getConf() {
        return conf;
    }
        
    public HostInfoStore getHostInfoStore() {
        return this.hostInfoStore;
    }

}
