/**
 * HostBasedDFCounter.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.tools;

import hu.sztaki.ilab.bigdata.common.constants.ConfigNames;
import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryBuilder;
import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryStore;
import hu.sztaki.ilab.bigdata.common.dictionary.FileDictionaryStore;
import hu.sztaki.ilab.bigdata.common.job.CmdOptHelper;
import hu.sztaki.ilab.bigdata.common.job.JobBuilders;
import hu.sztaki.ilab.bigdata.common.job.JobUtils;
import hu.sztaki.ilab.bigdata.common.job.Parameter;
import hu.sztaki.ilab.bigdata.common.parser.IdentityContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.record.FeatureRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.utils.HashUtils;
import hu.sztaki.ilab.bigdata.spam.features.tfreq.TermFreqCalculator;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoBuilder;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoRecord;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoStore;
import hu.sztaki.ilab.bigdata.spam.stacks.ContentProcessorStack;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author garzo
 */
public class HostBasedDFCounter extends Configured implements Tool {

    private final static String PROGRAM_NAME = "Host Based Document Frequency Counter";
    private final static String PROGRAM_DESC = "Counts how many hosts contains each words of the dictionary";
    private final static String COPYRIGHT = "(C) 2012 MTA SZTAKI";
    
    // TODO(garzo): move it to global config
    private static final int DICTIONARY_SIZE = 10000;

    private CmdOptHelper buildOptions(String[] args) {
        CmdOptHelper options = null;
        try {
            options = CmdOptHelper.create(PROGRAM_NAME)
                    .setDescription(PROGRAM_DESC)
                    .setCopyright(COPYRIGHT)
                    .addParameter(Parameter.INPUT_DIR, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.OUTPUT_DIR, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.DICTIONARY, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter(Parameter.REDUCER, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .ParseOptions(args);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            options.printHelp();
            System.exit(1);
        }
        return options;        
    }

    public static class HostBasedDFCounterMapper extends Mapper<Text, HomePageContentRecord, ImmutableBytesWritable, FeatureRecord> {

        private DictionaryStore dictionaryStore = null;
        private HostInfoStore hostInfoStore = null;
        private ContentProcessorStack processorStack = new ContentProcessorStack(
                new IdentityContentParsingStrategy());                
        
        private ImmutableBytesWritable hostID = new ImmutableBytesWritable();
        private FeatureRecord featureRecord = new FeatureRecord();
        
        private static final Log LOG = LogFactory.getLog(HostBasedDFCounterMapper.class);

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            hostInfoStore = HostInfoBuilder.Build(conf);
            dictionaryStore = DictionaryBuilder.buildDictionaryStore(conf);
            dictionaryStore.initialize(conf);
            
            TermFreqCalculator dfProcessor = new TermFreqCalculator();
            try {
                dfProcessor.setStemmer(JobUtils.createStemmer(context.getConfiguration()));
            } catch (Exception ex) {
                LOG.error("Cannot instantiate stemmer: " + ex.getMessage()); 
            }
            
            dfProcessor.setDictionary(dictionaryStore);
            processorStack.addContentProcessor(dfProcessor);
        }

        @Override
        protected void map(Text key, HomePageContentRecord record, Context context)
                throws IOException, InterruptedException {

            processorStack.execute(record);
            featureRecord.setFeatures(processorStack.getCalculatedFeatures());

            String host = record.getMetaData().getHostName();            
            if (hostInfoStore != null) {
                HostInfoRecord hinfo = hostInfoStore.getHostInfoRecord(host);
                if (hinfo != null) {
                    hostID.set(Bytes.toBytes(hinfo.getHostID()));
                }
            } else {
                hostID.set(Bytes.toBytes(HashUtils.hashCode64(host)));
            }            
            context.write(hostID, featureRecord);
        }
    }

    public static class HostBasedDFCounterCombiner extends Reducer<ImmutableBytesWritable, FeatureRecord, ImmutableBytesWritable, FeatureRecord> {

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<FeatureRecord> records, Context context)
                throws IOException, InterruptedException {
            long tempDF[] = new long[DICTIONARY_SIZE];
            for (FeatureRecord record : records) {
                for (int feature : record.getFeatures().keySet()) {
                    if (feature < DICTIONARY_SIZE) {
                        tempDF[feature]++;
                    }
                }
            }
            Map<Integer, Double> features = new TreeMap<Integer, Double>();
            for (int i = 0; i < DICTIONARY_SIZE; i++) {
                if (tempDF[i] > 0) {
                    features.put(i, (double)tempDF[i]);
                }
            }
            FeatureRecord record = new FeatureRecord();
            record.setFeatures(features);
            context.write(key, record);
        }
    }

    public static class HostBasedDFCounterReducer extends Reducer<ImmutableBytesWritable, FeatureRecord, Text, VLongWritable> {

        private DictionaryStore dictionaryStore = null;
        private long[] DF = new long[DICTIONARY_SIZE];
        HashSet<Integer> wordset = new HashSet<Integer>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            dictionaryStore = DictionaryBuilder.buildDictionaryStore(conf);
            dictionaryStore.initialize(conf);

            for (int i = 0; i < DICTIONARY_SIZE; i++)
                DF[i] = 0;
        }

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<FeatureRecord> records, Context context) {
            long tempDF[] = new long[DICTIONARY_SIZE];
            for (FeatureRecord record : records) {
                for (int feature : record.getFeatures().keySet()) {
                    if (feature < DICTIONARY_SIZE) {
                        tempDF[feature] = 1;
                    }
                }
            }
            for (int i = 0; i < DICTIONARY_SIZE; i++) {
                DF[i] += tempDF[i];
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            VLongWritable v = new VLongWritable();
            for (int i = 0; i < DICTIONARY_SIZE; i++) {
                v.set(DF[i]);
                context.write(new Text(dictionaryStore.getWord(i)), v);
            }
        }
    }

    public int run(String[] args) throws Exception {
        String[] oargs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        Job job = new Job(getConf(), PROGRAM_NAME + " " + COPYRIGHT);
        Configuration conf = job.getConfiguration();
        CmdOptHelper options = buildOptions(oargs);
        JobBuilders.buildDefaultHadoopJob(options, job);
        
        // check whether user set dictionary file manually
        if (options.hasOption(Parameter.DICTIONARY)) {
            conf.setBoolean(ConfigNames.CONF_DICTIONARY_ENABLED, true);
            conf.set(ConfigNames.CONF_DICTIONARY_CLASS, "hu.sztaki.ilab.bigdata.common.dictionary.FileDictionaryStore");
            conf.set(ConfigNames.CONF_DICTIONARY_FILENAME, options.getOptionValue(Parameter.DICTIONARY));
            JobUtils.setupDictionary(job);
            System.out.println("Using dictionary file: " + options.getOptionValue(Parameter.DICTIONARY));
        }

        job.setJarByClass(WordCounter.class);
        job.setMapperClass(HostBasedDFCounterMapper.class);
        job.setReducerClass(HostBasedDFCounterReducer.class);
        job.setCombinerClass(HostBasedDFCounterCombiner.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(FeatureRecord.class);                
        
        job.submit();
        System.out.println("Job submitted.");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(PROGRAM_NAME + " " + COPYRIGHT);
        int res = ToolRunner.run(new Configuration(), new HostBasedDFCounter(), args);
        System.exit(res);
    }

}
