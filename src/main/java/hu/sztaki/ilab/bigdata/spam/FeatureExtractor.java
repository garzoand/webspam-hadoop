/**
 * FeatureExtractor.java
 * Main hadoop job for feature extraction.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam;

import hu.sztaki.ilab.bigdata.common.job.CmdOptHelper;
import hu.sztaki.ilab.bigdata.common.job.JobBuilders;
import hu.sztaki.ilab.bigdata.common.job.Parameter;
import hu.sztaki.ilab.bigdata.common.record.FeatureRecord;
import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoBuilder;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
public class FeatureExtractor extends Configured implements Tool {

    public static final String PROGRAM_NAME = "Spam Feature Extractor";
    public static final String PROGRAM_DESC = "Extracts spam features from content";
    public static final String COPYRIGHT = "(C) 2012 MTA SZTAKI";

    private CmdOptHelper buildOptions(String[] args) {
        CmdOptHelper options = null;
        try {
            options = CmdOptHelper.create(PROGRAM_NAME)
                    .setDescription(PROGRAM_DESC)
                    .setCopyright(COPYRIGHT)
                    .addParameter(Parameter.INPUT_DIR, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.OUTPUT_DIR, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.REDUCER, CmdOptHelper.ParameterType.NOT_REQUIRED)                   
                    .ParseOptions(args);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            options.printHelp();
            System.exit(1);
        }
        return options;
    }
    
    private void initHostInfo(Configuration conf, CmdOptHelper options) {               
        // setting up host info
        try {
            HostInfoStore hostInfoStore = HostInfoBuilder.Build(conf);
            if (hostInfoStore != null) {
                hostInfoStore.setup(conf);                
            }
        } catch (Exception ex) {
            System.out.println("Unable to set host info: " + ex.getMessage());
        }              
    }
    
    public int run(String[] args) throws Exception {
        String[] oargs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        Job job = new Job(getConf(), PROGRAM_NAME + " " + COPYRIGHT);
        Configuration conf = job.getConfiguration();
        CmdOptHelper options = buildOptions(oargs);
        JobBuilders.buildDefaultHadoopJob(options, job);
        initHostInfo(conf, options);
        
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(FeatureRecord.class);
        job.setMapperClass((Class<? extends Mapper>)Class.forName(conf.get(SpamConfigNames.CONF_MAPPER_CLASS,
                SpamConfigNames.DEFAULT_MAPPER_CLASS)));
        job.setReducerClass((Class<? extends Reducer>)Class.forName(conf.get(SpamConfigNames.CONF_REDUCER_CLASS,
                SpamConfigNames.DEFAULT_REDUCER_CLASS)));
        job.setJarByClass(FeatureExtractor.class);
        job.submit();
        System.out.println("Job submitted.");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(PROGRAM_NAME + " " + COPYRIGHT);
        int res = ToolRunner.run(new Configuration(), new FeatureExtractor(), args);
        System.exit(res);
    }

}
