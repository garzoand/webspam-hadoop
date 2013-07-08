/**
 * CalcPredictions.java
 * Main hadoop job for calculating spamicity based on precalculated features.
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
import hu.sztaki.ilab.bigdata.spam.input.FeatureRecordInputFormat;
import hu.sztaki.ilab.bigdata.spam.map.PredictionMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author garzo
 */
public class CalcPredictions extends Configured implements Tool {

    public static final String PROGRAM_NAME = "Prediction Calculator";
    public static final String PROGRAM_DESC = "Calculates predictions of spamicity";
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
                    .addParameter(Parameter.MODEL_FILE, CmdOptHelper.ParameterType.REQUIRED)
                    .ParseOptions(args);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            options.printHelp();
            System.exit(1);
        }
        return options;
    }
    
    // copies model file to distributed cache
    private void copyModelFile(Configuration conf) throws IOException {
        String modelFile = conf.get(SpamConfigNames.CONF_CLASSIFIER_MODELFILE, 
                SpamConfigNames.DEFALT_CLASSIFIER_MODELFILE);
        Path modelPath = new Path(modelFile);
        modelPath = modelPath.makeQualified(modelPath.getFileSystem(conf));
        URI uri;
        try {
            uri = new URI(modelPath.toString());
            DistributedCache.addCacheFile(uri, conf);
            DistributedCache.createSymlink(conf);
            System.out.println("Using model file: " + modelFile);
        } catch (URISyntaxException ex) {
            System.out.println("Exception when trying to add dictionary file to distributed cache: "
                    + ex.getMessage());
            System.exit(1);
        }                
    }
    
    public int run(String[] args) throws Exception {
        String[] oargs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        Job job = new Job(getConf(), PROGRAM_NAME + " " + COPYRIGHT);
        Configuration conf = job.getConfiguration();
        CmdOptHelper options = buildOptions(oargs);
        JobBuilders.buildDefaultHadoopJob(options, job);
        if (options.hasOption(Parameter.MODEL_FILE)) {
            conf.set(SpamConfigNames.CONF_CLASSIFIER_MODELFILE, 
                    options.getOptionValue(Parameter.MODEL_FILE));
        }
        copyModelFile(conf);        
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(FeatureRecordInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(PredictionMapper.class);        
        job.setJarByClass(CalcPredictions.class);
        job.submit();
        System.out.println("Job submitted.");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(PROGRAM_NAME + " " + COPYRIGHT);
        int res = ToolRunner.run(new Configuration(), new CalcPredictions(), args);
        System.exit(res);
    }
    
}
