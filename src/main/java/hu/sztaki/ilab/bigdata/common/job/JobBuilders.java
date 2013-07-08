/**
 * JobBuilders.java
 * Set of utils for building Hadoop Job objects.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.job;

import hu.sztaki.ilab.bigdata.common.constants.ConfigNames;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author garzo
 */
public class JobBuilders {
    
    public static void buildDefaultHadoopJob(CmdOptHelper options, Job job) 
            throws IOException, ClassNotFoundException {
        Configuration conf = job.getConfiguration();
        
        if (options.hasOption(Parameter.REDUCER)) {
            conf.setInt(ConfigNames.CONF_REDUCER_NUM, 
                    Integer.parseInt(options.getOptionValue(Parameter.REDUCER)));
        }
        
        if (options.hasOption(Parameter.INPUT_DIR)) {
            String[] inputDirs = options.getOptionValue(Parameter.INPUT_DIR).split(",");
            for (String dir : inputDirs) {
                FileInputFormat.addInputPath(job, new Path(dir));
            }
        }
        
        if (options.hasOption(Parameter.OUTPUT_DIR)) {
            FileOutputFormat.setOutputPath(job, new Path(
                    options.getOptionValue(Parameter.OUTPUT_DIR)));
        }
        
        /* hbase output */
        if (options.hasOption(Parameter.HBASE_TABLE)) {
            conf.set(ConfigNames.CONF_OUTPUT_HBASE_TABLE, 
                    options.getOptionValue(Parameter.HBASE_TABLE));
        }

        if (options.hasOption(Parameter.HBASE_QUALIFIER)) {
            conf.set(ConfigNames.CONF_OUTPUT_HBASE_QUALIFIER, 
                    options.getOptionValue(Parameter.HBASE_QUALIFIER));
        }
        
        if (options.hasOption(Parameter.HBASE_CONF)) {
            conf.addResource(new Path(options.getOptionValue(Parameter.HBASE_CONF)));
        }
        
        System.out.println("Setting up job parameters ...");
        JobUtils.setInputOutputFormat(job);
        JobUtils.setNumberOfReducers(job);
        JobUtils.setupDictionary(job);
    }
    
}
