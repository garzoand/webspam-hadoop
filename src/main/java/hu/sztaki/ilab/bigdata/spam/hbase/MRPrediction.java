package hu.sztaki.ilab.bigdata.spam.hbase;

import hu.sztaki.ilab.bigdata.common.input.ClueWarcAdaptedInputFormat;
import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;
import hu.sztaki.ilab.bigdata.ml.svm.SVMModel;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author garzo
 */
public class MRPrediction extends Configured implements Tool {
    
    private static String programName = "Spamicity Calculator";
    private static String copyrightText = "(C) 2012 MTA SZTAKI";
    private static int FEATURE_VECT_SIZE = 10000;
    
    @SuppressWarnings("static-access")
    private static void printHelp(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("MRPrediction <OPTIONS>",
                "\nCalculated prediction from features stored in HBase.\n\n", options,
                "\nCopyright (c) 2012, MTA SZTAKI. Web: http://datamining.sztaki.hu/\n\n");
    }    
    
    @SuppressWarnings("static-access")
    private static Options getJobOptions() {

        Options jobOptions = new Options();

        Option helpOpt = OptionBuilder
            .withDescription("Prints this help")
            .create("help");
      
        Option tableOpt = OptionBuilder
            .withArgName("table").hasArg()
            .withDescription("Input HBase table")
            .isRequired()
            .create("table");

        Option colfamOpt = OptionBuilder
            .withArgName("colfam")
            .hasArg()
            .withDescription("Input HBase table column family")
            .isRequired()
            .create("colfam");
      
        Option modelOpt = OptionBuilder
            .withArgName("model").hasArg()
            .withDescription("Model file name")
            .isRequired()
            .create("model");

        Option reducerOpt = OptionBuilder
            .withArgName("reducer").hasArg()
            .withDescription("Number of reducers")
            .isRequired()
            .create("reducer");

        Option outputOpt = OptionBuilder
            .withArgName("output")
            .hasArg()
            .withDescription("Output directorz")
            .isRequired()
            .create("output");

        Option startRowOpt = OptionBuilder
            .withArgName("startRow")
            .hasArg()
            .withDescription("Starting key to scan")            
            .create("startRow");

        Option endRowOpt = OptionBuilder
            .withArgName("endRow")
            .hasArg()
            .withDescription("Stop scan at this key")            
            .create("endRow");
 
        Option confOpt = OptionBuilder
            .withArgName("conf")
            .hasArg()
            .withDescription("Path of hbase-site.xml")            
            .isRequired()
            .create("conf");
              
        jobOptions.addOption(tableOpt);
        jobOptions.addOption(colfamOpt);
        jobOptions.addOption(outputOpt);
        jobOptions.addOption(modelOpt);
        jobOptions.addOption(helpOpt);
        jobOptions.addOption(reducerOpt);
        jobOptions.addOption(startRowOpt);
        jobOptions.addOption(endRowOpt);
        jobOptions.addOption(confOpt);
        return jobOptions;
    }
    
    public static class MRPredictionMapper extends TableMapper<Text, DoubleWritable> {
        
        private SVMModel model = null;
        private static final Log LOG = LogFactory.getLog(MRPredictionMapper.class);
        
        private void initModel(InputStreamReader reader) throws IOException, Exception {
            model = new SVMModel();
            model.AddSVM(reader);
        }
        
        @Override
        public void setup(Context context) {
            try {
                if (model == null) {
                    Path [] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                    if (cacheFiles != null && cacheFiles.length > 0) {
                        InputStreamReader reader = new InputStreamReader(new GZIPInputStream(
                                new FileInputStream(cacheFiles[0].toString())));
                        initModel(reader);
                    }
                }
            } catch (Exception ex) {
                LOG.error(ex.getMessage());
            }
        }
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) 
                throws IOException, InterruptedException {
            FeatureOutputRecord record = new FeatureOutputRecord();                        
            for (KeyValue kv : value.list()) {    
                SplitKeyValue split = kv.split();
                String key = Bytes.toString(split.getRow());
                DataInputStream stream = new DataInputStream(new ByteArrayInputStream(split.getValue()));
                record.readFields(stream);
                                
                // calculates prediction
                double[] vect = new double[FEATURE_VECT_SIZE];
                for (int i = 0; i < record.getFeatures().size(); i++) {                    
                    int idx = Integer.parseInt((String)record.getFeatureNames().get(i));
                    vect[idx] = (Double)record.getFeatures().get(i);
                }
                System.out.println("Calculates kernel ...");
                model.calculateKernel(vect, 0);
                System.out.println("Calculates prediction ...");
                double prediction = model.predict(0, 1);        
                
                LOG.info("Host: " + key + " Prdiction: " + prediction);                
                context.write(new Text(key), new DoubleWritable(prediction));
            }
        }
    }
    
    public int run(String[] args) throws Exception {
        
        /* cmd line options */
        Options jobAvailableOptions = getJobOptions();
        Parser jobCmdOptParser = new GnuParser();
        CommandLine cli = null;
        
        /* hadoop job conf */
        Job job = new Job(getConf(), programName + " " + copyrightText);
        Configuration conf = job.getConfiguration();
        
        String tableName = null;
        String colFamName = null;
        String startRow = null;
        String endRow = null;
        int limit = 0;
        
        try {
            cli = jobCmdOptParser.parse(jobAvailableOptions, args, true);
            if (cli.hasOption("help")) {
                printHelp(jobAvailableOptions);
                System.exit(0);
            }
            // populate options
            for (Option o : cli.getOptions()) {
                
                if ("table".equals(o.getOpt())) {
                    tableName = o.getValue();
                }
                else if ("colfam".equals(o.getOpt())) {
                    colFamName = o.getValue();
                }
                else if ("startRow".equals(o.getOpt())) {
                    startRow = o.getValue();
                }
                else if ("endRow".equals(o.getOpt())) {
                    endRow = o.getValue();
                }
                else if ("reducer".equals(o.getOpt())) {
                    job.setNumReduceTasks(Integer.parseInt(o.getValue()));
                }
                else if ("output".equals(o.getOpt())) {
                    FileOutputFormat.setOutputPath(job, new Path(o.getValue()));
                }
                else if ("conf".equals(o.getOpt())) {
                    conf.addResource(new Path(o.getValue()));
                }                
                else if ("input".equals(o.getOpt())) {
                    String[] inputs = o.getValue().split(",");
                    for (String path : inputs) {
                        FileInputFormat.addInputPath(job, new Path((path)));
                    }
                }
                else if ("model".equals(o.getOpt())) {
                    Path dictionaryPath = new Path(o.getValue());
                    dictionaryPath = dictionaryPath.makeQualified(dictionaryPath.getFileSystem(conf));
                    URI uri = new URI(dictionaryPath.toString() + "#model");
                    DistributedCache.addCacheFile(uri, conf);
                    DistributedCache.createSymlink(conf);                    
                }
            }
        } catch (MissingOptionException ex) {
            System.out.println(ex.getMessage());
            printHelp(jobAvailableOptions);
            System.exit(1);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            printHelp(jobAvailableOptions);
            System.exit(1);
        }
        
        if (tableName == null || colFamName == null) {
            System.out.println("Table name of column family name is missing!");
            return 1;
        }
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        System.out.println("Orig batch size: " + scan.getBatch());
        scan.setBatch(1);
        scan.addFamily(Bytes.toBytes(colFamName));
        if (startRow != null) {
            scan.setStartRow(Bytes.toBytes(startRow));
        }
        if (endRow != null) {
            scan.setStopRow(Bytes.toBytes(endRow));
        }

        job.setInputFormatClass(ClueWarcAdaptedInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setJarByClass(MRPrediction.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        TableMapReduceUtil.initTableMapperJob(tableName, scan, MRPredictionMapper.class, 
                Text.class, DoubleWritable.class, job);
        job.submit();
        System.out.println("Job submitted");
        return 0;
    }
        
    public static void main(String[] args) throws Exception {
        System.out.println(programName + " " + copyrightText);
        int res = ToolRunner.run(new Configuration(), new MRPrediction(), args);
        System.exit(res);
    }
    
}
