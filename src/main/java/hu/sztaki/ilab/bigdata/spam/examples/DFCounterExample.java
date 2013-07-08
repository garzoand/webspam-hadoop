/**
 * DFCounterExample.java
 * Calculates NGram, free and powerset document frequencies.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.examples;

import hu.sztaki.ilab.bigdata.common.input.ClueWarcAdaptedInputFormat;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.StopWordFilter;
import hu.sztaki.ilab.bigdata.common.trie.EmittableDictionaryTrie;
import hu.sztaki.ilab.bigdata.spam.stacks.WordCountProcessorStack;
import hu.sztaki.ilab.bigdata.spam.wordcount.query.FreeDFCounter;
import hu.sztaki.ilab.bigdata.spam.wordcount.query.NGramDFCounter;
import hu.sztaki.ilab.bigdata.spam.wordcount.query.PowerSetDFCounter;
import hu.sztaki.ilab.bigdata.spam.wordcount.query.QueryWordCountProcessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author garzo
 */
public class DFCounterExample extends Configured implements Tool {

    private static String programName = "HadoopSpamCode DFCounterExample";
    private static String copyrightText = "(C) 2011 MTA SZTAKI";
     
    private static int printUsage() {
        System.out.println("DFCounterExample\n" +
                "   -reducer <reducer num>\n" +
                "   -input <list of input directories>\n" +
                "   -query <file name of queries>\n" +
                "   -stopwords <file containing stop words (need to be named stopwords.txt>\n" +
                "   -output <output directory>\n");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class DFCounterExampleMapper extends Mapper<Text, HomePageContentRecord, Text, VLongWritable> {

        private EmittableDictionaryTrie trie = new EmittableDictionaryTrie();
        private WordCountProcessorStack processorStack = new WordCountProcessorStack();
        private StopWordFilter stopWordFilter = null;
        private static final Log LOG = LogFactory.getLog(DFCounterExampleMapper.class);

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path[] cachedFiles = DistributedCache.getLocalCacheFiles(conf);
            List<String> queries = new ArrayList<String>();
            for (Path cachedFile : cachedFiles) {
                if (cachedFile.toString().contains("stopwords.txt")) {
                    stopWordFilter = new StopWordFilter();
                    stopWordFilter.loadFromFile(cachedFile.toString());
                } else {
                    BufferedReader reader = new BufferedReader(new FileReader(cachedFile.toString()));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        queries.add(line);
                    }
                    reader.close();
                }
            }
            LOG.info("Query num: " + queries.size());
            for (String query : queries) {
                LOG.debug(query);
            }

            QueryWordCountProcessor processor1 = new QueryWordCountProcessor(new FreeDFCounter());
            QueryWordCountProcessor processor2 = new QueryWordCountProcessor(new NGramDFCounter());
            QueryWordCountProcessor processor3 = new QueryWordCountProcessor(new PowerSetDFCounter());
            processor1.setPrefixChar('F');
            processor2.setPrefixChar('D');
            processor3.setPrefixChar('S');
            processor1.setQueries(queries);
            processor2.setQueries(queries);
            processor3.setQueries(queries);
            if (stopWordFilter != null) {
                processor1.setStopWordFilter(stopWordFilter);
                processor2.setStopWordFilter(stopWordFilter);
                processor3.setStopWordFilter(stopWordFilter);
            }
            processorStack.addProcessor(processor1);
            processorStack.addProcessor(processor2);
            processorStack.addProcessor(processor3);
        }

        @Override
        protected void map(Text key, HomePageContentRecord record, Context context) throws IOException, InterruptedException {
            trie = new EmittableDictionaryTrie();
            trie.setContext(context);
            processorStack.compute(record.getContent(), trie);
        }

    }

    public static class DFCounterExampleReducer extends Reducer<Text, VLongWritable, Text, VLongWritable> {

        @Override
        protected void reduce(Text key, Iterable<VLongWritable> records, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (VLongWritable l : records) {
                sum += l.get();
            }
            context.write(key, new VLongWritable(sum));
        }
        
    }

    public int run(String[] args) throws Exception {
        String[] oargs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        Job job = new Job(getConf(), programName + " " + copyrightText);
        // parse commad line parameters
        for (int i = 0; i < oargs.length; i++) {
            try {
                if ("-reducer".equals(oargs[i])) {
                    job.setNumReduceTasks(Integer.parseInt(oargs[++i]));
                } else if ("-output".equals(oargs[i])) {
                    FileOutputFormat.setOutputPath(job, new Path(oargs[++i]));
                } else if ("-query".equals(oargs[i])) {
                    URI queryUri = new URI(oargs[++i]);
                    DistributedCache.addCacheFile(queryUri, job.getConfiguration());
                    DistributedCache.createSymlink(job.getConfiguration());
                } else if ("-stopwords".equals(oargs[i])) {
                    URI queryUri = new URI(oargs[++i]);
                    DistributedCache.addCacheFile(queryUri, job.getConfiguration());
                    DistributedCache.createSymlink(job.getConfiguration());
                } else if ("-input".equals(oargs[i])) {
                    String[] inputDirs = oargs[++i].split(",");
                    for (String p : inputDirs)
                        FileInputFormat.addInputPath(job, new Path(p));
                } else {
                    System.out.println("Unknown parameter: " + oargs[i]);
                }
            } catch (NumberFormatException e) {
                System.out.println("ERROR: integer expected instead of "
                        + oargs[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("ERROR: required parameter missing from " +
                        oargs[i - 1]);
                return printUsage();
            }
        }
        if (job.getNumReduceTasks() < 1) {
            System.out.println("ERROR: more than one reducer needed");
            return printUsage();
        }

        job.setJarByClass(DFCounterExample.class);
        job.setMapperClass(DFCounterExampleMapper.class);
        job.setReducerClass(DFCounterExampleReducer.class);
        job.setCombinerClass(DFCounterExampleReducer.class);
        job.setInputFormatClass(ClueWarcAdaptedInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VLongWritable.class);

        job.submit();
        System.out.println("Job submitted.");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(programName + " " + copyrightText);
        int res = ToolRunner.run(new Configuration(), new DFCounterExample(), args);
        System.exit(res);
    }
    
}
