/**
 * WordCounter
 * Creates a dictionary from collection
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.tools;

import hu.sztaki.ilab.bigdata.common.job.CmdOptHelper;
import hu.sztaki.ilab.bigdata.common.job.JobBuilders;
import hu.sztaki.ilab.bigdata.common.job.JobUtils;
import hu.sztaki.ilab.bigdata.common.job.Parameter;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.StopWordFilter;
import hu.sztaki.ilab.bigdata.common.trie.EmittableDictionaryTrie;
import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import hu.sztaki.ilab.bigdata.spam.stacks.WordCountProcessorStack;
import hu.sztaki.ilab.bigdata.spam.wordcount.CountEveryWordStrategy;
import hu.sztaki.ilab.bigdata.spam.wordcount.CountJustOnceStrategy;
import hu.sztaki.ilab.bigdata.spam.wordcount.IWordCountStrategy;
import hu.sztaki.ilab.bigdata.spam.wordcount.WordCountProcessor;
import java.io.IOException;
import java.net.URI;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author garzo
 * TODO(garzo) need to refactor !
 */
public class WordCounter extends Configured implements Tool {

    private static final String PROGRAM_NAME = "WordCounter";
    private static String PROGRAM_DESCRIPTION = "Stems and counts each token in the collection";
    private static String COPYRIGHT = "(C) 2011 MTA SZTAKI";
    
    private static final String COUNT_ONCE_CONF = "bigdata.spam.wordcount.countonce";

    public static class WordCounterMapper extends Mapper<Text, HomePageContentRecord, Text, VLongWritable> {

        private EmittableDictionaryTrie trie = null;
        private StopWordFilter stopWordFilter = null;
        private WordCountProcessorStack processorStack = null;                

        private static final Log LOG = LogFactory.getLog(WordCounterMapper.class);
        
        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path[] cachedFiles = DistributedCache.getLocalCacheFiles(conf);
            if (cachedFiles != null) {
                for (Path cachedFile : cachedFiles) {
                    stopWordFilter = new StopWordFilter();
                    stopWordFilter.loadFromFile(cachedFile.toString());
                }
            }
            
            try {
                IWordCountStrategy wordcountStrategy = null;
                if (conf.getBoolean(COUNT_ONCE_CONF, false)) {
                    wordcountStrategy = new CountJustOnceStrategy();
                } else {
                    wordcountStrategy = new CountEveryWordStrategy();
                }
                
                processorStack = new WordCountProcessorStack(JobUtils.createParsingStrategyClass(conf));                               
                WordCountProcessor processor = new WordCountProcessor(wordcountStrategy);
                processor.setStemmer(JobUtils.createStemmer(conf));
                processor.setMinLength(conf.getInt(SpamConfigNames.CONF_WORDCOUNT_MINLENGTH, 
                        Integer.parseInt(SpamConfigNames.DEFAULT_WORDCOUNT_MINLENGTH)));
                processorStack.addProcessor(processor);
                if (stopWordFilter != null) {
                    processor.setStopWordFilter(stopWordFilter);
                }
            } catch (Exception ex) {
                LOG.error("Exception: " + ex.getMessage());
                throw new IOException("Cannot instantiate parsing or stemming strategy!");
            }
        }

        @Override
        protected void map(Text key, HomePageContentRecord record, Context context) throws IOException, InterruptedException {
            trie = new EmittableDictionaryTrie();
            trie.setContext(context);
            processorStack.compute(record.getContent(), trie);
        }

    }

    public static class WordCounterReducer extends Reducer<Text, VLongWritable, Text, VLongWritable> {

        @Override
        protected void reduce(Text key, Iterable<VLongWritable> records, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (VLongWritable l : records) {
                sum += l.get();
            }
            context.write(key, new VLongWritable(sum));
        }

    }
    
    private CmdOptHelper init(String[] args) {
        CmdOptHelper options = null;
        try {
            options = CmdOptHelper.create(PROGRAM_NAME)
                    .setDescription(PROGRAM_DESCRIPTION)
                    .setCopyright(COPYRIGHT)
                    .addParameter(Parameter.INPUT_DIR, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.OUTPUT_DIR, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.REDUCER, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter(Parameter.STOPWORDS, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter("countonce", "Count each word once per page. Suitable for calculating document frequency",
                        false, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .ParseOptions(args);                
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            options.printHelp();
            System.exit(1);
        }
        return options;        
    }

    public int run(String[] args) throws Exception {
        String[] oargs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        Configuration conf = getConf();
        Job job = new Job(conf, PROGRAM_NAME + " " + COPYRIGHT);
        CmdOptHelper options = init(args);
        JobBuilders.buildDefaultHadoopJob(options, job);
        
        // stop words
        if (options.hasOption(Parameter.STOPWORDS)) {
            URI queryUri = new URI(options.getOptionValue(Parameter.STOPWORDS));
            DistributedCache.addCacheFile(queryUri, job.getConfiguration());
            DistributedCache.createSymlink(job.getConfiguration());            
        }
        
        conf.setBoolean(COUNT_ONCE_CONF, options.hasOption("countonce"));
                        
        if (job.getNumReduceTasks() < 1) {
            System.out.println("ERROR: more than one reducer needed");
            System.exit(1);
        }

        job.setJarByClass(WordCounter.class);
        job.setMapperClass(WordCounterMapper.class);
        job.setReducerClass(WordCounterReducer.class);
        job.setCombinerClass(WordCounterReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VLongWritable.class);

        job.submit();
        System.out.println("Job submitted.");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(PROGRAM_NAME + " " + COPYRIGHT);
        int res = ToolRunner.run(new Configuration(), new WordCounter(), args);
        System.exit(res);
    }

}
