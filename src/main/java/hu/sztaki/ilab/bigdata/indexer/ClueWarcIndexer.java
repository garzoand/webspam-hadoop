package hu.sztaki.ilab.bigdata.indexer;


import hu.sztaki.ilab.bigdata.indexer.HitRecord.Hit;
import hu.sztaki.ilab.bigdata.input.ClueWarcAdaptedInputFormat;
import hu.sztaki.ilab.bigdata.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.tokenize.Token;
import hu.sztaki.ilab.bigdata.tokenize.filter.ITokenFilter;
import hu.sztaki.ilab.bigdata.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.tokenize.stream.StringTokenizerStream;
import hu.sztaki.ilab.bigdata.utils.HadoopUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.HTMLElementName;
import net.htmlparser.jericho.Source;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Creates block-compressed SequenceFiles from ClueWarc files.
 * The 'fieldType:position' lists are created/sorted at mapper side
 * <pre>
 * {@code
 * SequenceFile structure: (<term:docid>, <fieldType:position>,<fieldType:position>...)
 * }
 * </pre>
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 */
public class ClueWarcIndexer extends Configured implements Tool {

    private enum HtmlParser {
        DOC_NPE, ELEMENT_NPE, DOC_STACKOVERFLOW, ELEMENT_STACKOVERFLOW, NULL_ELEMENTS
    }
    
    private static String programName = "ClueWarcIndexer";
    private static String copyrightText = "(C) 2012 MTA SZTAKI";

    private static void printHelp(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp(
                "ClueWarcIndexer <OPTIONS>",
                "\nCreates block-compressed SequenceFiles from ClueWarc files.\nSequenceFile structure: (<term:docid>, <fieldType:position>)\n",
                options, "\nCopyright (c) 2012, MTA SZTAKI. Web: http://datamining.sztaki.hu/\n\n");
    }

    @SuppressWarnings("static-access")
    private static Options getJobOptions() {

        Options jobOptions = new Options();

        Option helpOpt = OptionBuilder
            .withDescription("Prints this help")
            .create("help");

        Option inputOpt = OptionBuilder
            .withArgName("i")
            .withLongOpt("input")
            .hasArg()
            .withDescription("Comma separated list of input directories")
            .isRequired()
            .create("i");

        Option outputOpt = OptionBuilder
            .withArgName("o")
            .withLongOpt("output")
            .hasArg()
            .withDescription("Output directory")
            .isRequired()
            .create("o");

        jobOptions.addOption(helpOpt);
        jobOptions.addOption(inputOpt);
        jobOptions.addOption(outputOpt);

        return jobOptions;
    }

    public static class ClueWarcIndexerMapper extends
            Mapper<Text, HomePageContentRecord, Text, HitRecord> {

        private static final Pattern DOCID_NUM_PATTERN = Pattern.compile("(\\D+)");
        private static final Map<String, Byte> HTML_ELEMENTS = new HashMap<String, Byte>();
        static {
            HTML_ELEMENTS.put(HTMLElementName.TITLE, (byte)0);
            HTML_ELEMENTS.put(HTMLElementName.BODY, (byte)1);
        }
        
        private void parseContent(Source source, String htmlElementName, String docId,
                Map<String, HitRecord> terms, Context context) throws IOException, InterruptedException {
            
            List<Element> elements = null;
            try {
                elements = source.getAllElements(htmlElementName);
            }
            catch (NullPointerException ex) {
                context.getCounter(HtmlParser.DOC_NPE).increment(1);
                return;
            }
            catch (StackOverflowError ex) {
                context.getCounter(HtmlParser.DOC_STACKOVERFLOW).increment(1);
            }
            
            if (elements == null) {
                context.getCounter(HtmlParser.NULL_ELEMENTS).increment(1);
                return;
            }
            
            for (Element e : elements) {
                
                String extractedText = null;
                try {
                    extractedText = e.getTextExtractor().toString();
                }
                catch (NullPointerException ex) {
                    context.getCounter(HtmlParser.ELEMENT_NPE).increment(1);
                    continue;
                }
                catch (StackOverflowError ex) {
                    context.getCounter(HtmlParser.ELEMENT_STACKOVERFLOW).increment(1);
                    continue;
                }
                
                ITokenFilter contentFilter = 
                    new PorterStemmerFilter(new LowerCaseFilter(new WordFilter(
                        new StringTokenizerStream(extractedText))));

                Token token;
                int counter = 0;
                while ((token = contentFilter.next()) != null) {

                    String termKey = new StringBuilder(token.getValue())
                        .append(":")
                        .append(docId).toString();
                   
                    
                    HitRecord hitRecord =  terms.get(termKey);
                    if (hitRecord == null) {
                        hitRecord = new HitRecord();
                    }
                    
                    Hit hit = new Hit(HTML_ELEMENTS.get(htmlElementName), ++counter);
                    hitRecord.addHit(hit);
                    terms.put(termKey, hitRecord);
                    
                }
            }
        }
        
        @Override
        protected void map(Text key, HomePageContentRecord record, Context context)
                throws IOException, InterruptedException {
            
            Source source = new Source(record.getContent());
            String collectionId = record.getMetaData().getCollectionID();
            String docId = StringUtils.join(DOCID_NUM_PATTERN.split(collectionId));
            
            Map<String, HitRecord> terms = new TreeMap<String, HitRecord>();
            
            parseContent(source, HTMLElementName.TITLE, docId, terms, context);
            parseContent(source, HTMLElementName.BODY, docId, terms, context);

            for (Map.Entry<String, HitRecord> t : terms.entrySet()) {
                
                context.write(new Text(t.getKey()), t.getValue());
            }

        }

    }
    

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        
        Job job = new Job(conf, programName + " " + copyrightText);

        // parse generic options
        String[] genericCmdLineOpts = new GenericOptionsParser(conf, args).getCommandLine()
                .getArgs();

        // parse job specific options
        Options jobAvailableOptions = getJobOptions();
        Parser jobCmdOptParser = new GnuParser();
        CommandLine cli = null;

        try {
            cli = jobCmdOptParser.parse(jobAvailableOptions, genericCmdLineOpts, true);

            if (cli.hasOption('h')) {
                printHelp(jobAvailableOptions);
                System.exit(0);
            }

            for (Option o : cli.getOptions()) {

                if ("i".equals(o.getOpt())) {
                    //TODO lbendig, May 10, 2012 : test!
                    //FileInputFormat.addInputPaths(job, o.getValue());
                    FileSystem fs = FileSystem.get(conf);
                    Path[] paths = HadoopUtils.getRecursivePaths(fs, o.getValue());
                    FileInputFormat.setInputPaths(job, paths);
                }
                else if ("o".equals(o.getOpt())) {
                    FileOutputFormat.setOutputPath(job, new Path(o.getValue()));
                }
            }

            job.setJarByClass(ClueWarcIndexer.class);

            job.setMapperClass(ClueWarcIndexerMapper.class);
            job.setNumReduceTasks(0);

            job.setInputFormatClass(ClueWarcAdaptedInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(HitRecord.class);

            SequenceFileOutputFormat.setCompressOutput(job, true);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

            job.waitForCompletion(true);
            
            return 0;

        }
        catch (MissingOptionException ex) {
            System.out.println(ex.getMessage());
            printHelp(jobAvailableOptions);
            System.exit(1);
        }

        return 0;

    }

    public static void main(String[] args) throws Exception {
        System.out.println(programName + " " + copyrightText);
        int res = ToolRunner.run(new Configuration(), new ClueWarcIndexer(), args);
        System.exit(res);
    }

}