package hu.sztaki.ilab.bigdata.indexer;


import hu.sztaki.ilab.bigdata.input.ClueWarcDocidUrlInputFormat;
import hu.sztaki.ilab.bigdata.utils.HadoopUtils;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
 * Reads ClueWarc records and creates plain Sequence files. 
 * SequenceFile structure: ( (<docid>, <url>), (<url>, <docid>))
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 */
public class DocidUrlJob extends Configured implements Tool {

    private static String programName = "DocidUrlJob";
    private static String copyrightText = "(C) 2012 MTA SZTAKI";

    private static void printHelp(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp(
                "DocidUrlJob <OPTIONS>",
                "\nCreates plain SequenceFiles from ClueWarc files.\nSequenceFile structure: (<docid>, <url>, <url>, <docid>)\n",
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

    public static class DocidUrlMapper extends Mapper<Text, NullWritable, Text, Text> {
        
        @Override
        protected void map(Text key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            
            String [] keyValues = key.toString().split("::");
            Text docId = new Text(keyValues[0]);
            Text url = new Text(keyValues[1]);
            
            //emit docId - url
            //context.write(docId, url);

            //emit url - docId
            context.write(url, docId);

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
                    FileSystem fs = FileSystem.get(conf);
                    //TODO lbendig, May 10, 2012 : test!
                    Path[] paths = HadoopUtils.getRecursivePaths(fs, o.getValue());
                    FileInputFormat.setInputPaths(job, paths);
                }
                else if ("o".equals(o.getOpt())) {
                    FileOutputFormat.setOutputPath(job, new Path(o.getValue()));
                }
            }

            job.setJarByClass(DocidUrlJob.class);

            job.setMapperClass(DocidUrlMapper.class);

            job.setInputFormatClass(ClueWarcDocidUrlInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            job.submit();
            System.out.println("Job submitted.");

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
        int res = ToolRunner.run(new Configuration(), new DocidUrlJob(), args);
        System.exit(res);
    }

}