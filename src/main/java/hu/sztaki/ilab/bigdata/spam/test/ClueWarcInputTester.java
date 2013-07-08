/**
 * ClueWarcInputTester.java
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.test;

import hu.sztaki.ilab.bigdata.common.input.ClueWarcAdaptedInputFormat;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.spam.examples.DFCounterExample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
public class ClueWarcInputTester extends Configured implements Tool {

    private static String programName = "ClueWarcInputTester";
    private static String copyrightText = "(C) 2011 MTA SZTAKI";

    private static int printUsage() {
        System.out.println("DFCounterExample\n" +
                "   -reducer <reducer num>\n" +
                "   -input <list of input directories>\n" +
                "   -output <output directory>\n");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class ClueWarcInputTesterMapper extends Mapper<Text, HomePageContentRecord, Text, VLongWritable> {

        private boolean first = true;

        @Override
        protected void map(Text key, HomePageContentRecord record, Context context) throws IOException, InterruptedException {
            if (first) {
                for (String s : record.getContent().split(" ")) {
                    context.write(new Text(s), new VLongWritable(1));
                }
                first = false;
            }            
        }
        
    }

    public static class ClueWarcInputTesterReducer extends Reducer<Text, VLongWritable, Text, VLongWritable> {

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
        job.setMapperClass(ClueWarcInputTesterMapper.class);
        job.setReducerClass(ClueWarcInputTesterReducer.class);
        job.setCombinerClass(ClueWarcInputTesterReducer.class);
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
        int res = ToolRunner.run(new Configuration(), new ClueWarcInputTester(), args);
        System.exit(res);
    }

}
