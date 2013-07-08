package hu.sztaki.ilab.bigdata.common.tools;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author garzo
 */
public class HostCounter extends Configured implements Tool {

    public static class HostCounterMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        
        @Override
        protected void map(LongWritable key, Text record, Context context) 
                throws IOException, InterruptedException {
            String[] data = record.toString().split(" ");
            if (data.length < 2)
                return;
            try {
                URL url = new URL(data[1]);
                context.write(new Text(url.getHost()), new LongWritable(1));
            } catch (MalformedURLException ex) {
                
            }
        }
        
    }
    
    public static class HostCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable l : values) {
                sum += l.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
    
    
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: HostCounter [input] [output]");
            System.exit(1);
        }
        String[] oargs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        Job job = new Job(getConf(), "HostCounter");
        Configuration conf = job.getConfiguration();
        job.setMapperClass(HostCounterMapper.class);
        job.setCombinerClass(HostCounterReducer.class);
        job.setReducerClass(HostCounterReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setJarByClass(HostCounter.class);
        job.setNumReduceTasks(43);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.submit();
        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HostCounter(), args);
        System.exit(res);
    }
    
}
