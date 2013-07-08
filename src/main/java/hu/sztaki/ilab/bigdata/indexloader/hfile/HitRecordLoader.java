/**
 * HitRecordLoader.java
 * Converts SEQ file contains HitRecords as values and Text as keys into HFiles.
 * Predefined HBase table is requiered in order to determine the bounderies of
 * the regions.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.indexloader.hfile;

import hu.sztaki.ilab.bigdata.indexer.HitRecord;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author garzo
 */
public class HitRecordLoader extends Configured implements Tool {
    
    /* TODO(garzo): derive this class from a global HFile loader class! 
     * Move commonly used methods / properties to the base class.
     */

    private static String programName = "HitRecord HFile Loader";
    private static String copyrightText = "(C) 2012 MTA SZTAKI";    
    private static String columnFamilyConfName = "bigdata.searching.loader.cloumnfamily";
    
    private static int printUsage() {
        System.out.println("HitRecordLoader -i <input files (comma separated>\n" +
                "   -o <output directory>\n" +
                "   -c <column family>\n" +
                "   -t <table name>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }    
    
    public static class HitRecordLoaderMapper
      extends Mapper<Text, HitRecord, ImmutableBytesWritable, KeyValue> {
        
        static enum Counters { INVALID_ROW };
        
        private static final Log LOG = LogFactory.getLog(HitRecordLoaderMapper.class);
        private String columnFamilyName = "idx";
        
        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            columnFamilyName = conf.get(columnFamilyConfName, "idx");
        }
        
        @Override
        protected void map(Text key, HitRecord record, Context context) 
                throws IOException, InterruptedException {
            
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos);
                record.write(out);
                KeyValue kv = new KeyValue(Bytes.toBytes(key.toString()),
                        Bytes.toBytes(columnFamilyName),
                        Bytes.toBytes("x"),
                        baos.toByteArray());
                context.write(new ImmutableBytesWritable(key.toString().getBytes()), kv);
            } catch (Exception ex) {
                LOG.warn("Exception when processing row: " + key.toString());
                context.getCounter(Counters.INVALID_ROW).increment(1);
            }
        }
        
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] oargs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
        // conf.set("hfile.compression", "lzo");
        Job job = new Job(conf, programName);
        String tableName = "";
        String columnFamily = "idx";
        for (int i = 0; i < oargs.length; i++) {
            try {
                if ("-i".equals(oargs[i])) {
                    String[] inputDirs = oargs[++i].split(",");
                    for (String p : inputDirs)
                        FileInputFormat.addInputPath(job, new Path(p));
                } else if ("-o".equals(oargs[i])) {
                    FileOutputFormat.setOutputPath(job, new Path(oargs[++i]));
                } else if ("-t".equals(oargs[i])) {
                    tableName = oargs[++i];
                    System.out.println("Table name: " + tableName);
                } else if ("-c".equals(oargs[i])) {                    
                    columnFamily = oargs[++i];
                    System.out.println("Column family name: " + columnFamily);
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

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(HitRecordLoaderMapper.class);
        job.setJarByClass(HitRecordLoader.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        job.setOutputFormatClass(HFileOutputFormat.class);
        job.setReducerClass(KeyValueSortReducer.class);
        conf.set(columnFamilyConfName, columnFamily);
        
        Configuration hConf = HBaseConfiguration.create(conf);
        HTable htable = new HTable(hConf, tableName);
        HFileOutputFormat.configureIncrementalLoad(job, htable);

        job.submit();
        System.out.println("Job submitted.");
        return 0;        
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println(programName + " " + copyrightText);
        int res = ToolRunner.run(new Configuration(), new HitRecordLoader(), args);
        System.exit(res);        
    }
    
}
