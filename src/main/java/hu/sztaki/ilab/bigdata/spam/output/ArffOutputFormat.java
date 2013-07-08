/**
 * ArffOutputFormat.java
 * Output format for producing output in standard ARFF format
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.output;

import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author garzo
 */
public class ArffOutputFormat<K extends Text, V extends FeatureOutputRecord>
    extends FileOutputFormat<K, V> {

    protected static class ArffRecordWriter extends GenericArffRecordWriter {

        public ArffRecordWriter(DataOutputStream stream) {
            super(stream);
        }

        @Override
        protected void printArffLine(Text key, FeatureOutputRecord record)
                throws IOException {
            writeObject(key);
            for (Double feature : (List<Double>)record.getFeatures()) {
                writeObject(",");
                writeObject(feature);
            }
            out.write(newline);
        }
        
    }
    
    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        Path file = getDefaultWorkFile(job, ".arff");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return (RecordWriter<K, V>) new ArffRecordWriter(fileOut);
    }

}
