/**
 * SimpleOutputFormat
 * Simply writes feature name, value pairs to text files. Key value will be dropped.
 * Used by host based DF counting.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.output;

import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author garzo
 */
public class SimpleOutputFormat<K, V extends FeatureOutputRecord>
        extends FileOutputFormat<K, V> {

    protected static class FeatureRecordWriter<K, V> extends RecordWriter<K, V> {

        private DataOutputStream out;
                
        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        private static final byte[] separator;

        static {
            try {
                newline = "\n".getBytes(utf8);
                separator = "\t".getBytes(utf8);
            } catch (UnsupportedEncodingException ex) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        public FeatureRecordWriter(DataOutputStream stream) {
            this.out = stream;
        }

        private void writeObject(Object o)
                throws UnsupportedEncodingException, IOException {
            out.write(o.toString().getBytes(utf8));
        }

        @Override
        public void write(K k, V v) throws IOException, InterruptedException {
            FeatureOutputRecord record = (FeatureOutputRecord)v;
            List<String> featureNames = record.getFeatureNames();
            List<Double> featureValues = record.getFeatures();
            for (int i = 0; i < featureNames.size(); i++) {
                writeObject(featureNames.get(i));
                out.write(separator);
                writeObject(featureValues.get(i).longValue());
                out.write(newline);
            }
        }

        @Override
        public void close(TaskAttemptContext tac)
                throws IOException, InterruptedException {
            out.close();
        }

    }

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
            throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream fileOut = fs.create(file, false);
        return new FeatureRecordWriter<K, V>(fileOut);
    }
   
}
