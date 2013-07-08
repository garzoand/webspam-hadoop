/**
 * FeatureTextOutputFormat.java
 * Text output format which writes key and feature values in one line using tab
 * as the separator
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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author garzo
 */
public class FeatureTextOutputFormat<K, V extends FeatureOutputRecord>
        extends FileOutputFormat<K, V> {
    
    protected static class FeatureRecordWriter<K, V> extends RecordWriter<K, V> {

        private DataOutputStream out;

        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
        private static final byte[] separator;
        private boolean first = true;

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
            if (o instanceof BytesWritable) {
                BytesWritable bw = (BytesWritable)o;
                out.write(bw.getBytes(), 0, bw.getLength());
            } else {
                out.write(o.toString().getBytes(utf8));
            }
        }

        @Override
        public void write(K k, V v) throws IOException, InterruptedException {
            FeatureOutputRecord record = (FeatureOutputRecord)v;
            List<String> featureNames = record.getFeatureNames();
            List<Double> featureValues = record.getFeatures();
            if (first) {
                first = false;
                StringBuilder builder = new StringBuilder();
                builder.append("; ");
                for (String featureName : featureNames) {
                    builder.append(featureName).append(" ");
                }
                writeObject(builder.toString());
                out.write(newline);
            }
            writeObject(k);
            for (int i = 0; i < featureNames.size(); i++) {
                out.write(separator);
                writeObject(featureValues.get(i));
            }
            out.write(newline);
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
