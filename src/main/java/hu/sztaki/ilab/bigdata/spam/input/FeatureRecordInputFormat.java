/**
 * FeatureRecordInputFormat.java
 * Hadoop input format class for reading feature records from text files. This class is
 * able to read files produced by SparseTextFeatureOutputFormat. Use this input
 * format with CalcPredictions hadoop job.

* (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.input;

import hu.sztaki.ilab.bigdata.common.record.FeatureRecord;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

/**
 *
 * @author garzo
 */
public class FeatureRecordInputFormat extends FileInputFormat<Text, FeatureRecord> {
    
    public static class FeatureRecordReader extends RecordReader<Text, FeatureRecord> {

        private static final Log LOG = LogFactory.getLog(FeatureRecordReader.class);
        private static final String utf8 = "UTF-8";
        private static final String separator = " ";
        private static final String featureSeparator = ":";
        
        
        private LineRecordReader lineRecordReader = new LineRecordReader();
        private Text key = new Text();
        private FeatureRecord featureRecord = new FeatureRecord();
        
        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            lineRecordReader.initialize(is, tac);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (lineRecordReader.nextKeyValue()) {
                Text value = lineRecordReader.getCurrentValue();
                String data[] = value.toString().split(separator);
                if (data.length < 2) 
                    continue;
                
                key.set(data[0]);
                featureRecord.getFeatures().clear();
                for (int i = 1; i < data.length; i++) {
                    String featureData[] = data[i].split(featureSeparator);
                    if (featureData.length < 2)
                        continue;
                    featureRecord.getFeatures().put(
                            Integer.parseInt(featureData[0]),
                            Double.parseDouble(featureData[1]));
                }
                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return this.key;
        }

        @Override
        public FeatureRecord getCurrentValue() throws IOException, InterruptedException {
            return this.featureRecord;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }
    
    }
    
    @Override
    public RecordReader<Text, FeatureRecord> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new FeatureRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = 
            new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }        

}
