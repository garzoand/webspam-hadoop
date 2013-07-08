/**
 * SparseArffOutputFormat.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.output;

import hu.sztaki.ilab.bigdata.common.constants.Parameters;
import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryBuilder;
import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryStore;
import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;
import hu.sztaki.ilab.bigdata.spam.constants.Constants;
import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.TreeMap;
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
public class SparseArffOutputFormat<K extends Text, V extends FeatureOutputRecord>
    extends FileOutputFormat<K, V> {

    protected static class SparseArffRecordWriter extends GenericArffRecordWriter {

        private DictionaryStore dictionary = null;
        private boolean showLabels = false;
        private int labelNum = 0;
        
        public SparseArffRecordWriter(DataOutputStream stream) {
            super(stream);
        }

        public void setDictionary(DictionaryStore dictionary) {
            this.dictionary = dictionary;
        }
        
        public void setShowLabels(boolean showLabels) {
            this.showLabels = showLabels;
        }
        
        @Override
        protected void writeHeader(FeatureOutputRecord record) 
                throws UnsupportedEncodingException, IOException {
            writeObject("@RELATION features\n");
            writeObject("@ATTRIBUTE hostname string\n");
            labelNum = 1;
            for (int i = 0; i < Parameters.DICTIONARY_SIZE; i++) {
                StringBuilder builder = new StringBuilder();
                builder.append("@ATTRIBUTE f").append(i).append(" real\n");
                writeObject(builder.toString());
                labelNum++;
            }
            if (showLabels) {
                writeObject("@ATTRIBUTE class {1,0}\n");
            }
            writeObject("@DATA\n");        
        }
        
        @Override
        protected void printArffLine(Text key, FeatureOutputRecord record)
                throws IOException {
            writeObject("{0 ");
            writeObject(key);
            
            TreeMap<Integer, Double> map = new TreeMap<Integer, Double>();
            int label = 0;
            for (int i = 0; i < record.getFeatures().size(); i++) {
                String featureName = (String)record.getFeatureNames().get(i);
                if (Constants.LABEL_OUTPUT_FEATURE.equals(featureName)) {
                    label = ((Double)(record.getFeatures().get(i))).intValue();
                } else {
                    map.put(Integer.parseInt(featureName),
                            (Double)(record.getFeatures().get(i)));
                }
            }
                        
            for (Integer k : map.keySet()) {
                Double v = (Double)map.get(k);
                if (v > 0.0 || v < 0.0) {
                    writeObject(",");
                    writeObject(k + 1);
                    writeObject(" ");
                    writeObject(v);
                }
            }
            if (showLabels) {
                writeObject(",");
                writeObject(labelNum);
                writeObject(" ");
                writeObject(label);
            }
            writeObject("}");
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
        SparseArffRecordWriter writer = new SparseArffRecordWriter(fileOut);
        
        DictionaryStore dictionary = DictionaryBuilder.buildDictionaryStore(conf);
        dictionary.initialize(conf);
        writer.setDictionary(dictionary);
        writer.setShowLabels(conf.getBoolean(
                SpamConfigNames.CONF_HOST_INFO_PASSLABEL,
                Boolean.parseBoolean(SpamConfigNames.DEFAULT_HOST_INFO_PASSLABEL)));
        
        return (RecordWriter<K, V>)writer;
    }
    
}
