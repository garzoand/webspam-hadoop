/**
 * ArffRecordWriter.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.output;

import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author garzo
 */
public abstract class GenericArffRecordWriter extends RecordWriter<Text, FeatureOutputRecord> {
        
    protected boolean first = true;
    protected DataOutputStream out;
    protected static final String utf8 = "UTF-8";
    protected static final byte[] newline;
    protected static final byte[] separator;

    static {
        try {
            newline = "\n".getBytes(utf8);
            separator = "\t".getBytes(utf8);
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");
        }
    }

    public GenericArffRecordWriter(DataOutputStream stream) {
        this.out = stream;
    }

    protected abstract void printArffLine(Text key, FeatureOutputRecord record)
            throws IOException;

    protected void writeObject(Object o)
            throws UnsupportedEncodingException, IOException {
        if (o instanceof BytesWritable) {
            BytesWritable bw = (BytesWritable)o;
            out.write(bw.getBytes(), 0, bw.getLength());
        } else {
            out.write(o.toString().getBytes(utf8));
        }
    }

    protected void writeHeader(FeatureOutputRecord record) 
            throws UnsupportedEncodingException, IOException {
        writeObject("@RELATION features\n");
        writeObject("@ATTRIBUTE hostname string\n");
        for (String featureName : (List<String>)record.getFeatureNames()) {
            StringBuilder builder = new StringBuilder();
            builder.append("@ATTRIBUTE ").append(featureName).append(" real\n");
            writeObject(builder.toString());
        }
        writeObject("@DATA\n");        
    }
    
    @Override
    public void write(Text key, FeatureOutputRecord record) throws IOException, InterruptedException {        
        // Text key = (Text)k;
        if (first) {
            writeHeader(record);
            first = false;
        }
        printArffLine(key, record);
    }

    @Override
    public void close(TaskAttemptContext tac)
            throws IOException, InterruptedException {
        out.close();
    }

}
