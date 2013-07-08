/**
 * TokenizedTextInputFormat.java
 * Input format which can read tokenized web content from text files
 * The format of input text file must be the following: url|title|content
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.input;

import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.utils.HashUtils;
import hu.sztaki.ilab.bigdata.common.utils.HostUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
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
public class TokenizedTextInputFormat extends FileInputFormat<Text, HomePageContentRecord> {
    
    public static class TokenizedTextRecordReader extends RecordReader<Text, HomePageContentRecord> {
        
        public final static int URL_FIELD = 0;
        public final static int TITLE_FIELD = 1;
        public final static int CONTENT_FIELD = 2;
        public final static int FIELDS_NUM = 3;
        public final static String SEPARATOR = "\\|";
        
        private LineRecordReader lineRecordReader = new LineRecordReader();
        private HomePageMetaData metaData = null;
        private ImmutableBytesWritable content = new ImmutableBytesWritable();
        private String[] data = null;
        
        private static final Log LOG = LogFactory.getLog(TokenizedTextInputFormat.class);

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            lineRecordReader.initialize(is, tac);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (lineRecordReader.nextKeyValue()) {
                Text value = lineRecordReader.getCurrentValue();
                data = value.toString().split(SEPARATOR);
                if (data.length < 3) 
                    continue;
                
                try {
                    metaData = new HomePageMetaData.Builder(data[URL_FIELD], HostUtils.determineHostName(data[URL_FIELD]))
                            .path(HostUtils.determinePath(data[URL_FIELD]))
                            .timestamp(System.currentTimeMillis())
                            .collectionID(HashUtils.hashCode64(data[URL_FIELD]))
                            .build();
                } catch (MalformedURLException ex) {
                    LOG.warn("Page skipped due to malformed url: " + data[URL_FIELD]);
                    continue;
                }
                
                /* building content */
                StringBuilder builder = new StringBuilder(data[TITLE_FIELD]);
                builder.append(" ").append(data[CONTENT_FIELD]);
                content.set(builder.toString().getBytes());
                
                return true;                                               
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text(data[URL_FIELD]);
        }

        @Override
        public HomePageContentRecord getCurrentValue() throws IOException, InterruptedException {            
            return new HomePageContentRecord(new BytesWritable(content.get()), metaData);
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
    public RecordReader<Text, HomePageContentRecord> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new TokenizedTextRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        CompressionCodec codec = 
            new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
        return codec == null;
    }        
    
}
