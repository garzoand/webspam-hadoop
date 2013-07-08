/**
 * Input format for ClueWeb obtaining meta data only.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.input;

import hu.sztaki.ilab.bigdata.common.record.ClueWarcMetaRecord;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.umd.cloud9.collection.clue.ClueWarcInputFormat.ClueWarcRecordReader;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;

/**
 *
 * @author garzo
 */
public class ClueWarcMetaInputFormat extends FileInputFormat<ImmutableBytesWritable, ClueWarcMetaRecord> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // Ensure the input files are not splittable.
        return false;
    }
    
    @Override
    public RecordReader<ImmutableBytesWritable, ClueWarcMetaRecord> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        RecordReader reader = new WarcAdaptorRecordReader();
        reader.initialize(is, tac);
        return reader;
    }
    
    public static class WarcAdaptorRecordReader extends RecordReader<ImmutableBytesWritable, ClueWarcMetaRecord> {
        
        private ClueWarcRecordReader reader = new ClueWarcRecordReader();
        private ClueWarcRecord record = null;
        private Text url = new Text();
        private ClueWarcMetaRecord metaRecord = new ClueWarcMetaRecord();
        private URI uri;

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
            reader.initialize(is, tac);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean done = false;
            while (!done) {
                boolean result = reader.nextKeyValue();              
                if (!result) {
                    return false;
                }                
                try {
                    record = reader.getCurrentValue();
                    String clueURL = record.getHeaderMetadataItem("WARC-Target-URI");
                    if (clueURL != null) {
                        uri = new URI(clueURL);
                        done = true;
                    }
                } catch (URISyntaxException ex) {
                    
                }                
            } 
            return true;
        }

        @Override
        public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
            return new ImmutableBytesWritable(Bytes.toBytes(uri.getHost()));
        }

        @Override
        public ClueWarcMetaRecord getCurrentValue() throws IOException, InterruptedException {                        
            if (uri == null || record == null)
                return null;
            metaRecord = new ClueWarcMetaRecord();
            metaRecord.setHostName(uri.getHost());
            metaRecord.setPath(uri.getPath());
            metaRecord.setTrecID(record.getHeaderMetadataItem("WARC-TREC-ID"));
            return metaRecord;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return reader.getProgress();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
        
    }
   
}
