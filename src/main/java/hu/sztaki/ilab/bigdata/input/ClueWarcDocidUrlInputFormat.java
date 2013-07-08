/**
 * Input format for ClueWeb obtaining meta data only.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.input;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import edu.umd.cloud9.collection.clue.ClueWarcInputFormat.ClueWarcRecordReader;
import edu.umd.cloud9.collection.clue.ClueWarcRecord;

/**
 * Input format for ClueWarc records. 
 * <br>
 * Input key: docid::url, url::docid , Input value: null 
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 *
 */
public class ClueWarcDocidUrlInputFormat extends
        FileInputFormat<Text, NullWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // Ensure the input files are not splittable.
        return false;
    }

    @Override
    public RecordReader<Text, NullWritable> createRecordReader(
            InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        RecordReader<Text, NullWritable> reader = new WarcDocidUrlRecordReader();
        reader.initialize(is, tac);
        return reader;
    }

    public static class WarcDocidUrlRecordReader extends RecordReader<Text, NullWritable> {

        private static final Pattern DOCID_NUM_PATTERN = Pattern.compile("(\\D+)");
        private static final String KEY_DELIMITER = "::";
        
        private ClueWarcRecordReader reader = new ClueWarcRecordReader();
        private ClueWarcRecord record = null;
        private String url = null;
        private String docId = null;

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException,
                InterruptedException {
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
                        URI uri = new URI(clueURL);
                        url = uri.toString();
                        String warcTrecId = record.getHeaderMetadataItem("WARC-TREC-ID"); 
                        docId = StringUtils.join(DOCID_NUM_PATTERN.split(warcTrecId));
                        done = true;
                    }
                } catch (URISyntaxException ex) {

                }
            }
            return true;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return new Text(docId + KEY_DELIMITER + url);
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return NullWritable.get();
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