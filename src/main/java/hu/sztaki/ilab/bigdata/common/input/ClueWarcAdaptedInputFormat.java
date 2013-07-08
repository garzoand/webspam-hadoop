/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.input;

import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
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
public class ClueWarcAdaptedInputFormat extends FileInputFormat<Text, HomePageContentRecord> {

    private static final Log LOG = LogFactory.getLog(ClueWarcAdaptedInputFormat.class);

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // Ensure the input files are not splittable.
        return false;
    }

    @Override
    public RecordReader<Text, HomePageContentRecord> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        return new WarcAdaptorRecordReader();
    }

    public static class WarcAdaptorRecordReader extends RecordReader<Text, HomePageContentRecord> {
        private ClueWarcRecordReader reader = new ClueWarcRecordReader();
        private ClueWarcRecord record = null;
        private Text url = new Text();
        private HomePageContentRecord contentRecord = new HomePageContentRecord();
        private HomePageMetaData meta;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            reader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean done = false;
            boolean result = false;
            while (!done) {
                result = reader.nextKeyValue();
                if (result) {
                    record = reader.getCurrentValue();
                    done = processRecordMeta();
                } else {
                    record = null;
                    done = true;
                }
            }
            return result;
        }

        protected boolean processRecordMeta() {
            String rawURI = record.getHeaderMetadataItem("WARC-Target-URI");
            String host;
            String path;
            if (rawURI == null) {
                LOG.warn("Target URI not found, skipping record ...");
                return false;
            }
            try {
                
                // TODO(garzo): use static lib utils here!
                URI uri = new URI(rawURI);                
                meta = new HomePageMetaData.Builder(uri.toString(), uri.getHost())
                        .path(uri.getPath())
                        .collectionID(0) /* !!! */
                        .timestamp(0)
                        .build();
            } catch (URISyntaxException ex) {
                LOG.warn("Bad Uri syntax: " + rawURI + ", skipping record.");
                return false;
            }
            return true;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            url.set(record.getHeaderMetadataItem("WARC-Target-URI"));
            return url;
        }

        @Override
        public HomePageContentRecord getCurrentValue() throws IOException, InterruptedException {
            contentRecord.setMetaData(meta);
            contentRecord.setContent(record.getContentUTF8().getBytes());
            return contentRecord;
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
