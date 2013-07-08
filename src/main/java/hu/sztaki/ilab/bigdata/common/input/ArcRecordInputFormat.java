package hu.sztaki.ilab.bigdata.common.input;

import hu.sztaki.ilab.bigdata.common.constants.ConfigNames;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.utils.HashUtils;
import hu.sztaki.ilab.bigdata.common.utils.UrlUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.httpclient.Header;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;


/**
 * InputFormat for reading ARC records into <Text, HomePageContentRecord> pairs.
 * <pre>
 * - Key : URL of the current record
 * - Value: Record content (mime-type: text/*)
 * </pre>
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 *
 */
public class ArcRecordInputFormat extends FileInputFormat<Text, HomePageContentRecord> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, HomePageContentRecord> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new ArcRecordReader();
    }

    
    /**
     * Converts ARCRecordMetaData into HomePageMetaData
     * 
     * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
     *
     */
    public static class ARCRecordAdapter {

        private String url;
        private HomePageMetaData homepageMetadata = null;
        
        public ARCRecordAdapter(ARCRecordMetaData metadata) {
            
            createHomepageMetadata(metadata);
        }
        
        private void createHomepageMetadata(ARCRecordMetaData metadata) {
            
            this.url = metadata.getUrl();
            this.homepageMetadata = 
                new HomePageMetaData.Builder(url, UrlUtils.getHostFromUrl(url))
                .collectionID(HashUtils.hashCode64(url))
                .ip(UrlUtils.ipAsLong(metadata.getIp()))
                .path(UrlUtils.getPathFromFromUrl(url))
                .timestamp(Long.valueOf(metadata.getDate())).build();
                
        }

        public String getUrl() {
            return url;
        }
        
        public HomePageMetaData getHomepageMetadata() {
            return homepageMetadata;
        }
    }

    
    /**
     * 
     * Wraps ARCReader into RecordReader
     * 
     * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
     *
     */
    public static class ArcRecordReader extends RecordReader<Text, HomePageContentRecord> {
        private static final Log LOG = LogFactory.getLog(ArcRecordReader.class);

        private static final String VALID_MIMETYPE_PREXIX = "text/";
        private static final String HEADER_CONTENT_TYPE = "Content-Type";
        
        private long totalNumBytesRead = 0;
        private float fileSize = 0;
        
        private ArchiveReader reader = null;
        private FSDataInputStream inputData = null;
        private Iterator<ArchiveRecord> iterator = null;
        
        private Text currentKey = new Text();
        private HomePageContentRecord currentValue = new HomePageContentRecord();
        private boolean parseTextualOnly = true;
        
        public ArcRecordReader() {
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            
            parseTextualOnly = context.getConfiguration().getBoolean(
                    ConfigNames.ARC_INPUT_CONTENT_TEXT_ONLY,
                    ConfigNames.DEFAULT_ARC_INPUT_CONTENT_TEXT_ONLY);
              
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path filePath = ((FileSplit) split).getPath();

            inputData = fs.open(filePath);
            FileStatus status = fs.getFileStatus(filePath);
            fileSize = (float) status.getLen();
            reader = ARCReaderFactory.get(filePath.toString(), inputData, true);
            iterator = reader.iterator();
            
            LOG.info("Reading from " + filePath.toString());
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            while (iterator.hasNext()) {
                ARCRecord arcRecord = (ARCRecord) iterator.next();
                arcRecord.skipHttpHeader();
                Header[] httpHeaders = arcRecord.getHttpHeaders();
               
                //skip non textual content by default
                if (parseTextualOnly) {
                    boolean valid = isInvalidMimeType(httpHeaders);
                    if (!valid) {
                        continue;
                    }    
                }

                ARCRecordMetaData metadata = arcRecord.getMetaData();
                totalNumBytesRead += metadata.getLength();
                
                ByteArrayOutputStream contentStream = new ByteArrayOutputStream();
                arcRecord.dump(contentStream);
                
                ARCRecordAdapter adapter = new ARCRecordAdapter(metadata);
                
                currentKey.set(adapter.getUrl());
                currentValue.setMetaData(adapter.getHomepageMetadata());
                currentValue.setContent(contentStream.toByteArray());

                return true;
            }
            return false;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }

        @Override
        public HomePageContentRecord getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }
        
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) totalNumBytesRead / fileSize;
        }
        
        @Override
        public void close() throws IOException {
            reader.close();
        }
        
        private static boolean isInvalidMimeType(Header[] httpHeaders) {
            if (httpHeaders == null) {
                return false;
            }
            for (Header h : httpHeaders) {
                String value = h.getValue();
                if (HEADER_CONTENT_TYPE.equals(h.getName()) && !StringUtils.isEmpty(value)
                        && value.startsWith(VALID_MIMETYPE_PREXIX)) {
                    return true;
                }
            }
            return false;
        }
    }
}
