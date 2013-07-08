/**
 * CustomRecordReader.java
 * RecordReader class for reading content files via IReader interface.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.input.readers;

import hu.sztaki.ilab.bigdata.common.input.readers.IReader;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author garzo
 */
public final class CustomRecordReader extends RecordReader<Text, HomePageContentRecord> {

    private Text key = new Text();
    private HomePageContentRecord value = new HomePageContentRecord();
    private float fileSize = 0;
    private long bytesRead = 0;
    private DataInputStream inputStream = null;
    private IReader reader;

    public CustomRecordReader(IReader reader) {
        this.setReader(reader);
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) 
            throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path mFilePath = ((FileSplit) split).getPath();
        GzipCodec compressionCodec = new GzipCodec();
        compressionCodec.setConf(context.getConfiguration());
        this.fileSize = (float) fs.getFileStatus(mFilePath).getLen();
        this.inputStream = new DataInputStream(compressionCodec.createInputStream(
                fs.open(mFilePath)));            
    }
    
    public void setReader(IReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        HomePageContentRecord record = reader.readNextRecord(inputStream);
        if (record == null) {
            return false;
        }
        key.set(record.getMetaData().getUrl());
        value.set(record);
        bytesRead += reader.getTotalRecordLength();
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public HomePageContentRecord getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException {
        return (float)this.bytesRead / this.fileSize;
    }
    
    public long getPos() {
        return this.bytesRead;
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

}
