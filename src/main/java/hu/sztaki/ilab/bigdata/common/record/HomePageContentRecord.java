/**
 * <  PROGRAM NAME >
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author garzo
 */
public class HomePageContentRecord implements Writable {

    private HomePageMetaData meta;
    private BytesWritable contentBuffer;

    public HomePageContentRecord() {
        
    }

    public HomePageContentRecord(BytesWritable contentBuffer, HomePageMetaData meta) {
        this.meta = meta;
        this.contentBuffer = contentBuffer;
    }

    public HomePageMetaData getMetaData() {
        return this.meta;
    }

    public void setMetaData(HomePageMetaData meta) {
        this.meta = meta;
    }

    public String getContent() {
        //we want to get back the exact original content without extra allocated bytes!
        contentBuffer.setCapacity(contentBuffer.getLength());
        return Bytes.toString(contentBuffer.getBytes());
    }

    public void setContent(byte[] bytes) {
        contentBuffer = new BytesWritable(bytes);
    }
    
    public void set(HomePageContentRecord record) {
        setContent(record.getContent().getBytes());
        setMetaData(record.getMetaData());
    }

    public void write(DataOutput out) throws IOException {
        contentBuffer.write(out);
        meta.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        
        meta = new HomePageMetaData.Builder(null, null).build();
        contentBuffer = new BytesWritable();
        
        contentBuffer.readFields(in);
        meta.readFields(in);
    }

    @Override
    public String toString() {
        return "HomePageContentRecord [meta=" + meta + ", contentBuffer=" + getContent() + "]";
    }

}
