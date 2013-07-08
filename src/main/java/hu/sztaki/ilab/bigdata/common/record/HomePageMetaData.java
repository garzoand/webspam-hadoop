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
import java.util.HashMap;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author garzo
 */
public class HomePageMetaData implements Writable {

    private String url = null;
    private String hostName = null;
    private String path = "";
    private long ip;
    private long collectionID;
    private long timestamp;
    private HashMap<String, byte[]> customProperty;

    public static class Builder {
        private final String url;
        private final String hostName;
        private String path;
        private long ip;
        private long collectionID;
        private long timestamp;
        private HashMap<String, byte[]> customProperty = new HashMap<String, byte[]>();
        
        public Builder(String url, String hostName) {
            this.url = url;
            this.hostName = hostName;
        }

        public Builder ip(long ID) {
            this.ip = ID;
            return this;
        }
        
        public Builder path(String p) {
            this.path = p;
            return this;
        }

        public Builder collectionID(long ID) {
            this.collectionID = ID;
            return this;
        }

        public Builder timestamp(long t) {
            this.timestamp = t;
            return this;
        }
        
        public Builder addCustomPropery(String key, byte[] value) {
            this.customProperty.put(key, value);
            return this;
        }

        public HomePageMetaData build() {
            return new HomePageMetaData(this);
        }
    }

    private HomePageMetaData(Builder builder) {
        this.url = builder.url;
        this.hostName = builder.hostName;
        this.ip = builder.ip;
        this.path = builder.path;
        this.collectionID = builder.collectionID;
        this.timestamp = builder.timestamp;
        this.customProperty = builder.customProperty;
    }

    public String getUrl() {
        return this.url;
    }

    public String getHostName() {
        return this.hostName;
    }

    public long getIp() {
        return this.ip;
    }
    
    public String getPath() {
        return this.path;
    }

    public long getCollectionID() {
        return this.collectionID;
    }

    public long getTimestamp() {
        return this.timestamp;
    }
    
    public byte[] getCustomProperty(String key) {
        return customProperty.get(key);
    }

    public void write(DataOutput out) throws IOException {
        
        StringBuilder builder = new StringBuilder(url)
            .append("\n")
            .append(hostName)
            .append("\n")
            .append(path)
            .append("\n");
        out.writeBytes(builder.toString());
        out.writeLong(timestamp);
        out.writeLong(collectionID);
        out.writeLong(ip);
        out.writeByte(customProperty.size());
        for (String key : customProperty.keySet()) {
            out.writeBytes(key + "\n");
            BytesWritable bw = new BytesWritable(customProperty.get(key));
            bw.write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        customProperty.clear();        
        url = in.readLine();
        hostName = in.readLine();
        path = in.readLine();
        timestamp = in.readLong();
        collectionID = in.readLong();
        ip = in.readLong();
        int size = in.readByte();
        for (int i = 0; i < size; i++) {
            String key = in.readLine();
            BytesWritable bw = new BytesWritable();
            bw.readFields(in);
            customProperty.put(key, bw.getBytes());
        }
    }

    @Override
    public String toString() {
        return "HomePageMetaData [url=" + url + ", hostName=" + hostName + ", path=" + path
                + ", ip=" + ip + ", collectionID=" + collectionID + ", timestamp=" + timestamp
                + " customProperty=["+ customProperty + "]]";
    }
    
}
