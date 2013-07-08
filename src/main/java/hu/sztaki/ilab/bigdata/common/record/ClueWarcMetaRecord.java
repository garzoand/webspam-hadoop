/**
 * Record for storing host name, url and TREC ID
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author garzo
 */
public class ClueWarcMetaRecord implements Writable {

    private String hostName;
    private String path;
    private String trecID;

    public ClueWarcMetaRecord() {
        
    }

    public ClueWarcMetaRecord(String hostName, String path, String trecID) {
        this.hostName = hostName;
        this.path = path;
        this.trecID = trecID;
    }

    public String getHostName() {
        return hostName;
    }

    public String getPath() {
        return path;
    }

    public String getTrecID() {
        return trecID;
    }
    
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
    
    public void setPath(String path) {
        this.path = path;
    }
    
    public void setTrecID(String trecID) {
        this.trecID = trecID;
    }

    public void write(DataOutput d) throws IOException {
        d.writeBytes(hostName + "\n");
        d.writeBytes(path + "\n");
        d.writeBytes(trecID + "\n");
    }

    public void readFields(DataInput di) throws IOException {
        this.hostName = di.readLine();
        this.path = di.readLine();
        this.trecID = di.readLine();
    }

}
