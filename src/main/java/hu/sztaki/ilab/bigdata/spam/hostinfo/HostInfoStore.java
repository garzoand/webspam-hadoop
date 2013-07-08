/**
 * URL <-> HostID mapping
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.hostinfo;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author garzo
 */
public interface HostInfoStore {
    
    public boolean initialize(Configuration config) throws Exception;
    public void setup(Configuration config) throws Exception;
    public void close() throws Exception;
    
    // must return null if record does not exist
    public HostInfoRecord getHostInfoRecord(String hostName);
    public HostInfoRecord getHostInfoRecordByID(long hostID);
}
