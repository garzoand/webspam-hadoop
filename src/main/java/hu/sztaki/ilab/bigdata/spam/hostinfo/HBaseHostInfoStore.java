/**
 * URL <-> HostID mapping
 * HBase table must have the following structure
 *      (rowid: hostname, family name: "paths" column name: path part of url
 *       the host id will be the value of the record)
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.hostinfo;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author garzo
 */
public class HBaseHostInfoStore implements HostInfoStore {

    @Override
    public boolean initialize(Configuration config) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public HostInfoRecord getHostInfoRecord(String hostName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public HostInfoRecord getHostInfoRecordByID(long hostID) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setup(Configuration config) throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }


}
