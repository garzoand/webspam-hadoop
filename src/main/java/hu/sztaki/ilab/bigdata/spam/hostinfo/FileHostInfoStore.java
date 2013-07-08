/**
 * FileHostInfoStore.java
 * Host info record contains host data such as host name, url of page with max page
 * rank, etc.
 * 
 * File format must be the following: hostname max_pr_url_hash hp_url_hash label
 *
 * (C) 2012 MTA SZTAKI
 * Author: Lorand Bendig
 */
package hu.sztaki.ilab.bigdata.spam.hostinfo;

import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 *
 * @author garzo
 */
public class FileHostInfoStore implements HostInfoStore {

    private static final Logger LOG = Logger.getLogger(FileHostInfoStore.class); 
    
    private static final String DICT_DELIMITER = "\t";
    private Map<String, HostInfoRecord> store = new HashMap<String, HostInfoRecord>();
    private List<HostInfoRecord> ID2Store = new ArrayList<HostInfoRecord>();
    
    public void setup(Configuration conf) throws Exception {
        String hostInfoFile = conf.get(SpamConfigNames.CONF_HOST_INFO_FILENAME, 
                SpamConfigNames.DEFAULT_HOST_INFO_FILENAME);
        Path hostInfoPath = new Path(hostInfoFile);
        hostInfoPath = hostInfoPath.makeQualified(hostInfoPath.getFileSystem(conf));
        URI uri;
        try {
            uri = new URI(hostInfoPath.toString());
            DistributedCache.addCacheFile(uri, conf);
            DistributedCache.createSymlink(conf);
            LOG.info("Using host info file: " + hostInfoFile);
        } catch (URISyntaxException ex) {
            LOG.error("Exception when trying to add dictionary file to distributed cache: "
                    + ex.getMessage());
        }
    }
    
    public boolean initialize(Configuration conf) throws Exception {
        
        String hostInfoFileName = new Path(conf.get(SpamConfigNames.CONF_HOST_INFO_FILENAME,
                SpamConfigNames.DEFAULT_HOST_INFO_FILENAME)).getName();
        Path[] cachedFiles = DistributedCache.getLocalCacheFiles(conf);
        InputStreamReader streamReader = null;
        for (Path cachedFile : cachedFiles) {
            String fileName = cachedFile.getName();
            if (fileName.contains(hostInfoFileName)) {
                streamReader = new FileReader(cachedFile.toString());
                break;
            }
        }
        if (streamReader == null) {
            LOG.warn("Unable to determine host info file name!");
            return false;
        }
        
        BufferedReader br = null;
        long counter = 0;
        try {
            br = new BufferedReader(streamReader);
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] split = line.split(DICT_DELIMITER);
                String hostName = split[0];
                int maxPagerankHash = -1;
                int maxHomepageHash = -1;
                int label = -1;
                try {
                    maxPagerankHash = Integer.parseInt(split[1]);
                    maxHomepageHash = Integer.parseInt(split[2]);
                    label = Integer.parseInt(split[3]);
                }
                catch (NumberFormatException e) {
                    String err = "Can' parse HostInfo entry! maxPagerankHash: '%1$s' , maxHomepageHash: '%2$s'";
                    LOG.warn(String.format(err, maxPagerankHash, maxHomepageHash));
                    continue;
                }                
                HostInfoRecord record = new HostInfoRecord.Builder(hostName, counter)
                    .maxHomePageUrlHash(maxPagerankHash)
                    .maxHomePageUrlHash(maxHomepageHash)
                    .label(label)
                    .build();
                
                store.put(hostName, record);
                ID2Store.add(record);
                counter++;
            }
            br.close();
            LOG.info("Host info store initialized with " + counter + " entries.");
            return true;
        }
        finally {
            br.close();
        }
    }

    public void close() throws Exception {
        
    }

    public HostInfoRecord getHostInfoRecord(String hostName) {
        return store.get(hostName);
    }

    public HostInfoRecord getHostInfoRecordByID(long hostID) {
        return ID2Store.get(new Long(hostID).intValue());
    }
    
}
