/**
 * HostInfoRecord.java
 * Host info record contains host data such as host name, url of page with max page
 * rank, etc.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.hostinfo;

/**
 *
 * @author garzo
 */
public class HostInfoRecord {
    
    private final String hostName;
    private final long hostID;
    private final int maxPageRank;
    private final int homePage; 
    private final int label;
    
    private HostInfoRecord(Builder builder) {
        this.hostName = builder.bHostName;
        this.hostID = builder.bHostID;
        this.maxPageRank = builder.bMaxPageRank;
        this.homePage = builder.bHomePage;
        this.label = builder.label;
    }
    
    public String getHostName() {
        return this.hostName;
    }
    
    public long getHostID() {
        return this.hostID;
    }
    
    public int getMaxPageRankUrlHash() {
        return this.maxPageRank;
    }
    
    public int getHomePageUrlHash() {
        return this.homePage;
    }
    
    public int getLabel() {
        return this.label;
    }
    
    public static class Builder {
        
        private String bHostName;
        private long bHostID;
        private int bMaxPageRank;
        private int bHomePage;
        private int label;
        
        public Builder(String hostName, long hostID) {
            this.bHostName = hostName;
            this.bHostID = hostID;
        }
        
        public Builder maxPageRankUrlHash(int hash) {
            this.bMaxPageRank = hash;
            return this;
        }
        
        public Builder maxHomePageUrlHash(int hash) {
            this.bHomePage = hash;
            return this;
        }
        
        public Builder label(int label) {
            this.label = label;
            return this;
        }
        
        public HostInfoRecord build() {
            return new HostInfoRecord(this);
        }        
    }
    
}
