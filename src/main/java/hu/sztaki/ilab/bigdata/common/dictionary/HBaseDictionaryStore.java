/**
 * Class for HBase stored dictionaries
 * (not yet implemented)
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.dictionary;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author garzo
 */

public class HBaseDictionaryStore implements DictionaryStore {

    public int getWordId(String word) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getWord(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public long getDocumentFrequency(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setDocumentFrequency(int id, long df) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean containsWord(String word) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public DictionaryStore getDictionaryStoreRange(int limit) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
    public void setup(Configuration conf) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void initialize(Configuration conf) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getWordsNum() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
