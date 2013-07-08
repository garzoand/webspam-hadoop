/**
 * DictionaryStore.java
 * Interface for dictionary which contains (term, df) tuples for the most frequent terms.
 * 
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.dictionary;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author garzo
 */
public interface DictionaryStore {
    
    public boolean containsWord(String word);
    public int getWordId(String word);
    public String getWord(int id);
    public long getDocumentFrequency(int id);
    public void setDocumentFrequency(int id, long df);
    
    public void setup(Configuration conf) throws IOException;
    public void initialize(Configuration conf) throws IOException;
    public int getWordsNum();
    
    
    /**
     * Return a top-k sub DictionaryStore 
     * 
     * @param limit - top-k value
     * @return
     */
    public DictionaryStore getDictionaryStoreRange(int limit);

}
