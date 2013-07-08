/**
 * FileDictionaryStore.java
 * Stores (term, DF) tuples read from files stored in distributed cache.
 * 
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.dictionary;

import hu.sztaki.ilab.bigdata.common.constants.ConfigNames;
import hu.sztaki.ilab.bigdata.common.constants.Parameters;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author garzo
 */
public class FileDictionaryStore implements DictionaryStore {
       
    private static final Log LOG = LogFactory.getLog(FileDictionaryStore.class);
    private Map<String, Integer> dictionary = new HashMap<String, Integer>();
    private String[] words;
    private long[] DF;       
    
    // WARN: it may be overwritten by hadoop configuration object
    private String separator = "\t";

    public FileDictionaryStore() {
        
    }
    
    public FileDictionaryStore(Map<String, Integer> dictionary, String[] words, long[] DF) {
        this.dictionary = dictionary;
        this.words = words;
        this.DF = DF;
    }
    
    public FileDictionaryStore(InputStreamReader streamReader) throws IOException {
        readFromInputStream(streamReader);
    }

    public void setup(Configuration conf) throws IOException {
        String dictionaryFile = conf.get(ConfigNames.CONF_DICTIONARY_FILENAME, 
                ConfigNames.DEFAULT_DICTIONARY_FILENAME);
        Path dictionaryPath = new Path(dictionaryFile);
        dictionaryPath = dictionaryPath.makeQualified(dictionaryPath.getFileSystem(conf));
        URI uri;
        try {
            uri = new URI(dictionaryPath.toString());
            DistributedCache.addCacheFile(uri, conf);
            DistributedCache.createSymlink(conf);        
        } catch (URISyntaxException ex) {
            LOG.error("Exception when trying to add dictionary file to distributed cache: "
                    + ex.getMessage());
        }
    }
    
    public void initialize(Configuration conf) throws IOException {        
        Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
        if (cacheFiles == null)
            return;
        this.separator = conf.get(ConfigNames.CONF_DICTIONARY_SEPARATOR, 
                ConfigNames.DEFAULT_DICTIONARY_SEPARATOR);
        String dictionaryName = new Path(
                conf.get(ConfigNames.CONF_DICTIONARY_FILENAME, 
                ConfigNames.DEFAULT_DICTIONARY_FILENAME)).getName();
        for (Path path : cacheFiles) {
            String fileName = path.getName();
            if (fileName.contains(dictionaryName)) {
                InputStreamReader reader = new FileReader(path.toString());
                readFromInputStream(reader);
            }
        }
    }
    
    private void readFromInputStream(InputStreamReader streamReader) throws IOException {
        words = new String[Parameters.DICTIONARY_SIZE];
        DF = new long[Parameters.DICTIONARY_SIZE];
        BufferedReader br = new BufferedReader(streamReader);        
        
        String line;
        int counter = 0; int lines = 0;
        while ((line = br.readLine()) != null) {
            String[] entry = line.split("\t");
            lines++;
            if (entry.length < 2) {
                LOG.warn("Invalid dictionary line #" + lines);
            } else {                
                dictionary.put(entry[0], counter);
                words[counter] = entry[0];
                DF[counter] = Long.parseLong(entry[1]);
                counter++;
            }
        }
        LOG.info(counter + " words stored to dictionary.");
    }

    public boolean containsWord(String word) {
        return dictionary.containsKey(word);
    }

    public int getWordId(String word) {
        return dictionary.get(word);
    }

    public String getWord(int id) {
        return words[id];
    }

    public long getDocumentFrequency(int id) {
        return DF[id];
    }

    public void setDocumentFrequency(int id, long df) {
        DF[id] = df;
    }
    
    public DictionaryStore getDictionaryStoreRange(int limit) {
        
        String[] subWords = Arrays.copyOfRange(words, 0, limit);
        long[] subDF = Arrays.copyOfRange(DF, 0, limit);
        Map<String, Integer> subDict = new HashMap<String, Integer>();
        
        for (int i=0; i!=limit; i++) {
            subDict.put(words[i], i);
        }
        
        return new FileDictionaryStore(subDict, subWords, subDF);
    }
    
    public int getWordsNum() {
        // TODO(garzo) !
        return 10000;
    }
    
 
}
