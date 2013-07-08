/**
 * MultiLangDictionary.java
 * Dictionary for translated term frequency based features
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.dictionary;

import hu.sztaki.ilab.bigdata.common.constants.ConfigNames;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.ITokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordNumberFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
public class MultiLangDictionary implements DictionaryStore {
    
    private ITokenFilter stemmer1 = null;
    private ITokenFilter stemmer2 = null;
    private String separator = "\t";
    
    private Map<String, List<String>> translateMap = new HashMap<String, List<String>>();
    private Map<String, List<String>> reverseTranslateMap = new HashMap<String, List<String>>();
    private Map<String, Integer> IDMap = new HashMap<String, Integer>();
    private Map<Integer, String> reverseIDMap = new HashMap<Integer, String>();
    private Map<Integer, Long> DFMap = new HashMap<Integer, Long>();
    private int counter = 0;
    
    private static final Log LOG = LogFactory.getLog(MultiLangDictionary.class);    
    
    public MultiLangDictionary() {
        
    }
    
    public MultiLangDictionary(ITokenFilter stemmer1, ITokenFilter stemmer2) {
        this.stemmer1 = stemmer1;
        this.stemmer2 = stemmer2;
    }
    
    public void setSeparator(String separator) {
        this.separator = separator;
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
                readFromFile(reader);
            }
        }        
    }
    
    @Override
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
    
    public void readFromFile(InputStreamReader stream) throws IOException {
        String line = null;
        BufferedReader br = new BufferedReader(stream);
        counter = 0;        
        while ((line = br.readLine()) != null) {
            String[] s = line.split(separator);
            if (s.length < 3) {
                continue;
            }    
            String t1 = s[0];
            if (stemmer1 != null) {
                t1 = stem1(s[0]);
            }
            String t2 = s[1];
            if (stemmer2 != null) {
                t2 = stem2(s[1]);
            }
            if (!translateMap.containsKey(t1)) {
                translateMap.put(t1, new ArrayList<String>());
            }
            if (!reverseTranslateMap.containsKey(t2)) {
                reverseTranslateMap.put(t2, new ArrayList<String>());
            }
            if (!IDMap.containsKey(t1)) {
                IDMap.put(t1, counter);
                reverseIDMap.put(counter, t1);
                DFMap.put(counter, Long.parseLong(s[2]));
                counter++;
            }
            ((List<String>)translateMap.get(t1)).add(t2);
            ((List<String>)reverseTranslateMap.get(t2)).add(t1);
        }
        LOG.info(counter + " entries read from dictionary file.");
        LOG.info("translate map size = " + translateMap.size() + " reverse translate map size = "
                 + reverseTranslateMap.size());
    }
    
    public boolean isOriginalWord(String word) {
        return translateMap.containsKey(word);
    }
    
    public boolean containsWord(String word) {
        return translateMap.containsKey(word);
    }
    
    public boolean isTranslatedWord(String word) {
        return reverseTranslateMap.containsKey(word);
    }
    
    public List<String> translate(String word) {
        return translateMap.get(word);
    }
    
    public List<String> reverseTranslate(String word) {
        return reverseTranslateMap.get(word);
    }
    
    public int getWordId(String word) {
        return IDMap.get(word);
    }
    
    public String getWord(int ID) {
        return reverseIDMap.get(ID);
    }
    
    public int getWordsNum() {
        return counter;
    }
    
    public long getDocumentFrequency(int id) {
        return DFMap.get(id);
    }

    @Override
    public void setDocumentFrequency(int id, long df) {
        DFMap.put(id, df);
    }
    
    private String stem1(String word) {
        StringTokenizerStream filter = //new LowerCaseFilter(
                // new WordNumberFilter(
                new StringTokenizerStream(word);
        stemmer1.setInput(filter);
        Token token = null;
        if ((token = stemmer1.next()) != null) {
            return token.getValue();
        } else {
            return null;
        }
    }
    
    private String stem2(String words) {
        ITokenFilter filter = new LowerCaseFilter(
                new WordNumberFilter(
                new StringTokenizerStream(words)));
        stemmer2.setInput(filter);
        Token token = null;
        if ((token = stemmer2.next()) != null) {
            return token.getValue();
        } else {
            return null;
        }
    }

    @Override
    public DictionaryStore getDictionaryStoreRange(int limit) {
        // TODO(garzo) !
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
