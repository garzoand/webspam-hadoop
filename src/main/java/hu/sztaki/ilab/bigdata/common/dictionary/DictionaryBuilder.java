/**
 * DictionaryBuilder.java
 * Builds dictionary class instance depends on Configuration.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.dictionary;

import hu.sztaki.ilab.bigdata.common.constants.ConfigNames;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author garzo
 */
public class DictionaryBuilder {
    
    private static final Log LOG = LogFactory.getLog(DictionaryBuilder.class);
    
    public static DictionaryStore buildDictionaryStore(Configuration conf) {
        String className = conf.get(ConfigNames.CONF_DICTIONARY_CLASS,
                ConfigNames.DEFAULT_DICTIONARY_CLASS);
        try {
            DictionaryStore store = (DictionaryStore)Class.forName(className).newInstance();
            LOG.info("Instantiated dictionary store: " + className);            
            return store;
        } catch (Exception ex) {
            LOG.error("Cannot instantiate dictionary store: " + className);
            LOG.error("Exception: " + ex.getMessage());
            return null;
        }
    }
        
}
