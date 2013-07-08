package hu.sztaki.ilab.bigdata.spam.hostinfo;

import hu.sztaki.ilab.bigdata.spam.constants.SpamConfigNames;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author garzo
 */
public class HostInfoBuilder {
    
    private static final Log LOG = LogFactory.getLog(HostInfoBuilder.class);
    
    public static HostInfoStore Build(Configuration conf) {
        boolean enabled = Boolean.parseBoolean(conf.get(SpamConfigNames.CONF_HOST_INFO_ENABLED, 
                SpamConfigNames.DEFAULT_HOST_INFO_ENABLED).toLowerCase());
        if (!enabled) {
            LOG.warn("Host info store disabled. Using host names as intermediate keys.");
            return null;
        }
        
        // try to instantiate
        String className = conf.get(SpamConfigNames.CONF_HOST_INFO_STORE, 
                SpamConfigNames.DEFAULT_HOST_INFO_STORE);
        try {
            HostInfoStore store = (HostInfoStore)Class.forName(className).newInstance();
            LOG.info("Instantiated host info store: " + className);            
            return store;
        } catch (Exception ex) {
            LOG.error("Cannot instantiate host info store: " + className);
            LOG.error("Exception: " + ex.getMessage());
            return null;
        }                
    }
    
}
