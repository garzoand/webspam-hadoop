/**
 * Abstract Content Feature Calculator class
 * Decides content has to be parsed, but not tokenized and stemmed.
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.features.content;

import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.record.HomePageMetaData;
import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.ITokenFilter;
import hu.sztaki.ilab.bigdata.spam.enums.features.IFeature;
import hu.sztaki.ilab.bigdata.spam.features.IContentProcessingStrategy;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoRecord;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoStore;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author garzo
 */
public abstract class AbstractContentFeatureCalculator implements IContentProcessingStrategy {

    private Map<Integer, Double> features;
    protected HostInfoStore hostInfoStore = null;
    protected static final Log LOG = LogFactory.getLog(AbstractContentFeatureCalculator.class);
    protected HostInfoRecord hostInfoRecord = null;
    protected boolean calcMaxPRAndHP = false;
    private boolean maxpr = false;
    private boolean hp = false;
    
    // Porter is the default stemmer
    protected ITokenFilter stemmer = new PorterStemmerFilter();    

    public AbstractContentFeatureCalculator() {
        
    }
    
    public void setStemmer(ITokenFilter stemmer) {
        this.stemmer = stemmer;
    }

    protected void buildFeatureMap() {
        features = new TreeMap<Integer, Double>();
    }

    // For those features which weren't defined as enum, e.g. term freqs    
    public void addRawFeature(int id, double value) {
        features.put(id, value);        
    }
    
    public void addFeature(IFeature feature, double value) {
        features.put(feature.index(), value);
        if (maxpr) {
            features.put(feature.index_maxmp(), value);
        }
        if (hp) {
            features.put(feature.index_hp(), value);
        }
    }

    @Override
    public Map getFeatures() {
        return this.features;
    }

    @Override
    public boolean needHTMLParsing() {
        return true;
    }

    @Override
    // Deprecated!
    public boolean needTokenizedAndStemmed() {
        return true;
    }
    
    public void setHostInfoStore(HostInfoStore store) {
        this.hostInfoStore = store;
    }
    
    public void setCalcMaxPRAndHP(boolean b) {
        this.calcMaxPRAndHP = b;
    }

    @Override
    public boolean processContent(ParseResult result, HomePageMetaData meta) {
        maxpr = false;
        hp = false;
        
        buildFeatureMap();
        if (hostInfoStore != null && calcMaxPRAndHP) {
            hostInfoRecord = hostInfoStore.getHostInfoRecord(meta.getHostName());
            if (hostInfoRecord != null) {
                if (hostInfoRecord.getHomePageUrlHash() == meta.getUrl().hashCode()) {
                    hp = true;
                }
                if (hostInfoRecord.getMaxPageRankUrlHash() == meta.getUrl().hashCode()) {
                    maxpr = true;
                }
            }
        }
        return false;
    }

}
