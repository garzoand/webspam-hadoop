/**
 * Applies content processor strategies to contents
 *
 * (C) 2011 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.stacks;

import hu.sztaki.ilab.bigdata.common.parser.DefaultContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.parser.IContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import hu.sztaki.ilab.bigdata.common.parser.strategy.JerichoHTMLParsingStrategy;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import hu.sztaki.ilab.bigdata.spam.features.IContentProcessingStrategy;
import hu.sztaki.ilab.bigdata.spam.hostinfo.HostInfoStore;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author garzo
 */
public class ContentProcessorStack {

    protected IContentParsingStrategy parsingStrategy = null;
    protected List<IContentProcessingStrategy> processingStrategies;
    protected boolean needTokenize = false;
    protected boolean needParse = false;
    private Map<Integer, Double> features;
    private HostInfoStore hostInfoStore = null;

    public ContentProcessorStack() {
        this(new DefaultContentParsingStrategy(new JerichoHTMLParsingStrategy()));
    }

    public ContentProcessorStack(IContentParsingStrategy strategy) {
        this.parsingStrategy = strategy;
        initialize();
    }

    public void setHostInfoStore(HostInfoStore store) {
        this.hostInfoStore = store;
    }
    
    private void initialize() {
        this.processingStrategies = new ArrayList<IContentProcessingStrategy>();
        this.features = new TreeMap<Integer, Double>();
    }

    public void addContentProcessor(IContentProcessingStrategy strategy) {
        needTokenize = needTokenize || strategy.needTokenizedAndStemmed();
        needParse = needParse || strategy.needHTMLParsing();
        strategy.setHostInfoStore(hostInfoStore);
        processingStrategies.add(strategy);        
    }

    public Map<Integer, Double> getCalculatedFeatures() {
        return this.features;
    }

    public Map<Integer, Double> execute(HomePageContentRecord record) {
        ParseResult result = new ParseResult();
        result.setOriginalContent(record.getContent());
        if (needParse) {
            parsingStrategy.ParseContent(record.getContent(), result);
        }
        features.clear();
        for (IContentProcessingStrategy s : processingStrategies) {
            if (s.processContent(result, record.getMetaData())) {
                features.putAll(s.getFeatures());
            }
        }
        return features;
    }

}
