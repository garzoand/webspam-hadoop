/**
 * HostSummaryFeatures
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.enums.features;

/**
 *
 * @author garzo
 */
public enum HostSummaryFeatures implements IFeature {
    Wordcount(100, "wordcount"),  // per host (avg, std)
    OverallWordcount(101, "overall_wordcount"),
    HomePageNum(102, "hpnum"), // per host (avg, std)
    OrigContentSize(103, "origsize");

    private final int index;
    private final String featureName;

    private HostSummaryFeatures(int index, String featureName) {
        this.index = index;
        this.featureName = featureName;
    }

    public int index() {
        return this.index;
    }

    @Override
    public int index_maxmp() {
        return 0;
    }

    @Override
    public int index_hp() {
        return 0;
    }

    @Override
    public String featureName() {
        return this.featureName;
    }
}
