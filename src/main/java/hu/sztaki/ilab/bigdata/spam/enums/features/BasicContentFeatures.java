package hu.sztaki.ilab.bigdata.spam.enums.features;

/**
 *
 * @author garzo
 */
public enum BasicContentFeatures implements IFeature {
    WordCount (0, "wordcount"),
    NumTitleWords (1, "title"),
    AvgLength (2, "avglen"),
    FracAnchor (3, "fracanchor"),
    FracVisible (4, "fracvisible"),
    CompressRate (5, "compress"),
    Entropy (6, "entropy"),
    IndepLikelihood (7, "indeplh");

    private static final int FNUM = 8;
    private final int index;
    private final String name;

    private BasicContentFeatures(int index, String name) {
        this.index = index;
        this.name = name;
    }

    public int index() {
        return this.index;
    }
    
    public String featureName() {
        return this.name;
    }
    
    public int index_maxmp() {
        return this.index + FNUM;
    }

    public int index_hp() {
        return this.index + 2 * FNUM;
    }

}

