package hu.sztaki.ilab.bigdata.spam.constants;

/**
 *
 * @author garzo
 */
public class SpamConfigNames {
    
    /* feature selection */
    public static final String CONF_MAPPER_CLASS = "bigdata.spam.mapper";
    public static final String DEFAULT_MAPPER_CLASS = "hu.sztaki.ilab.bigdata.spam.map.BasicContentFeatureMapper";
    
    public static final String CONF_REDUCER_CLASS = "bigdata.spam.reducer";
    public static final String DEFAULT_REDUCER_CLASS = "hu.sztaki.ilab.bigdata.spam.reduce.BasicContentProcessorReducer";
    
    /* host info */
    public static final String CONF_HOST_INFO_ENABLED = "bigdata.spam.hostinfo.enabled";
    public static final String DEFAULT_HOST_INFO_ENABLED = "false";
    
    // framework will process those hosts only which are in included in the host info
    public static final String CONF_HOST_INFO_PROCESSONLY = "bigdata.spam.hostinfo.processonly";
    public static final String DEFAULT_HOST_INFO_PROCESSONLY = "false";
    
    public static final String CONF_HOST_INFO_STORE = "bigdata.spam.hostinfo.store";
    public static final String DEFAULT_HOST_INFO_STORE = "hu.sztaki.ilab.bigdata.spam.hostinfo.FileHostInfoStore";
    
    public static final String CONF_HOST_INFO_FILENAME = "bigdata.spam.hostinfo.filestore.name";
    public static final String DEFAULT_HOST_INFO_FILENAME = "/user/garzo/hostinfo.txt";    
    
    // whether pass the label value as a feature value to output format
    public static final String CONF_HOST_INFO_PASSLABEL = "bigdata.spam.hostinfo.passlabel";
    public static final String DEFAULT_HOST_INFO_PASSLABEL = "false";    

    /** Corpus and Query features. Default value: {@link QueryPrecisionRecallFeatures#FEATURE_TYPE_QUERY} */
    public static final String CONF_PREC_RECALL_FEATURE_TYPE = "bigdata.spam.precrecall.feautre.type";
    
    /* term weigthing */
    public static final String CONF_TERMW_STRATEGY = "bigdata.spam.termweighting.class";
    public static final String DEFAULT_TERMW_STRATEGY = "hu.sztaki.ilab.bigdata.spam.termweigthing.PivotedTermWeighting";
    
    public static final String CONF_TERMW_COLLECTIONSIZE = "bigdata.spam.termweighting.collectionsize";
    public static final long DEFAULT_TERMW_COLLECTIONSIZE = 1000;

    public static final String CONF_TERMW_AVGDOCSIZE = "bigdata.spam.termweighting.avgdocsize";
    public static final long DEFAULT_TERMW_AVGDOCSIZE = 10;
    
    /* model */
    public static final String CONF_CLASSIFIER = "bigdata.spam.classifier";
    public static final String DEFAULT_CLASSIFIER = "svm";
    
    public static final String CONF_CLASSIFIER_MODELFILE = "bigdata.spam.classifier.modelfile";
    public static final String DEFALT_CLASSIFIER_MODELFILE = "/user/garzo/model.txt";
    
    /* wordcount */
    public static final String CONF_WORDCOUNT_MINLENGTH = "bigdata.spam.wordcount.minlength";
    public static final String DEFAULT_WORDCOUNT_MINLENGTH = "3";

}
