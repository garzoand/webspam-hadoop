package hu.sztaki.ilab.bigdata.common.constants;

/**
 *
 * @author garzo
 */
public class ConfigNames {
    
    /* main conf */
    public static final String CONF_INPUT_FORMAT = "bigdata.common.inputformat";
    public static final String DEFAULT_INPUT_FORMAT = "hu.sztaki.ilab.bigdata.common.input.TokenizedTextInputFormat";
    
    public static final String CONF_OUTPUT_FORMAT = "bigdata.common.outputformat";
    public static final String DEFAULT_OUTPUT_FORMAT = "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat";
    
    public static final String CONF_REDUCER_NUM = "bigdata.common.reducers";
    public static final int DEFAULT_REDUCER_NUM = 5;
    
    /* hbase output */
    public static final String CONF_OUTPUT_HBASE_TABLE = "bigdata.common.output.hbase.table";
    public static final String DEFAULT_OUTPUT_HBASE_TABLE = "";
    
    public static final String CONF_OUTPUT_HBASE_QUALIFIER = "bigdata.common.output.hbase.qualifier";
    public static final String DEFAULT_OUTPUT_HBASE_QUALIFIER = "";
    
    /* hbase input */
    // ...
    
    /* stemming */
    public static final String CONF_STEMMER = "bigdata.common.stemmer";
    public static final String DEFAULT_STEMMER = "porter"; // porter|snowball
    
    public static final String CONF_STEMMER_SNOWBALL_LANG = "bigdata.common.stemmer.snowball.language";
    public static final String DEFAULT_STEMMER_SNOWBALL_LANG = "italian"; 
    
    /* tokenizer */
    public static final String CONF_TOKENIZER = "bigdata.common.tokenizer.class";
    public static final String DEFAULT_TOKENIZER = "hu.sztaki.ilab.bigdata.common.tokenize.strategy.ICUBasedTokenizingStrategy";
    
    /* parsing */
    public static final String CONF_PARSING = "bigdata.common.parsing";
    public static final String DEFAULT_PARSING = "hu.sztaki.ilab.bigdata.common.parser.DefaultContentParsingStrategy";
    
    /* html parsing strategy */
    public static final String CONF_HTML_PARSING = "bigdata.common.parsing.html";
    public static final String DEFAULT_HTML_PARSING = "hu.sztaki.ilab.bigdata.common.parser.strategy.JerichoHTMLParsingStrategy";

    /* Arc record parsing */ 
    public static final String ARC_INPUT_CONTENT_TEXT_ONLY = "bigdata.common.input.arc.textonly";
    public static final boolean DEFAULT_ARC_INPUT_CONTENT_TEXT_ONLY = true; // false = all  
    
    /* dictionary */
    public static final String CONF_DICTIONARY_ENABLED = "bigdata.spam.dictionary.enabled";
    public static final String DEFAULT_DICTIONARY_ENABLED = "false";
    
    public static final String CONF_DICTIONARY_CLASS = "bigdata.spam.dictionary.class";
    public static final String DEFAULT_DICTIONARY_CLASS = "hu.sztaki.ilab.bigdata.common.dictionary.FileDictionaryStore";

    public static final String CONF_DICTIONARY_FILENAME = "bigdata.spam.dictionary.file.name";
    public static final String DEFAULT_DICTIONARY_FILENAME = "/user/garzo/dictionary.txt";
        
    public static final String CONF_DICTIONARY_SEPARATOR = "bigdata.spam.dictionary.file.separator";
    public static final String DEFAULT_DICTIONARY_SEPARATOR = "\t";
    
}
