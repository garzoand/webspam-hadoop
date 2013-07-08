package hu.sztaki.ilab.bigdata.common.job;

/**
 *
 * @author garzo
 */
public enum Parameter {
    
    HELP("help", "Prints this help", false),
    INPUT_DIR("input", "Comma separated list of input directories placed on HDFS", true),
    OUTPUT_DIR("output", "Output directory on HDFS", true),
    DICTIONARY("dictionary", "Dictionary file", true),
    MULTILANG_DICTIONARY("multilang_dict", "Multi language dictionary", true),
    MODEL_FILE("model", "SVM model file", true),
    REDUCER("reducer", "Number of reducers", true),
    LANGUAGE("language", "Language of input content", true, "italian"),

    INPUT_FILE("input_file", "File name to process", true),
    OUTPUT_FILE("output_file", "Output file name", true),
    
    PARSER("parser", "Parsing strategy class to use", true, 
            "hu.sztaki.ilab.bigdata.common.parser.IdentityContentParsingStrategy"),
    STOPWORDS("stopwords", "File of comma separated stop words", true),
    
    /* HBASE confs */
    HBASE_CONF("hbase_conf", "Config file: hbase-site.xml", true),
    HBASE_TABLE("table", "HBase table name", true),
    HBASE_COLFAM("colfam", "HBase table column family", true),
    HBASE_QUALIFIER("qualifier", "HBase value qualifier (colfam:col)", true);

    private final String id;
    private final String desc;
    private String defaultValue;
    private final boolean hasArg;

    private Parameter(String id, String desc, boolean hasArg) {
        this.id = id;
        this.desc = desc;
        this.hasArg = hasArg;
        this.defaultValue = "";
    }
    
    private Parameter(String id, String desc, boolean hasArg, String defaultValue) {
        this(id, desc, hasArg);
        this.defaultValue = defaultValue;
    }
    
    public String getID() {
        return this.id;
    }
    
    public String getDesc() {
        return this.desc;
    }
    
    public boolean hasArg() {
        return this.hasArg;
    }
    
    public String defaultValue() {
        return this.defaultValue;
    }
    
}
