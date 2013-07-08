package hu.sztaki.ilab.bigdata.common.job;

import hu.sztaki.ilab.bigdata.common.constants.ConfigNames;
import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryBuilder;
import hu.sztaki.ilab.bigdata.common.dictionary.DictionaryStore;
import hu.sztaki.ilab.bigdata.common.parser.IContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.parser.strategy.IHTMLParsingStrategy;
import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.stemming.SnowballStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.ITokenFilter;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author garzo
 */
public class JobUtils {
    
    private static final Map<String, Class<? extends ITokenFilter>> stemmerClasses 
            = new TreeMap<String, Class<? extends ITokenFilter>>();
    
    static {
        stemmerClasses.put("porter", PorterStemmerFilter.class);
        stemmerClasses.put("snowball", SnowballStemmerFilter.class);
    }
    
    public static ITokenFilter createStemmer(Configuration conf) throws Exception {
        String stemmerName = conf.get(ConfigNames.CONF_STEMMER, ConfigNames.DEFAULT_STEMMER);
        Class<? extends ITokenFilter> stemmerClass = stemmerClasses.get(stemmerName);
        if (stemmerClass == null)
            throw new Exception("invalid stemmer class");
        if ("snowball".equals(stemmerName)) {
            return stemmerClass.getConstructor(String.class).newInstance(
                    conf.get(ConfigNames.CONF_STEMMER_SNOWBALL_LANG, 
                             ConfigNames.DEFAULT_STEMMER_SNOWBALL_LANG));
        } else {
            // default instantiation for stemmer token filters
            return stemmerClass.newInstance();
        }
    }
    
    public static IContentParsingStrategy createParsingStrategyClass(Configuration conf) 
            throws Exception {
        String parserName = conf.get(ConfigNames.CONF_PARSING, ConfigNames.DEFAULT_PARSING);
        String htmlParserName = conf.get(ConfigNames.CONF_HTML_PARSING, ConfigNames.DEFAULT_HTML_PARSING);
        Class<? extends IContentParsingStrategy> parserClass = 
                (Class<? extends IContentParsingStrategy>)Class.forName(parserName);
        Class<? extends IHTMLParsingStrategy> htmlParserClass =
                (Class<? extends IHTMLParsingStrategy>)Class.forName(htmlParserName);
        IHTMLParsingStrategy htmlParser = (IHTMLParsingStrategy)htmlParserClass.newInstance();
        return parserClass.getConstructor(IHTMLParsingStrategy.class).newInstance(htmlParser);
    }
    
    public static void setInputOutputFormat(Job job) throws ClassNotFoundException {
        String inputFormatClassName = job.getConfiguration().get(ConfigNames.CONF_INPUT_FORMAT, 
                ConfigNames.DEFAULT_INPUT_FORMAT);
        String outputFormatClassName = job.getConfiguration().get(ConfigNames.CONF_OUTPUT_FORMAT, 
                ConfigNames.DEFAULT_OUTPUT_FORMAT);
        System.out.println("Input format class: " + inputFormatClassName);
        System.out.println("Output format class: " + outputFormatClassName);
        job.setInputFormatClass(Class.forName(inputFormatClassName).asSubclass(FileInputFormat.class));
        job.setOutputFormatClass(Class.forName(outputFormatClassName).asSubclass(FileOutputFormat.class));        
    }
    
    public static void setNumberOfReducers(Job job) {
        job.setNumReduceTasks(job.getConfiguration().getInt(
                ConfigNames.CONF_REDUCER_NUM, ConfigNames.DEFAULT_REDUCER_NUM));
    }
    
    public static void setupDictionary(Job job) {
        Configuration conf = job.getConfiguration();
        if (conf.getBoolean(ConfigNames.CONF_DICTIONARY_ENABLED, false)) {
            try {
                DictionaryStore dictionary = DictionaryBuilder.buildDictionaryStore(conf);
                dictionary.setup(conf);
            } catch (Exception ex) {
                System.out.println("Error while setting up dictionary: " + ex.getMessage());
                ex.printStackTrace();
            }
        }        
    }
  
}
