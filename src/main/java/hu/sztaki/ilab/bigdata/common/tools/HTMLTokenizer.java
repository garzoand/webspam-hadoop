/*
 * HTMLTokenizer.java
 * Command line tool for parse and tokenize HTML files.
 * The result will be a CSV like format which is able to process by 
 * TokenizedTextInputFormat.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu> * 
 */
package hu.sztaki.ilab.bigdata.common.tools;

import hu.sztaki.ilab.bigdata.common.input.TokenizedTextInputFormat.TokenizedTextRecordReader;
import hu.sztaki.ilab.bigdata.common.job.CmdOptHelper;
import hu.sztaki.ilab.bigdata.common.job.JobUtils;
import hu.sztaki.ilab.bigdata.common.job.Parameter;
import hu.sztaki.ilab.bigdata.common.parser.IContentParsingStrategy;
import hu.sztaki.ilab.bigdata.common.parser.ParseResult;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author garzo
 */
public class HTMLTokenizer {
    
    public static final String PROGRAM_NAME = "HTMLTokenizer";
    public static final String PROGRAM_DESC = "Parse and tokenize HTML files";
    public static final String COPYRIGHT = "(C) 2012 MTA SZTAKI";
    
    private String fileName = null;
    private String url = null;
    
    private void init(String[] args) {
        CmdOptHelper options = null;
        try {
            options = CmdOptHelper.create(PROGRAM_NAME)
                    .setDescription(PROGRAM_DESC)
                    .setCopyright(COPYRIGHT)
                    .addParameter(Parameter.INPUT_FILE, CmdOptHelper.ParameterType.REQUIRED)                    
                    .ParseOptions(args);                
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            options.printHelp();
            System.exit(1);
        }
        fileName = options.getOptionValue(Parameter.INPUT_FILE);       
    }
    
    private String readContent() throws IOException {
        BufferedReader br = null;
        StringBuilder builder = new StringBuilder();
        String line = null;
        boolean first = true;
        
        try {
            br = new BufferedReader(new FileReader(fileName));
            while ((line = br.readLine()) != null) {
                if (first) {
                    url = line;
                    first = false;
                } else {
                    builder.append(line).append("\n");
                }                
            }            
        } catch (Exception ex) {
            System.out.println("Exception : " + ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
        } finally {
            if (br != null) {
                br.close();
            }
        }
        return builder.toString();
    }
    
    private void parseContent(String content) throws Exception {
        // TODO(garzo): ability to add external hadoop job conf
        Configuration conf = new Configuration();
        ParseResult result = new ParseResult();
        IContentParsingStrategy strategy = JobUtils.createParsingStrategyClass(conf);        
        strategy.ParseContent(content, result);
        
        String output[] = new String[TokenizedTextRecordReader.FIELDS_NUM];
        output[TokenizedTextRecordReader.URL_FIELD] = this.url;
        output[TokenizedTextRecordReader.TITLE_FIELD] = result.getTitle();
        output[TokenizedTextRecordReader.CONTENT_FIELD] = result.getTokenizedContent();
        
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < output.length; i++) {
            builder.append(output[i]).append(TokenizedTextRecordReader.SEPARATOR);
        }
        String out = builder.toString();
        System.out.println(out.substring(0, out.length() - 1));
    }
    
    public static void main(String[] args) {
        try {
            HTMLTokenizer tokenizer = new HTMLTokenizer();
            tokenizer.init(args);
            String content = tokenizer.readContent();
            tokenizer.parseContent(content);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }
    
}
