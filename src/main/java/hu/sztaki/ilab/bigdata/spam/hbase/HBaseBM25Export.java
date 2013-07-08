/**
 * HBaseFeatureImporter.java
 * Imports SEQ file with Text as keys and FeatureOutputRecord as value to HBase table.
 * HBase table's values will containt serialized FeatureOutputRecords.
 * This records can be converted to ARFF or libsvm file with 
 * HBaseFeatureExport and HBaseBM25Export class.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 *         Lorand Bendig <lbendig.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.spam.hbase;

import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.SplitKeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Exports HBase table to Weka sparse arff file format
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 *
 */
public class HBaseBM25Export {

    private static final String ATTR_PREFIX = "f";
    private static final String ATTR_TYPE = "real";
    private static final String DATAROW_START = "{";
    private static final String DATAROW_END = "}";
    private static final String LABEL_DELIM = ",";

    private Map<String, Integer> dictElements = new TreeMap<String, Integer>();
    private Map<String, String> labelElements = new HashMap<String, String>();
    
    private boolean isLabelPresent;
    private boolean isShowIDs;
    private Configuration conf;
    
    private String tableName = null;
    private String colfamName = null;
    private String outputFileName = null;

    
    private static void printHelp(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("HTableToArffExporter <OPTIONS>",
                "\nExports HBase table data to Weka sparse arff format.\n\n", options,
                "\nCopyright (c) 2012, MTA SZTAKI. Web: http://datamining.sztaki.hu/\n\n");
    }

    @SuppressWarnings("static-access")
    private static Options getJobOptions() {

        Options jobOptions = new Options();

        Option helpOpt = OptionBuilder
            .withDescription("Prints this help")
            .create("help");

        Option confOpt = OptionBuilder
            .withArgName("conf").hasArg()
            .withDescription("Config file: hbase-site.xml")
            .isRequired()
            .create("conf");
        
        Option tableOpt = OptionBuilder
            .withArgName("table").hasArg()
            .withDescription("HBase table name")
            .isRequired()
            .create("table");

        Option colfamOpt = OptionBuilder
            .withArgName("colfam")
            .hasArg()
            .withDescription("HBase table column family")
            .isRequired()
            .create("colfam");

        Option dictFileOpt = OptionBuilder
            .withArgName("dictfile")
            .hasArg()
            .withDescription("Path to dictionary file")
            .isRequired()
            .create("dictfile");
        
        Option labelFileOpt = OptionBuilder
            .withArgName("labelfile")
            .hasArg()
            .withDescription("Path to label file")
            .create("labelfile");

        Option showIDOpt = OptionBuilder
            .withArgName("showids")
            .withDescription("Whether show IDs")
            .create("showids");

        Option outputOpt = OptionBuilder
            .withArgName("output")
            .hasArg()
            .withDescription("Output file name")
            .isRequired()
            .create("output");

        jobOptions.addOption(confOpt);
        jobOptions.addOption(tableOpt);
        jobOptions.addOption(colfamOpt);
        jobOptions.addOption(outputOpt);
        jobOptions.addOption(dictFileOpt);
        jobOptions.addOption(labelFileOpt);
        jobOptions.addOption(showIDOpt);
        jobOptions.addOption(helpOpt);

        return jobOptions;

    }

    private void parseOptions(String[] args) throws IOException {
        
        Options jobAvailableOptions = getJobOptions();
        Parser jobCmdOptParser = new GnuParser();
        CommandLine cli = null;
        try {
            cli = jobCmdOptParser.parse(jobAvailableOptions, args, true);
            if (cli.hasOption('h')) {
                printHelp(jobAvailableOptions);
                System.exit(0);
            }
            // populate options
            for (Option o : cli.getOptions()) {
                
                if ("conf".equals(o.getOpt())) {
                    conf = HBaseConfiguration.create();
                    conf.addResource(new Path(o.getValue()));
                }
                else if ("table".equals(o.getOpt())) {
                    tableName = o.getValue();
                }
                else if ("colfam".equals(o.getOpt())) {
                    colfamName = o.getValue();
                }
                else if ("dictfile".equals(o.getOpt())) {
                    initDictionary(o.getValue());
                }
                else if ("labelfile".equals(o.getOpt())) {
                    initLabels(o.getValue());
                    isLabelPresent = true;
                }
                else if ("output".equals(o.getOpt())) {
                    outputFileName = o.getValue();
                }
                else if ("showids".equals(o.getOpt())) {
                    isShowIDs = true;
                }
            }
        } catch (MissingOptionException ex) {
            System.out.println(ex.getMessage());
            printHelp(jobAvailableOptions);
            System.exit(1);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            printHelp(jobAvailableOptions);
            System.exit(1);
        }
        
    }
    private void initDictionary(String dictFileName) throws IOException {
        Set<String> dict = new TreeSet<String>();
        BufferedReader br = null;
        try {
            String line = null;
            br = new BufferedReader(new FileReader(dictFileName));
            while ((line = br.readLine()) != null) {
                dict.add(line);
            }

            int count = 0;
            for (String d : dict) {
                dictElements.put(d, count);
                count++;
            }

        } finally {
            if (br != null) {
                br.close();
            }
        }
    }
    
    private void initLabels(String labelFileName) throws IOException {
        
        BufferedReader br = null;
        Pattern delim = Pattern.compile(LABEL_DELIM);
        try {
            String line = null;
            br = new BufferedReader(new FileReader(labelFileName));
            while ((line = br.readLine()) != null) {
               String [] tokens = delim.split(line);
               if (tokens.length != 2) {
                   continue;
               }
               labelElements.put(tokens[0], tokens[1]);
            }

        } finally {
            if (br != null) {
                br.close();
            }
        }
        
    }
    
    private void writeHeader(BufferedWriter bw) throws IOException {
        bw.append("@relation " + tableName);
        bw.newLine();
        bw.newLine();

        // append attributes
        StringBuilder attrSb = null;
        for (Map.Entry<String, Integer> entry : dictElements.entrySet()) {
            attrSb = new StringBuilder("@attribute ")
                .append(ATTR_PREFIX)
                .append(entry.getValue())
                .append(" ")
                .append(ATTR_TYPE)
                .append("\n");
            
            bw.append(attrSb.toString());
        }
        
        //label - a special attribute
        if (isLabelPresent) {
            StringBuilder labelSb = new StringBuilder("@attribute ")
                .append("label")
                .append(" ")
                // .append(ATTR_TYPE)
                .append("{1,0}")
                .append("\n");
            bw.append(labelSb.toString());
        }
        if (isShowIDs) {
            bw.append("@attribute id string\n");
        }
        
        // mark beginning of data
        bw.newLine();
        bw.append("@data");
        bw.newLine();
    }

    private void writeData(BufferedWriter bw) throws IOException {
        
        HTable htable = null;
        ResultScanner rs = null;
        try {
            htable = new HTable(conf, tableName);
            int counter = 0;
            for (String key : labelElements.keySet()) {
                Get get = new Get(Bytes.toBytes(key));
                get.addFamily(Bytes.toBytes(colfamName));
                get.setMaxVersions(1);
                Result result = htable.get(get);
                if (result == null || result.list() == null) {
                    System.out.println("Unable to fetch record: " + key);
                    continue;
                }
                writeDataRow(result.list(),bw);
                counter++;
                if (counter % 100 == 0) {
                    System.out.println(counter + " records processed.");
                }
            }
            System.out.println(counter + " records fetched");
            
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (htable != null) {
                htable.close();
            }
        }
    }
    
    private void writeDataRow(List<KeyValue> rowContent, BufferedWriter bw) throws IOException {
        StringBuilder sb = new StringBuilder(DATAROW_START);
        for (KeyValue kv : rowContent) {
            
            SplitKeyValue split = kv.split();
            FeatureOutputRecord record = new FeatureOutputRecord();            
            DataInputStream stream = new DataInputStream(new ByteArrayInputStream(split.getValue()));
            record.readFields(stream);
            
            Map<Integer, Double> features = new TreeMap<Integer, Double>();
            for (int i = 0; i < record.getFeatures().size(); i++) {
                features.put(Integer.parseInt((String)record.getFeatureNames().get(i)), 
                        (Double)record.getFeatures().get(i));                
            }
            
            for (Integer i : features.keySet())
            {
                sb.append(i)
                  .append(" ")
                  .append(features.get(i))
                  .append(",");
            }
            
            int k = dictElements.size();
            if (isLabelPresent) {
                String labelValue = labelElements.get(new String(kv.getRow()));
                sb.append(k++)
                  .append(" ")
                  .append(labelValue)
                  .append(",");
            }
            
            if (isShowIDs) {
                sb.append(k++).append(" ")
                .append(new String(kv.getRow())).append(",");
            }
            
        }
        
        sb.deleteCharAt(sb.length() - 1);
        sb.append(DATAROW_END);
        bw.append(sb.toString());
        bw.newLine();
    }

    private void export() throws IOException {
        if (dictElements.isEmpty()) {
            return;
        }

        BufferedWriter bw = null;
        try {

            bw = new BufferedWriter(new FileWriter(new File(outputFileName)));
            writeHeader(bw);
            writeData(bw);

        } finally {
            if (bw != null) {
                bw.close();
            }
        }
    }

    public static void main(String[] args) throws IOException {

        HBaseBM25Export exporter = new HBaseBM25Export();
        exporter.parseOptions(args);
        exporter.export();
    }

}