package hu.sztaki.ilab.bigdata.spam.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
public class SVMTrainExport {

    public static class TrainData {
        
        private String key = null;
        private int label = 0;
        
        public TrainData(String key, int label) {
            this.key = key;
            this.label = label;
        }
        
        public String getKey() {
            return this.key;            
        }
        
        public int getLabel() {
            return this.label;
        }
        
    }
    
    private static final String DELIM = " ";

    private List<TrainData> trainList = new ArrayList<TrainData>();
    private HashMap<String, Integer> features = new HashMap<String, Integer>();
    private int lastID = 0;
    private Configuration conf;    
    private String tableName = null;
    private String colfamName = null;
    private String outputFileName = null;
    
    private static void printHelp(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("SVMTrainExport <OPTIONS>",
                "\nExports HBase table data to libsvm train file.\n\n", options,
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
                else if ("output".equals(o.getOpt())) {
                    outputFileName = o.getValue();
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
        BufferedReader br = null;
        try {
            String line = null;
            br = new BufferedReader(new FileReader(dictFileName));
            while ((line = br.readLine()) != null) {
                String[] d = line.split(",");
                if (d.length < 2) {
                    System.out.println("Illegal train data line: " + line);
                    continue;
                }
                int label = Integer.parseInt(d[1]);
                trainList.add(new TrainData(d[0], label));
            }
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }
        
    private void writeData(BufferedWriter bw) throws IOException {
        
        HTable htable = null;
        ResultScanner rs = null;
        try {
            htable = new HTable(conf, tableName);
            int counter = 0;
            for (TrainData data : trainList) {
                Get get = new Get(Bytes.toBytes(data.getKey()));
                get.addFamily(Bytes.toBytes(colfamName));
                get.setMaxVersions(1);
                Result result = htable.get(get);    
                if (result == null || result.list() == null) {
                    System.out.println("Unable to fetch record: " + data.key);
                    continue;
                }
                writeDataRow(result.list(), data.getLabel(), bw);
                counter++;
                if (counter % 100 == 0) {
                    System.out.println(counter + " records processed.");
                }
            }
            
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (htable != null) {
                htable.close();
            }
        }
    }
    
    private void writeDataRow(List<KeyValue> rowContent, int label, BufferedWriter bw) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(label);
        for (KeyValue kv : rowContent) {
            sb.append(DELIM);
            String attrName = new String(kv.getQualifier());
            if (!features.containsKey(attrName)) {
                features.put(attrName, lastID++);
            }
            
            int id = features.get(attrName);
            sb.append(id).append(":").append(Bytes.toDouble(kv.getValue()));
        }
        
        // sb.append("\n");
        bw.append(sb.toString());
        bw.newLine();
    }

    private void export() throws IOException {
        if (trainList.isEmpty()) {
            return;
        }

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(new File(outputFileName)));
            writeData(bw);
        } finally {
            if (bw != null) {
                bw.close();
            }
        }
    }

    public static void main(String[] args) throws IOException {

        SVMTrainExport exporter = new SVMTrainExport();
        exporter.parseOptions(args);
        exporter.export();
    }

}
