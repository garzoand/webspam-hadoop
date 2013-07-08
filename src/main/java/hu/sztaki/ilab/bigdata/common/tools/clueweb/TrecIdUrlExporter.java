package hu.sztaki.ilab.bigdata.common.tools.clueweb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Retrieves and exports hostname - url pairs from a HTable for a given list of trec Ids 
 * 
 * @author Bendig Loránd <lbendig@ilab.sztaki.hu>
 *
 */
public class TrecIdUrlExporter {

    private static final String OUTPUT_DELIM = " "; 
    
    private Configuration conf;
    
    private Set<String> trecIds = new HashSet<String>();
    private String tableName = null;
    private String colFamName = null;
    private String qualifierName = null;
    private String outputFileName = null;

    private static void printHelp(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("TrecIdUrlResolver <OPTIONS>",
                "\nExports hostname - url pairs for a given trec ID list.\n\n", options,
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
            .withArgName("colname")
            .hasArg()
            .withDescription("HBase table column name (format: family:qualifier)")
            .isRequired()
            .create("colname");

        Option dictFileOpt = OptionBuilder
            .withArgName("trecidfile")
            .hasArg()
            .withDescription("Path to trec id file")
            .isRequired()
            .create("trecidfile");
        
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
                else if ("colname".equals(o.getOpt())) {
                    String []colFamSplit = o.getValue().split(":");
                    if (colFamSplit.length != 2) {
                        throw new IllegalArgumentException(
                             "Invalid column name format! Should be: family:qualifier");
                    }
                    colFamName = colFamSplit[0];
                    qualifierName = colFamSplit[1];
                    
                }
                else if ("trecidfile".equals(o.getOpt())) {
                   initTrecIds(o.getValue());
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
    
    private void initTrecIds(String dictFileName) throws IOException {
        BufferedReader br = null;
        try {
            String line = null;
            br = new BufferedReader(new FileReader(dictFileName));
            while ((line = br.readLine()) != null) {
                trecIds.add(line);
            }
        }
        finally {
            if (br != null) {
                br.close();
            }
        }
    }
    
    private void writeData(BufferedWriter bw) throws IOException {

        HTable htable = null;
        try {

            if (trecIds.isEmpty()) {
                return;
            }

            htable = new HTable(conf, tableName);
            for (String trecId : trecIds) {

                Get get = new Get(Bytes.toBytes(trecId));
                get.addColumn(Bytes.toBytes(colFamName), Bytes.toBytes(qualifierName));
                get.setMaxVersions(1);
                
                Result result = htable.get(get);
                if (result == null || result.value() == null) {
                    System.out.println("Key not found: " + trecId);
                    continue;
                }
                String value = new String(result.value());
                URL url = null;
                try {
                    url = new URL(value);
                }
                catch (MalformedURLException ex) {
                    String msg = String.format("ID: %1$s : can't parse URL '%2$s'", trecId, value);
                    System.err.println(msg);
                }

                bw.append(trecId).append(OUTPUT_DELIM)
                  .append(url.getHost()).append(OUTPUT_DELIM).append(value);
                bw.newLine();

            }
        }
        finally {
            if (htable != null) {
                htable.close();
            }
        }
    }
    
    private void export() throws IOException {
        if (trecIds.isEmpty()) {
            return;
        }

        BufferedWriter bw = null;
        try {

            bw = new BufferedWriter(new FileWriter(new File(outputFileName)));
            writeData(bw);
        }
        finally {
            if (bw != null) {
                bw.close();
            }
        }
    }
    
    public static void main(String[] args) throws IOException {

        TrecIdUrlExporter exporter = new TrecIdUrlExporter();
        exporter.parseOptions(args);
        exporter.export();
        
    }

}
