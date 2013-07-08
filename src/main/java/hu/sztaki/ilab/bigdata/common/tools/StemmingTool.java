package hu.sztaki.ilab.bigdata.common.tools;

import hu.sztaki.ilab.bigdata.common.stemming.PorterStemmerFilter;
import hu.sztaki.ilab.bigdata.common.stemming.SnowballStemmerFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.Token;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.ITokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.IdentityTokenFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.LowerCaseFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.filter.WordFilter;
import hu.sztaki.ilab.bigdata.common.tokenize.stream.StringTokenizerStream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;

/**
 *
 * @author garzo
 */
public class StemmingTool {
    
    private static void printHelp(Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("StemmingTool <OPTIONS>",
                "\nStem text files with a stemming method given as parameter.\n\n", options,
                "\nCopyright (c) 2012, MTA SZTAKI. Web: http://datamining.sztaki.hu/\n\n");
    }

    @SuppressWarnings("static-access")
    private static Options createOptions() {

        Options jobOptions = new Options();

        Option helpOpt = OptionBuilder
            .withDescription("Prints this help")
            .create("help");

        Option inputOpt = OptionBuilder
            .withArgName("input")
            .hasArg()
            .withDescription("Input file name")
            .isRequired()
            .create("input");
        
        Option outputOpt = OptionBuilder
            .withArgName("output")
            .hasArg()
            .withDescription("Output file name")
            .isRequired()
            .create("output");
        
        Option stemmerOpt = OptionBuilder
            .withArgName("stemmer")
            .hasArg()
            .withDescription("Stemming method (porter|snowball)")
            .isRequired()
            .create("stemmer");
        
        Option langOpt = OptionBuilder
            .withArgName("lang")
            .hasArg()
            .withDescription("Stemming method (porter|snowball)")
            .create("lang");

        Option columnOpt = OptionBuilder
            .withArgName("column")
            .hasArg()
            .withDescription("Specify column")
            .create("column");

        Option sepOpt = OptionBuilder
            .withArgName("sep")
            .hasArg()
            .withDescription("Column spearator")
            .create("sep");
        
        jobOptions.addOption(outputOpt);
        jobOptions.addOption(inputOpt);
        jobOptions.addOption(stemmerOpt);
        jobOptions.addOption(helpOpt);
        jobOptions.addOption(langOpt);
        jobOptions.addOption(columnOpt);
        jobOptions.addOption(sepOpt);
        return jobOptions;
    }  
    
    private String language = "english";    
    private ITokenFilter stemmer = new IdentityTokenFilter();
    private String inputFileName;
    private String outputFileName;
    private int column = -1;
    private String separator = "\t";
    
    public void parseOptions(String[] args) {
        Options options = createOptions();
        Parser jobCmdOptParser = new GnuParser();
        CommandLine cli = null;
        try {
            cli = jobCmdOptParser.parse(options, args, true);
            if (cli.hasOption("help")) {
                printHelp(options);
                System.exit(0);
            }
            // populate options
            String stemmerName = "porter";
            for (Option o : cli.getOptions()) {
                
                if ("input".equals(o.getOpt())) {
                    inputFileName = o.getValue();
                }
                else if ("output".equals(o.getOpt())) {
                    outputFileName = o.getValue();
                }
                else if ("stemmer".equals(o.getOpt())) {
                    stemmerName = o.getValue();
                }
                else if ("lang".equals(o.getOpt())) {
                    language = o.getValue();
                }
                else if ("column".equals(o.getOpt())) {
                    column = Integer.parseInt(o.getValue()) - 1;
                }
                else if ("sep".equals(o.getOpt())) {
                    separator = o.getValue();
                }                
            }
            if ("porter".equals(stemmerName)) {
                stemmer = new PorterStemmerFilter();
            } else if ("snowball".equals(stemmerName)) {
                try {
                    stemmer = new SnowballStemmerFilter(language);
                } catch (InstantiationException e) {
                    System.out.println(e.getMessage());
                    System.exit(1);
                } catch (IllegalAccessException e) {
                    System.out.println(e.getMessage());
                    System.exit(1);
                }
            } else {
                throw new ClassNotFoundException("Unknown stemming method.");
            }
        } catch (MissingOptionException ex) {
            System.out.println(ex.getMessage());
            printHelp(options);
            System.exit(1);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            printHelp(options);
            System.exit(1);                
        } catch (ClassNotFoundException ex) {
            System.out.println("Stemmer class not found: " + ex.getMessage());
            printHelp(options);
            System.exit(1);
        }        
    }
    
    public BufferedWriter getOuputStream() throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFileName));
        return bw;
    }
    
    public BufferedReader getInputStream() throws FileNotFoundException {
        BufferedReader br = new BufferedReader(new FileReader(inputFileName));
        return br;
    }
    
    public void go() throws IOException {
        BufferedReader br = null;
        BufferedWriter bw = null;
        try {
            br = getInputStream();
            bw = getOuputStream();
            String line = null;            
            while ((line = br.readLine()) != null) {
                if (line.length() == 0) {
                    bw.newLine();
                    continue;
                }
                String[] s = null;
                if (column >= 0) {
                    s = line.split(separator);
                    if (s.length < column + 1) {
                        continue;
                    }
                    line = s[column];
                }
                stemmer.setInput(
                        new WordFilter(
                        new LowerCaseFilter(
                        new StringTokenizerStream(line))));
                Token token = null;
                StringBuilder sb = new StringBuilder();
                while ((token = stemmer.next()) != null) {
                    sb.append(token.getValue()).append(" ");
                }
                bw.append(sb.toString().trim());
                bw.newLine();
            }            
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());            
        } finally {
            if (br != null)
                br.close();
            if (bw != null)
                bw.close();
        }        
    }
    
    public static void main(String[] args) throws IOException {
        StemmingTool stemmingTool = new StemmingTool();
        stemmingTool.parseOptions(args);
        stemmingTool.go();        
    }
    
}
