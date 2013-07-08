/**
 * CmdOptHelper.java
 * Helps to parse command line parameters.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.job;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.*;

/**
 *
 * @author garzo
 */
public class CmdOptHelper {
    
    public static enum ParameterType { REQUIRED, NOT_REQUIRED };
        
    private final String programName;
    private String description;
    private String copyright;
    
    private Options options = new Options();
    private Map<String, String> parameters = new HashMap<String, String>(10);
    
    private CmdOptHelper(String programName) {
        this.programName = programName;             
    }
    
    public static CmdOptHelper create(String programName) {
        return new CmdOptHelper(programName)
                .addParameter(Parameter.HELP, ParameterType.NOT_REQUIRED);
    }
    
    @SuppressWarnings("static-access")
    public CmdOptHelper addParameter(Parameter param, ParameterType required) {
        Option opt = OptionBuilder
                .withArgName(param.getID())
                .withDescription(param.getDesc())
                .create(param.getID());
        if (param.hasArg()) {
            opt.setArgs(1);
        }
        if (required == ParameterType.REQUIRED) {
            opt.setRequired(true);            
        }
        options.addOption(opt);
        return this;
    }
    
    @SuppressWarnings("static-access")
    public CmdOptHelper addParameter(String paramName, String desc, boolean hasArg, ParameterType required) {
        Option opt = OptionBuilder
                .withArgName(paramName)
                .withDescription(desc)
                .create(paramName);
        if (hasArg) {
            opt.setArgs(1);
        }
        if (required == ParameterType.REQUIRED) {
            opt.setRequired(true);            
        }
        options.addOption(opt);
        return this;        
    }
    
    public CmdOptHelper setDescription(String description) {
        this.description = description;
        return this;
    }
    
    public CmdOptHelper setCopyright(String copyright) {
        this.copyright = copyright;
        return this;
    }

    public void printHelp() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp(programName + " <OPTIONS>",
                "\n" + description + "\n\n", options,
                "\n" + copyright + "\n\n");               
    }
    
    public CmdOptHelper ParseOptions(String[] args) {
        try {
            Parser jobCmdOptParser = new GnuParser();
            CommandLine cli = jobCmdOptParser.parse(options, args, true);
            parameters.clear();
            if (cli.hasOption(Parameter.HELP.getID())) {
                printHelp();
                System.exit(0);
            }
            for (Option option : cli.getOptions()) {
                parameters.put(option.getOpt(), option.getValue());                
            }
        } catch (MissingOptionException ex) {
            System.out.println(ex.getMessage());
            printHelp();
            System.exit(1);
        } catch (ParseException ex) {
            System.out.println(ex.getMessage());
            printHelp();
            System.exit(1);
        }        
        return this;
    }
    
    public boolean hasOption(Parameter param) {
        return this.parameters.containsKey(param.getID());
    }
    
    public boolean hasOption(String paramID) {
        return this.parameters.containsKey(paramID);
    }

    public String getOptionValue(Parameter param) {
        return this.parameters.get(param.getID());
    }
    
    public String getOptionValue(String paramID) {
        return this.parameters.get(paramID);
    }

}
