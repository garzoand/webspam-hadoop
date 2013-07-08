/**
 * TableCreator.java
 * Creates HBase table with predefined region boundaries and partitions
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tools.hbase;

import hu.sztaki.ilab.bigdata.common.job.CmdOptHelper;
import hu.sztaki.ilab.bigdata.common.job.Parameter;
import hu.sztaki.ilab.bigdata.common.utils.HBaseUtils;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author garzo
 */
public class TableCreator extends Configured implements Tool {
    
    public static final String PROGRAM_NAME = "TableCreator";
    public static final String PROGRAM_DESC = "Creates HBase table with predefined region boundaries and partitions";
    public static final String COPYRIGHT = "(C) 2012 MTA SZTAKI";
    
    private int partitionNum = 0;
    private int blockSize = 65536;
    private String tableName = null;
    private String[] colFam = new String[1];
    private String[] boundaries = null;
    private Compression.Algorithm compression = Compression.Algorithm.NONE;
    
    private void init(String[] args, Configuration conf) throws IOException {
        CmdOptHelper options = null;
        try {
            options = CmdOptHelper.create(PROGRAM_NAME)
                    .setDescription(PROGRAM_DESC)
                    .setCopyright(COPYRIGHT)
                    .addParameter(Parameter.HBASE_CONF, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter(Parameter.HBASE_TABLE, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter(Parameter.HBASE_COLFAM, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter("boundaries", "Path of file which contains predefined boundaries of regions", true, CmdOptHelper.ParameterType.REQUIRED)
                    .addParameter("partition", "Number of partitions (0 = disable partitioning, default)", true, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter("compression", "Table compression (default: NONE)", true, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .addParameter("blocksize", "Size of the hfile blocks (default: 65536)", true, CmdOptHelper.ParameterType.NOT_REQUIRED)
                    .ParseOptions(args);                
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            options.printHelp();
            System.exit(1);
        }
        
        if (options.hasOption("partition")) {
            partitionNum = Integer.parseInt(options.getOptionValue("partition"));
        }
        if (options.hasOption("blocksize")) {
            blockSize = Integer.parseInt(options.getOptionValue("blocksize"));
        }
        if (options.hasOption(Parameter.HBASE_CONF)) {
            conf.addResource(new Path(options.getOptionValue(Parameter.HBASE_CONF)));
        }
        if (options.hasOption("compression")) {
            if ("NONE".equals(options.getOptionValue("compression"))) {
                compression = Compression.Algorithm.NONE;
            } else if ("LZO".equals(options.getOptionValue("compression"))) {
                compression = Compression.Algorithm.LZO;
            } else if ("GZ".equals(options.getOptionValue("compression"))) {
                compression = Compression.Algorithm.GZ;
            } else {
                System.out.println("Unknown compression algorithm: " + options.getOptionValue("compression"));
                System.exit(1);
            }
        }
        tableName = options.getOptionValue(Parameter.HBASE_TABLE);
        colFam[0] = options.getOptionValue(Parameter.HBASE_COLFAM);
        boundaries = readBoundaries(conf, options.getOptionValue("boundaries"));
    }
    
    private String[] readBoundaries(Configuration conf, String partitionFile) throws IOException {
        final Path partFile = new Path(partitionFile);
        final FileSystem fs = partFile.getFileSystem(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, partFile, conf);
        ArrayList<String> parts = new ArrayList<String>();
        BytesWritable key = new BytesWritable();
        NullWritable value = NullWritable.get();
        while (reader.next(key, value)) {
          parts.add(Bytes.toString(key.getBytes(), 0, key.getLength()));
          key = new BytesWritable();
        }
        reader.close();
        return parts.toArray((String[])Array.newInstance(String.class, parts.size()));
    }

    public int run(String[] args) throws Exception {
        try {
            Configuration conf = getConf();
            init(args, conf);
            
            // sets up partitions
            byte[][] b = null;
            if (partitionNum < 1) {
                b = new byte[boundaries.length][];
                for (int i = 0; i < boundaries.length; i++) {
                    b[i] = Bytes.toBytes(boundaries[i]);
                }
            } else {
                b = new byte[partitionNum * boundaries.length][];
                for (int i = 0; i < boundaries.length; i++) {
                    for (int p = 0; p < partitionNum; p++) {
                        StringBuilder builder = new StringBuilder(Integer.toString(p))
                                .append(":").append(boundaries[i]);
                        b[i * partitionNum + p] = Bytes.toBytes(builder.toString());
                    }
                }
            }
            
            System.out.println("Appling the following boundaries for certain regions:");
            for (int i = 0; i < b.length; i++)
                System.out.println(new Text(b[i]));
            
            HBaseUtils.createTable(conf, tableName, colFam, b, compression, blockSize);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            return 1;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        TableCreator tableCreator = new TableCreator();
        int res = ToolRunner.run(tableCreator, args);
        System.exit(res);
    }

}
