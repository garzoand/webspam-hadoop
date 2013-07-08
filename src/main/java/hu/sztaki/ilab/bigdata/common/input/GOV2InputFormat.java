/**
 * GOV2InputFormat.java
 * Input format for the GOV2 dataset.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.input;

import hu.sztaki.ilab.bigdata.common.input.readers.CustomRecordReader;
import hu.sztaki.ilab.bigdata.common.input.readers.GOV2Reader;
import hu.sztaki.ilab.bigdata.common.record.HomePageContentRecord;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author garzo
 */
public class GOV2InputFormat extends FileInputFormat<Text, HomePageContentRecord> {

    private static final Log LOG = LogFactory.getLog(GOV2InputFormat.class);
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        // Ensure the input files are not splittable.
        return false;
    }    
    
    @Override
    public RecordReader<Text, HomePageContentRecord> createRecordReader(InputSplit is, TaskAttemptContext tac) 
            throws IOException, InterruptedException {
        return new CustomRecordReader(new GOV2Reader());
    }

}
