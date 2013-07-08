/**
 * SequenceFileReader.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.tools;

import hu.sztaki.ilab.bigdata.common.record.FeatureOutputRecord;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

/**
 *
 * @author garzo
 */
public class SequenceFileReader {

    public static void main(String[] args) throws IOException {
         
        if (args.length < 2) {
            System.out.println("Usage: SequenceFileReader [seq file] [max records to write]");
            return;
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(args[0]), conf);

        BytesWritable key = new BytesWritable();
        FeatureOutputRecord value = new FeatureOutputRecord();

        int counter = Integer.parseInt(args[1]);
        while (reader.next(key, value)) {
            counter--;
            if (counter < 0)
                break;
            System.out.println(new String(key.getBytes()));
            key = new BytesWritable();
        }
        
    }

}
