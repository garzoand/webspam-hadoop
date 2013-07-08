/**
 * HBasePerformanceTester.java
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.indexloader.hfile;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;

/**
 *
 * @author garzo
 */
public class HBasePerformanceTester {
    
    public static void main(String args[]) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: HBasePerformanceTester [query term] [scan cache size] [batch size]");
            return;
        }
        
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "clueweb_index");
        PrefixFilter prefixFilter = new PrefixFilter(args[0].getBytes());
        Scan scan = new Scan();
        // end:end82378123129 -> startkey = end
        // STARTROW => ""
        scan.setStartRow((args[0] + ":").getBytes());
        scan.setCaching(Integer.parseInt(args[1]));
        scan.setBatch(Integer.parseInt(args[2]));
        scan.addColumn("idx".getBytes(), "x".getBytes());
        scan.setFilter(prefixFilter);
        
        ResultScanner resultScanner = table.getScanner(scan);
        Result result = new Result();
        long time1 = System.currentTimeMillis();
        long counter = 0;
        System.out.println("Start row fetching ...");
        while ((result = resultScanner.next()) != null) {            
            counter++;            
            if (counter % 100000 == 0) {
                System.out.println(counter + " rows fetched.");
            }
        }
        long time2 = System.currentTimeMillis();
        resultScanner.close();
        System.out.println(counter + " rows fetched in " + (time2 - time1) + " ms.");
    }
    
}
