/**
 * HBaseUtils.java
 * Implements simple HBase operations.
 *
 * (C) 2012 MTA SZTAKI
 * Author: Andras Garzo <garzo.at.ilab.sztaki.hu>
 */
package hu.sztaki.ilab.bigdata.common.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression;

/**
 *
 * @author garzo
 */
public class HBaseUtils {
    
    public static void createTable(Configuration conf, String tableName, 
            String[] familys, byte[][] boundaries, Compression.Algorithm compression, int blockSize) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        if (admin.tableExists(tableName)) {
            throw new Exception("table is already exists");
        } else {
            // TODO(garzo): implement blocksize!
            
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);            
            for (int i = 0; i < familys.length; i++) {
                HColumnDescriptor colDesc = new HColumnDescriptor(familys[i]);
                // colDesc.setCompressionType(compression);                
                colDesc.setBlockCacheEnabled(true);
                colDesc.setBlocksize(blockSize);
                tableDesc.addFamily(colDesc);                
            }
            
            admin.createTable(tableDesc, boundaries);
        }                
    }
    
}
