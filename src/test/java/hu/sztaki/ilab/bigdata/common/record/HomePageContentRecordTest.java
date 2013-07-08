package hu.sztaki.ilab.bigdata.common.record;

import hu.sztaki.ilab.bigdata.common.testutils.SerializationUtils;
import hu.sztaki.ilab.bigdata.common.utils.UrlUtils;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.junit.Before;
import org.junit.Test;

public class HomePageContentRecordTest {

   private HomePageContentRecord data;

    @Before
    public void init() {

        HomePageMetaData metaData = null;
        
        String url = "http://example.com/index.html";
        String hostName = "example.com";
        long ip = UrlUtils.ipAsLong("192.168.0.1");
        String path = "/";
        long collectionId = 1234l;
        long timestamp = System.currentTimeMillis();

        metaData = new HomePageMetaData.Builder(url, hostName)
                .collectionID(collectionId)
                .ip(ip)
                .path(path)
                .timestamp(timestamp)
                .build();
        
        data = new HomePageContentRecord(new BytesWritable(Bytes.toBytes("Hello world")), metaData);
        
    }

    @Test
    public void serializationRoundTripTest() throws IOException {
        
        doTest(data);
        
    }
    
    private void doTest(HomePageContentRecord data) throws IOException {
        
        //serialize data
        byte[] serializedBytes = SerializationUtils.serialize(data);
        
        //deserialize data
        HomePageContentRecord deserialized = new HomePageContentRecord();
        SerializationUtils.deserialize(deserialized, serializedBytes);

        //compare
        HomePageMetaData input = data.getMetaData();
        HomePageMetaData output = deserialized.getMetaData();

        Assert.assertEquals("HomePageContentRecord.HomePageMetaData.Url mismatch!",
                getEmptyIfNull(input.getUrl()), getEmptyIfNull(output.getUrl()));

        Assert.assertEquals("HomePageContentRecord.HomePageMetaData.Hostname mismatch!",
                getEmptyIfNull(input.getHostName()), getEmptyIfNull(output.getHostName()));

        Assert.assertEquals("HomePageContentRecord.HomePageMetaData.Path mismatch",
                getEmptyIfNull(input.getPath()), getEmptyIfNull(output.getPath()));

        Assert.assertEquals("HomePageContentRecord.HomePageMetaData.IP mismatch", input.getIp(),
                output.getIp());

        Assert.assertEquals("HomePageContentRecord.HomePageMetaData.CollectionID mismatch",
                input.getCollectionID(), output.getCollectionID());

        Assert.assertEquals("HomePageContentRecord.HomePageMetaData.Timestamp mismatch",
                input.getTimestamp(), output.getTimestamp());

        Assert.assertEquals("HomePageContentRecord.content mismatch", data.getContent(),
                deserialized.getContent());

    }
   
    private String getEmptyIfNull(String val) {
        return (val == null || "null".equals(val)) ? "" : val;
    }

}
